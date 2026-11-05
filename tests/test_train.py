"""Huấn luyện và đánh giá model phát hiện phá hoại (spec §6.1)."""

import random
from datetime import timedelta

import pytest

from tests.rows import T0, bronze

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from src.batch.train import (  # noqa: E402
    FEATURE_COLS,
    add_class_weights,
    evaluate,
    hand_rule_baseline,
    read_metrics,
    save_model,
    temporal_split,
    train_gbt,
    write_metrics,
)
from src.stream.features import add_cheap_features  # noqa: E402
from src.stream.main import silver_frame  # noqa: E402
from src.stream.scoring import load_current, score  # noqa: E402


def labeled(spark, n_days=10, per_day=60, pos_rate=0.15, seed=7):
    rnd = random.Random(seed)
    recs = []
    for d in range(n_days):
        for i in range(per_day):
            vandal = rnd.random() < pos_rate
            r = bronze(
                event_time=T0 + timedelta(days=d, minutes=i * 7),
                is_anon=vandal or rnd.random() < 0.2,
                byte_old=5000,
                byte_new=5000 + (rnd.choice([-1, 1]) * rnd.randint(600, 3000) if vandal
                                 else rnd.randint(-50, 80)),
                comment="" if vandal else "copyedit",
            )
            r.update(diff_added="AAAAAAAA!!!" if vandal else "fixed a typo", diff_removed="",
                     diff_status="ok", label=int(vandal))
            recs.append(r)
    labels = {r["rev_id"]: r["label"] for r in recs}
    df = add_cheap_features(silver_frame(spark, recs).drop(
        *[c for c in FEATURE_COLS if c not in ("is_anon", "is_minor")]))
    lab = spark.createDataFrame(list(labels.items()), "rev_id long, label int")
    return df.join(lab, "rev_id").withColumn("page_edits_prev_1h", F.lit(0)).withColumn(
        "user_edits_prev_1h", F.lit(0))


@pytest.fixture(scope="module")
def data(spark):
    df = labeled(spark).cache()
    tr, te = temporal_split(df, test_days=3)
    model = train_gbt(tr, max_iter=5)
    return df, tr, te, model


def test_feature_cols_exclude_every_forbidden_column():
    forbidden = {"label", "label_source", "labeled_at", "risk_score", "model_version",
                 "tags", "rev_id", "rev_parent_id", "event_id", "raw_json", "page_title",
                 "user_hash"}
    assert not (set(FEATURE_COLS) & forbidden)


def test_feature_cols_match_spec_list():
    assert set(FEATURE_COLS) == {
        "byte_delta", "abs_byte_delta", "is_anon", "is_minor", "comment_len",
        "comment_empty", "hour_of_day", "day_of_week", "upper_ratio", "repeat_char",
        "has_url_added", "words_added", "words_removed", "page_edits_prev_1h",
        "user_edits_prev_1h",
    }


def test_temporal_split_puts_no_test_day_before_any_train_day(data):
    _, tr, te, _ = data
    assert tr.agg(F.max("event_time")).head()[0] < te.agg(F.min("event_time")).head()[0]
    assert te.select("dt").distinct().count() == 3


def test_temporal_split_with_too_few_days_still_splits_by_time(spark):
    df = labeled(spark, n_days=1, per_day=40)
    tr, te = temporal_split(df, test_days=3)
    assert tr.count() > 0 and te.count() > 0
    assert tr.agg(F.max("event_time")).head()[0] <= te.agg(F.min("event_time")).head()[0]


def test_evaluate_reports_pr_auc_and_precision_at_50_and_not_accuracy(data):
    _, _, te, model = data
    m = evaluate(model, te)
    assert {"pr_auc", "roc_auc", "precision_at_50", "recall_at_50", "n_samples",
            "n_positive", "tp", "fp", "fn", "tn"} <= set(m)
    assert "accuracy" not in m
    assert 0.0 <= m["pr_auc"] <= 1.0


def test_model_learns_the_planted_signal_better_than_chance(data):
    _, _, te, model = data
    m = evaluate(model, te)
    assert m["pr_auc"] > m["n_positive"] / m["n_samples"]


def test_class_weight_column_is_inverse_of_class_frequency(spark):
    rows = [(1,)] * 5 + [(0,)] * 95
    w = add_class_weights(spark.createDataFrame(rows, "label int"))
    assert w.filter("label=1").head().weight == pytest.approx(19.0, rel=0.1)
    assert w.filter("label=0").head().weight == pytest.approx(1.0)


def test_hand_rule_baseline_returns_same_metric_keys_as_model(data):
    _, _, te, model = data
    assert set(hand_rule_baseline(te)) == set(evaluate(model, te))


def test_write_metrics_appends_one_row_per_run(spark, tmp_path, data):
    _, _, te, model = data
    m = evaluate(model, te)
    m1 = {**m, "model_version": "v1", "train_days": 7, "test_days": 3, "notes": "a"}
    m2 = {**m1, "model_version": "v2"}
    write_metrics(spark, m1, str(tmp_path))
    write_metrics(spark, m2, str(tmp_path))
    assert read_metrics(spark, str(tmp_path)).count() == 2


def test_saved_model_loads_and_scores(spark, tmp_path, data):
    _, _, te, model = data
    save_model(model, str(tmp_path), "gbt-test-1")
    loaded, version = load_current(str(tmp_path))
    assert version == "gbt-test-1"
    out = score(te.drop("risk_score", "model_version"), loaded, version)
    r = out.select("risk_score", "model_version").head()
    assert 0.0 <= r.risk_score <= 1.0 and r.model_version == "gbt-test-1"


def test_score_without_model_gives_null_not_made_up_numbers(data):
    _, _, te, _ = data
    r = score(te.drop("risk_score", "model_version"), None, None).head()
    assert r.risk_score is None
