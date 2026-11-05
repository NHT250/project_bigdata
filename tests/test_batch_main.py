"""Job batch chạy trọn một vòng trên dữ liệu nhỏ (spec §3.1 job-batch)."""

import random
from datetime import datetime, timedelta, timezone

import pytest

from tests.rows import bronze

pytest.importorskip("pyspark")

from src.batch.main import run_once  # noqa: E402
from src.common.config import Settings  # noqa: E402
from src.stream.main import merge_silver, silver_frame  # noqa: E402

START = datetime(2026, 11, 1, 0, 0, tzinfo=timezone.utc)
NOW = START + timedelta(days=5)


def settings(tmp_path):
    return Settings(kafka_bootstrap="x", kafka_topic="rc.raw", pg_dsn="x",
                    delta_root=str(tmp_path), wiki_contact="t@example.com")


def seed_silver(spark, root, n_days=4, per_day=80):
    rnd = random.Random(3)
    recs, rev = [], 10_000
    for d in range(n_days):
        for i in range(per_day):
            rev += 1
            t = START + timedelta(days=d, minutes=i * 15)
            page = f"Page {i % 9}"
            vandal = rnd.random() < 0.2
            recs.append(bronze(rev_id=rev, rev_parent_id=rev - 1, page_title=page,
                               user_hash=f"v{rev}" if vandal else f"u{i % 5}",
                               is_anon=vandal, event_time=t, byte_delta=900 if vandal else 5,
                               abs_byte_delta=900 if vandal else 5, comment_len=0 if vandal else 8))
            if vandal:
                rev += 1
                recs.append(bronze(rev_id=rev, rev_parent_id=rev - 1, page_title=page,
                                   user_hash="patrol", tags=["mw-rollback"],
                                   event_time=t + timedelta(minutes=3)))
    merge_silver(spark, silver_frame(spark, recs), root)


def test_run_once_trains_writes_metrics_and_page_hourly(spark, tmp_path, pg_conn):
    seed_silver(spark, str(tmp_path))
    report = run_once(spark, settings(tmp_path), pg_conn, now=NOW, test_days=1, max_iter=3)

    assert report["trained"] is True and report["model_version"].startswith("gbt-")
    metrics = spark.read.format("delta").load(f"{tmp_path}/gold/model_metrics").collect()
    assert len(metrics) == 1 and metrics[0].baseline_pr_auc is not None
    assert spark.read.format("delta").load(f"{tmp_path}/gold/training_set").count() > 0
    assert (tmp_path / "models" / "CURRENT_VERSION").exists()
    assert pg_conn.execute("SELECT count(*) FROM page_hourly").fetchone()[0] > 0


def test_run_once_without_enough_positives_skips_training(spark, tmp_path, pg_conn):
    recs = [bronze(event_time=START + timedelta(minutes=i)) for i in range(20)]
    merge_silver(spark, silver_frame(spark, recs), str(tmp_path))
    report = run_once(spark, settings(tmp_path), pg_conn, now=NOW)
    assert report["trained"] is False and "positive" in report["reason"]


def test_run_once_on_empty_lake_does_nothing(spark, tmp_path, pg_conn):
    report = run_once(spark, settings(tmp_path), pg_conn, now=NOW)
    assert report["trained"] is False


def test_run_nightly_optimizes_and_records_label_audit(spark, tmp_path):
    from src.batch.main import run_nightly

    seed_silver(spark, str(tmp_path))
    out = run_nightly(spark, settings(tmp_path), now=START + timedelta(days=3, hours=1))
    assert "silver/edits_enriched" in out["optimize"]
    assert out["late_reverts"]["total"] > 0
    audit = spark.read.format("delta").load(f"{tmp_path}/gold/label_audit")
    assert audit.count() == 1
