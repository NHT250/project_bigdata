"""Huấn luyện GBT phát hiện edit bị revert — nhóm TỰ HUẤN LUYỆN (spec §6.1).

Ba nguyên tắc không được phá:
1. Chia tập theo THỜI GIAN, không random — chia random trên chuỗi thời gian
   là rò rỉ.
2. Báo cáo PR-AUC và precision@50, KHÔNG báo cáo accuracy: model luôn trả
   "bình thường" đạt accuracy ~95% và vô dụng.
3. Lớp dương chỉ ~3–7% → ``weightCol`` theo tỉ lệ nghịch tần suất lớp.

Nếu PR-AUC > 0.95 thì mặc định là có rò rỉ — điều tra trước khi tin.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.stream.scoring import (
    CURRENT_VERSION_FILE,
    FEATURE_COLS,
    MODEL_DIR,
    feature_input_cols,
    prepare_features,
)

__all__ = ["FEATURE_COLS"]

METRICS_TABLE = "gold/model_metrics"
TOP_K = 50
LEAK_SUSPICION_PR_AUC = 0.95
FALLBACK_TEST_FRACTION = 0.3


def temporal_split(df: DataFrame, test_days: int) -> tuple[DataFrame, DataFrame]:
    """``test_days`` ngày cuối làm tập test, các ngày trước làm tập train.

    Dữ liệu chưa đủ ``test_days + 1`` ngày (mấy ngày đầu đồ án) thì cắt theo
    mốc thời gian ở phân vị 70% — vẫn là chia theo thời gian, không random.
    """
    with_dt = df if "dt" in df.columns else df.withColumn("dt", F.to_date("event_time"))
    days = sorted(r[0] for r in with_dt.select("dt").distinct().collect())
    if len(days) > test_days:
        first_test = days[-test_days]
        return with_dt.filter(F.col("dt") < first_test), with_dt.filter(F.col("dt") >= first_test)

    ts = with_dt.withColumn("_t", F.unix_timestamp("event_time"))
    cut = ts.approxQuantile("_t", [1 - FALLBACK_TEST_FRACTION], 0.0)[0]
    return (
        ts.filter(F.col("_t") < cut).drop("_t"),
        ts.filter(F.col("_t") >= cut).drop("_t"),
    )


def add_class_weights(df: DataFrame) -> DataFrame:
    """``weight`` = n_âm / n_dương cho lớp dương, 1 cho lớp âm."""
    counts = {r["label"]: r["count"] for r in df.groupBy("label").count().collect()}
    n_pos, n_neg = counts.get(1, 0), counts.get(0, 0)
    pos_weight = float(n_neg) / n_pos if n_pos else 1.0
    return df.withColumn(
        "weight", F.when(F.col("label") == 1, F.lit(pos_weight)).otherwise(F.lit(1.0))
    )


def train_gbt(
    train_df: DataFrame, max_iter: int = 50, seed: int = 42, feature_cols=FEATURE_COLS
) -> PipelineModel:
    """``feature_cols`` chỉ đổi khi CỐ Ý minh hoạ rò rỉ (evidence/ev5_leakage.py)."""
    data = add_class_weights(prepare_features(train_df, feature_cols)).withColumn(
        "label_d", F.col("label").cast("double")
    )
    pipeline = Pipeline(
        stages=[
            VectorAssembler(inputCols=feature_input_cols(feature_cols), outputCol="features"),
            GBTClassifier(
                featuresCol="features",
                labelCol="label_d",
                weightCol="weight",
                maxIter=max_iter,
                maxDepth=5,
                seed=seed,
            ),
        ]
    )
    return pipeline.fit(data)


def _metrics_from_scores(scored: DataFrame) -> dict:
    """``scored`` cần ``label`` và ``p`` (điểm rủi ro). Không có accuracy."""
    scored = scored.select(F.col("label").cast("double").alias("label"), F.col("p").cast("double"))
    scored = scored.cache()
    n = scored.count()
    n_pos = scored.filter("label = 1").count()
    with_raw = scored.withColumn(
        "raw", F.array(F.lit(1.0) - F.col("p"), F.col("p"))
    )
    from pyspark.ml.functions import array_to_vector

    with_raw = with_raw.withColumn("raw", array_to_vector("raw"))
    if 0 < n_pos < n:
        ev = BinaryClassificationEvaluator(rawPredictionCol="raw", labelCol="label")
        pr_auc = ev.setMetricName("areaUnderPR").evaluate(with_raw)
        roc_auc = ev.setMetricName("areaUnderROC").evaluate(with_raw)
    else:
        pr_auc = roc_auc = float("nan")

    top = scored.orderBy(F.col("p").desc()).limit(TOP_K)
    k = min(TOP_K, n)
    hits = top.filter("label = 1").count()
    conf = scored.groupBy(
        (F.col("p") >= 0.5).alias("pred"), (F.col("label") == 1).alias("actual")
    ).count()
    cells = {(r["pred"], r["actual"]): r["count"] for r in conf.collect()}
    scored.unpersist()
    return {
        "n_samples": n,
        "n_positive": n_pos,
        "pr_auc": pr_auc,
        "roc_auc": roc_auc,
        "precision_at_50": hits / k if k else 0.0,
        "recall_at_50": hits / n_pos if n_pos else 0.0,
        "tp": cells.get((True, True), 0),
        "fp": cells.get((True, False), 0),
        "fn": cells.get((False, True), 0),
        "tn": cells.get((False, False), 0),
        "leak_suspected": bool(pr_auc == pr_auc and pr_auc > LEAK_SUSPICION_PR_AUC),
    }


def evaluate(model: PipelineModel, test_df: DataFrame, feature_cols=FEATURE_COLS) -> dict:
    from pyspark.ml.functions import vector_to_array

    pred = model.transform(prepare_features(test_df, feature_cols))
    return _metrics_from_scores(
        pred.withColumn("p", vector_to_array("probability")[1])
    )


def hand_rule_baseline(df: DataFrame) -> dict:
    """Đường cơ sở 1: ``is_anon AND |byte_delta| > 500``."""
    rule = (F.coalesce(F.col("is_anon"), F.lit(False))) & (
        F.coalesce(F.col("abs_byte_delta"), F.lit(0)) > 500
    )
    return _metrics_from_scores(df.withColumn("p", rule.cast("double")))


def new_version(now: datetime | None = None) -> str:
    moment = now or datetime.now(timezone.utc)
    return "gbt-" + moment.strftime("%Y%m%dT%H%M%S")


def save_model(model: PipelineModel, delta_root: str, version: str) -> str:
    """Ghi model vào ``models/<version>`` rồi mới trỏ ``CURRENT_VERSION`` sang.

    Thứ tự này để job streaming không bao giờ nạp một model ghi dở.
    """
    path = os.path.join(delta_root, MODEL_DIR, version)
    model.write().overwrite().save(path)
    pointer = os.path.join(delta_root, MODEL_DIR, CURRENT_VERSION_FILE)
    tmp = pointer + ".tmp"
    os.makedirs(os.path.dirname(pointer), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(version)
    os.replace(tmp, pointer)
    return path


_METRIC_SCHEMA = (
    "model_version string, trained_at timestamp, n_samples long, n_positive long, "
    "pr_auc double, roc_auc double, precision_at_50 double, recall_at_50 double, "
    "baseline_pr_auc double, baseline_precision_at_50 double, "
    "tp long, fp long, fn long, tn long, train_days int, test_days int, notes string"
)


def write_metrics(spark: SparkSession, metrics: dict, delta_root: str) -> None:
    """Append một dòng vào ``gold/model_metrics`` cho mỗi lần train."""
    names = [c.split()[0] for c in _METRIC_SCHEMA.split(", ")]
    row = {n: metrics.get(n) for n in names}
    row["trained_at"] = row["trained_at"] or datetime.now(timezone.utc)
    for n in ("n_samples", "n_positive", "tp", "fp", "fn", "tn", "train_days", "test_days"):
        if row[n] is not None:
            row[n] = int(row[n])
    df = spark.createDataFrame([tuple(row[n] for n in names)], _METRIC_SCHEMA)
    df.write.format("delta").mode("append").save(f"{delta_root.rstrip('/')}/{METRICS_TABLE}")


def read_metrics(spark: SparkSession, delta_root: str) -> DataFrame:
    return spark.read.format("delta").load(f"{delta_root.rstrip('/')}/{METRICS_TABLE}")
