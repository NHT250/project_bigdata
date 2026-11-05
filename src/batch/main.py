"""Job batch, chạy theo cron 15 phút (spec §3.1).

    label_edits → gold/training_set → train_gbt → evaluate (+ luật thủ công)
    → gold/model_metrics → lưu model → page_hourly → Postgres → cleanup_ttl

Cờ ``--nightly``: thêm OPTIMIZE/VACUUM ngày hôm qua và đo lại nhãn ở mốc 24 giờ
của ngày hôm kia (cần đủ 24 giờ sau ngày đó).

Dùng: ``python -m src.batch.main [--nightly]`` (Makefile: ``make nightly``).
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.batch import aggregate, maintenance, train
from src.batch.labeling import label_edits
from src.stream import serving

TRAINING_TABLE = "gold/training_set"
LATE_REVERT_TABLE = "gold/label_audit"
MIN_POSITIVES = 20
DEFAULT_TEST_DAYS = 2
PAGE_HOURLY_LOOKBACK_DAYS = 2


def _silver(spark: SparkSession, delta_root: str):
    from delta.tables import DeltaTable

    path = f"{delta_root.rstrip('/')}/silver/edits_enriched"
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return spark.read.format("delta").load(path)


def run_once(
    spark: SparkSession,
    settings,
    conn,
    now: datetime | None = None,
    test_days: int = DEFAULT_TEST_DAYS,
    max_iter: int = 50,
) -> dict:
    moment = now or datetime.now(timezone.utc)
    root = settings.delta_root.rstrip("/")
    report: dict = {"trained": False, "reason": "", "model_version": None}

    silver = _silver(spark, root)
    if silver is None:
        report["reason"] = "silver chưa có dữ liệu"
        return report

    # 1. nhãn + tập huấn luyện
    labeled = label_edits(spark, silver, settings.label_cutoff_minutes, moment).cache()
    labeled.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy(
        "dt"
    ).save(f"{root}/{TRAINING_TABLE}")

    # 2. train + đánh giá, chỉ khi đủ dữ liệu
    tr, te = train.temporal_split(labeled, test_days=test_days)
    n_pos_tr = tr.filter("label = 1").count()
    n_pos_te = te.filter("label = 1").count()
    if n_pos_tr < MIN_POSITIVES or n_pos_te == 0:
        report["reason"] = (
            f"chưa đủ mẫu positive (train={n_pos_tr}, test={n_pos_te}, cần ≥{MIN_POSITIVES})"
        )
    else:
        model = train.train_gbt(tr, max_iter=max_iter)
        metrics = train.evaluate(model, te)
        baseline = train.hand_rule_baseline(te)
        version = train.new_version(moment)
        train_days = tr.select("dt").distinct().count()
        test_days_real = te.select("dt").distinct().count()
        notes = "LEAK SUSPECTED: PR-AUC > 0.95" if metrics["leak_suspected"] else ""
        train.write_metrics(
            spark,
            {
                **metrics,
                "model_version": version,
                "trained_at": moment,
                "baseline_pr_auc": baseline["pr_auc"],
                "baseline_precision_at_50": baseline["precision_at_50"],
                "train_days": train_days,
                "test_days": test_days_real,
                "notes": notes,
            },
            root,
        )
        train.save_model(model, root, version)
        report.update(trained=True, model_version=version, metrics=metrics, baseline=baseline)
    labeled.unpersist()

    # 3. page_hourly → Postgres
    recent = silver.filter(
        F.col("event_time") >= F.lit(moment - timedelta(days=PAGE_HOURLY_LOOKBACK_DAYS))
    )
    report["page_hourly_rows"] = aggregate.to_postgres(conn, aggregate.page_hourly(recent))
    report["cleanup"] = serving.cleanup_ttl(conn)
    return report


def run_nightly(spark: SparkSession, settings, now: datetime | None = None) -> dict:
    moment = now or datetime.now(timezone.utc)
    yesterday = (moment - timedelta(days=1)).date()
    day_before = (moment - timedelta(days=2)).date()
    optimized = maintenance.optimize_tables(spark, settings.delta_root, yesterday)
    late = maintenance.measure_late_reverts(spark, settings.delta_root, day_before, moment)
    spark.createDataFrame([late]).withColumn("measured_at", F.lit(moment)).write.format(
        "delta"
    ).mode("append").save(f"{settings.delta_root.rstrip('/')}/{LATE_REVERT_TABLE}")
    return {"optimize": optimized, "late_reverts": late}


def main(argv=None) -> int:  # pragma: no cover - chạy trên VM
    import psycopg

    from src.common.config import Settings
    from src.stream.main import build_spark

    parser = argparse.ArgumentParser()
    parser.add_argument("--nightly", action="store_true")
    args = parser.parse_args(argv)

    settings = Settings.load()
    spark = build_spark("realtime-wiki-batch")
    with psycopg.connect(settings.pg_dsn, autocommit=True) as conn:
        report = run_once(spark, settings, conn)
    if args.nightly:
        report["nightly"] = run_nightly(spark, settings)
    print(json.dumps(report, default=str, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
