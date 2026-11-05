"""Bảo trì hằng đêm: OPTIMIZE/VACUUM và đo lại nhãn ở mốc 24 giờ.

Trigger 10 giây sinh ~8.600 file nhỏ mỗi ngày mỗi bảng (spec §5.5). OPTIMIZE
gộp chúng lại theo partition ``dt`` — bằng chứng #3. VACUUM giữ 168 giờ để
time travel vẫn dùng được cho bằng chứng.

``measure_late_reverts`` trả lời câu hỏi "mốc 60 phút bỏ sót bao nhiêu revert
muộn?" — bằng chứng #7, giới hạn vận hành phải khai báo.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.batch.labeling import label_edits

TABLES = ("bronze/edits", "silver/edits_enriched")
VACUUM_RETAIN_HOURS = 168
LABEL_CUTOFF_MINUTES = 60
LATE_RECHECK_MINUTES = 24 * 60


def _path(delta_root: str, table: str) -> str:
    return f"{delta_root.rstrip('/')}/{table}"


def active_file_count(spark: SparkSession, path: str) -> int:
    """Số file trong snapshot hiện tại (không tính file chờ VACUUM)."""
    return int(spark.sql(f"DESCRIBE DETAIL delta.`{path}`").head()["numFiles"])


def optimize_tables(spark: SparkSession, delta_root: str, day: date) -> dict:
    from delta.tables import DeltaTable

    result: dict[str, dict] = {}
    for table in TABLES:
        path = _path(delta_root, table)
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        before = active_file_count(spark, path)
        dt = DeltaTable.forPath(spark, path)
        dt.optimize().where(f"dt = '{day.isoformat()}'").executeCompaction()
        dt.vacuum(VACUUM_RETAIN_HOURS)
        result[table] = {"files_before": before, "files_after": active_file_count(spark, path)}
    return result


def measure_late_reverts(
    spark: SparkSession, delta_root: str, day: date, now: datetime | None = None
) -> dict:
    """So nhãn mốc 60 phút với nhãn mốc 24 giờ cho mọi edit của ``day``."""
    silver = spark.read.format("delta").load(_path(delta_root, "silver/edits_enriched"))
    start = datetime.combine(day, time(0), tzinfo=timezone.utc)
    moment = now or (start + timedelta(days=2))
    window = silver.filter(
        (F.col("event_time") >= F.lit(start - timedelta(hours=1)))
        & (F.col("event_time") < F.lit(start + timedelta(days=2)))
    )

    def labels(cutoff):
        return (
            label_edits(spark, window, cutoff, moment)
            .filter(F.to_date("event_time") == F.lit(day))
            .select("wiki", "rev_id", F.col("label").alias(f"l{cutoff}"))
        )

    joined = labels(LABEL_CUTOFF_MINUTES).join(labels(LATE_RECHECK_MINUTES), ["wiki", "rev_id"])
    agg = joined.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum("l60").alias("pos60"),
        F.sum("l1440").alias("pos24"),
        F.sum(((F.col("l1440") == 1) & (F.col("l60") == 0)).cast("int")).alias("missed"),
    ).head()
    total, missed = int(agg["total"] or 0), int(agg["missed"] or 0)
    pos24 = int(agg["pos24"] or 0)
    return {
        "day": day.isoformat(),
        "total": total,
        "positive_at_60m": int(agg["pos60"] or 0),
        "positive_at_24h": pos24,
        "missed": missed,
        "missed_fraction": missed / total if total else 0.0,
        "missed_share_of_reverts": missed / pos24 if pos24 else 0.0,
    }
