"""Tổng hợp ``page_hourly`` cho trang tra cứu chủ đề (spec §5.4, §7.3).

Chỉ ghi trang có ≥5 edit trong ngày: hàng trăm nghìn trang chỉ bị sửa 1–2 lần
mỗi ngày sẽ làm phình bảng phục vụ mà không ai tra cứu.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.stream import serving

DEFAULT_MIN_EDITS_PER_DAY = 5


def page_hourly(silver_df: DataFrame) -> DataFrame:
    return silver_df.groupBy(
        "wiki", "page_title", F.date_trunc("hour", "event_time").alias("hour_bucket")
    ).agg(
        F.count(F.lit(1)).cast("int").alias("edit_count"),
        F.countDistinct("user_hash").cast("int").alias("distinct_users"),
        F.sum("byte_delta").cast("long").alias("total_byte_delta"),
        F.avg(F.coalesce(F.col("is_anon"), F.lit(False)).cast("double")).alias("anon_ratio"),
        F.avg("risk_score").alias("mean_risk_score"),
        F.max("risk_score").alias("max_risk_score"),
    )


def to_postgres(conn, df: DataFrame, min_edits_per_day: int = DEFAULT_MIN_EDITS_PER_DAY) -> int:
    """Upsert các giờ của những trang đạt ngưỡng edit trong ngày. Trả số dòng."""
    day = Window.partitionBy("wiki", "page_title", F.to_date("hour_bucket"))
    kept = df.withColumn("_day_edits", F.sum("edit_count").over(day)).filter(
        F.col("_day_edits") >= min_edits_per_day
    )
    rows = [r.asDict() for r in kept.drop("_day_edits").collect()]
    with conn.transaction():
        return serving.upsert_page_hourly(conn, rows)
