"""Job streaming: Kafka ``rc.raw`` → Delta bronze → enrich → silver → Postgres.

Một job, hai stage (spec §3.1):
- stage 1 ghi bronze nguyên trạng, append, có checkpoint riêng
- stage 2 ``foreachBatch`` → ``process_batch``: diff, feature, scoring, MERGE
  silver, phát hiện sự kiện nóng, upsert Postgres

Chạy trên VM: ``spark-submit --packages ... -m src.stream.main`` (xem README).
"""

from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import timedelta, timezone
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.schema import BRONZE_FIELDS, bronze_struct, enriched_input_struct, silver_struct
from src.stream import burst, serving
from src.stream.diff_client import (
    STATUS_ERROR,
    STATUS_NEW_PAGE,
    STATUS_OK,
    STATUS_TIMEOUT,
    STATUS_TRUNCATED,
    DiffResult,
    RateLimiter,
    fetch_diff,
)
from src.stream.features import add_cheap_features, add_window_features
from src.stream.scoring import score
from src.stream.summarize import template_summary

BRONZE_TABLE = "bronze/edits"
SILVER_TABLE = "silver/edits_enriched"
TRIGGER = "10 seconds"
MAX_OFFSETS_PER_TRIGGER = 2000
"""Giới hạn kích thước batch: phát lại cả ngày dữ liệu không được thành một
batch khổng lồ phải gọi 170.000 diff liền một lúc."""
HIGH_RISK_THRESHOLD = 0.7
DIFF_WORKERS = 8
CLEANUP_EVERY_SECONDS = 600


def table_path(delta_root: str, table: str) -> str:
    return f"{delta_root.rstrip('/')}/{table}"


def build_spark(app_name: str = "realtime-wiki-stream") -> SparkSession:  # pragma: no cover
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
        # cache --packages của Ivy phải ở thư mục ghi được trong container
        .config("spark.jars.ivy", os.path.join(os.environ.get("HOME", "/tmp"), ".ivy2"))
    )
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"],
    ).getOrCreate()


def with_dt(df: DataFrame) -> DataFrame:
    return df.withColumn("dt", F.to_date("event_time"))


def parse_kafka_values(raw: DataFrame) -> DataFrame:
    """Giải mã cột ``value`` (JSON UTF-8) thành cột bronze + ``dt``.

    Giá trị không parse được bị bỏ: một message hỏng không làm chết stream.
    """
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), bronze_struct()).alias("r")
    ).select("r.*")
    return with_dt(parsed.filter(F.col("rev_id").isNotNull() & F.col("event_time").isNotNull()))


def records_to_df(spark: SparkSession, records: Iterable[dict]) -> DataFrame:
    return with_dt(spark.createDataFrame(list(records), schema=bronze_struct()))


def write_bronze_batch(spark: SparkSession, records, delta_root: str) -> None:
    """Ghi batch (dùng cho test và replay). Append, KHÔNG dedup."""
    df = records if isinstance(records, DataFrame) else records_to_df(spark, records)
    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("dt")
        .save(table_path(delta_root, BRONZE_TABLE))
    )


def write_bronze_from_values(spark: SparkSession, raw: DataFrame, delta_root: str) -> None:
    """Ghi bronze từ DataFrame Kafka (cột ``value``) theo kiểu batch."""
    write_bronze_batch(spark, parse_kafka_values(raw), delta_root)


def write_bronze(df: DataFrame, delta_root: str, checkpoint: str):  # pragma: no cover
    """Stage 1: stream bronze append-only có checkpoint."""
    return (
        df.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("dt")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=TRIGGER)
        .start(table_path(delta_root, BRONZE_TABLE))
    )


def conform_silver(df: DataFrame) -> DataFrame:
    """Chọn đúng cột silver theo đúng kiểu; cột còn thiếu thành null."""
    cols = []
    for field in silver_struct().fields:
        if field.name in df.columns:
            cols.append(F.col(field.name).cast(field.dataType).alias(field.name))
        elif field.name == "dt":
            cols.append(F.to_date("event_time").alias("dt"))
        else:
            cols.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*cols)


def silver_frame(spark: SparkSession, records: Iterable[dict]) -> DataFrame:
    names = [f.name for f in silver_struct().fields]
    rows = [{n: r.get(n) for n in names} for r in records]
    df = spark.createDataFrame(rows, schema=silver_struct())
    return conform_silver(df.drop("dt"))


def merge_silver(spark: SparkSession, batch_df: DataFrame, delta_root: str) -> None:
    """MERGE batch vào silver theo ``(wiki, rev_id)`` — idempotent.

    Replay bằng Last-Event-ID chắc chắn sinh event trùng; xử lý một event 5 lần
    vẫn cho đúng một dòng. Lần chạy sau ghi đè lần trước (ví dụ diff từng
    timeout nay đã lấy được).

    Phải dedup trong batch trước: MERGE của Delta lỗi khi nguồn có khoá trùng.
    """
    from delta.tables import DeltaTable

    source = conform_silver(batch_df).dropDuplicates(["wiki", "rev_id"])
    path = table_path(delta_root, SILVER_TABLE)
    if not DeltaTable.isDeltaTable(spark, path):
        source.write.format("delta").mode("append").partitionBy("dt").save(path)
        return
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(source.alias("s"), "t.wiki = s.wiki AND t.rev_id = s.rev_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# --- stage 2 -----------------------------------------------------------------


@dataclass
class BatchStats:
    rows: int = 0
    rejected: int = 0
    diff_ok: int = 0
    diff_timeout: int = 0
    diff_error: int = 0
    hot_pages: int = 0
    seconds: float = 0.0
    diff_status_counts: dict = field(default_factory=dict)


def _fetch_all(records: list[dict], settings, http) -> list[DiffResult]:
    limiter = RateLimiter(settings.diff_max_rps)

    def one(rec):
        limiter.acquire()
        try:
            return fetch_diff(
                rec["rev_id"],
                rec.get("rev_parent_id"),
                settings.user_agent,
                http=http,
                api_url=settings.api_url,
                timeout=settings.diff_timeout_seconds,
                max_chars=settings.diff_max_chars,
                page_title=rec.get("page_title"),
            )
        except Exception:  # một diff hỏng không được làm chết cả batch
            return DiffResult("", "", STATUS_ERROR)

    workers = max(1, min(DIFF_WORKERS, settings.diff_max_rps, len(records)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(one, records))


def _read_silver(spark: SparkSession, delta_root: str) -> DataFrame | None:
    from delta.tables import DeltaTable

    path = table_path(delta_root, SILVER_TABLE)
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return spark.read.format("delta").load(path)


def _utc_date(t):
    # datetime từ collect() là giờ địa phương không kèm múi; astimezone hiểu đúng điều đó
    return t.astimezone(timezone.utc).date()


def silver_between(silver: DataFrame, t_from, t_to) -> DataFrame:
    """Dòng silver có ``t_from <= event_time <= t_to``.

    Lọc thêm theo partition ``dt`` bằng hằng số ngày để Delta chỉ mở vài thư
    mục ngày, thay vì quét toàn bộ silver mỗi batch 10 giây.
    """
    return silver.filter(
        F.col("dt").between(F.lit(_utc_date(t_from)), F.lit(_utc_date(t_to)))
        & (F.col("event_time") >= F.lit(t_from))
        & (F.col("event_time") <= F.lit(t_to))
    )


def _with_history_window_features(batch: DataFrame, silver, t_min, t_max) -> DataFrame:
    """Feature ``*_prev_1h`` cần 1 giờ lịch sử, mà batch chỉ có 10 giây.

    Ghép thêm silver của 1 giờ trước batch làm ngữ cảnh, tính window, rồi chỉ
    giữ lại dòng của batch.
    """
    cols = ["wiki", "page_title", "user_hash", "event_time", "rev_id"]
    data = batch.withColumn("_in_batch", F.lit(True))
    if silver is not None:
        context = (
            silver_between(silver, t_min - timedelta(hours=1), t_max)
            .select(*cols)
            .join(batch.select("wiki", "rev_id"), ["wiki", "rev_id"], "left_anti")
        )
        for c in data.columns:
            if c not in cols and c != "_in_batch":
                context = context.withColumn(c, F.lit(None).cast(data.schema[c].dataType))
        context = context.withColumn("_in_batch", F.lit(False))
        data = data.unionByName(context.select(*data.columns))
    return add_window_features(data).filter("_in_batch").drop("_in_batch")


def _metrics_rows(silver: DataFrame, minutes: list, hot_df) -> list[dict]:
    """Tính lại TRỌN mỗi phút bị chạm từ silver — replay không đếm trùng."""
    if not minutes:
        return []
    span = silver_between(silver, min(minutes), max(minutes) + timedelta(minutes=1))
    m = span.withColumn("minute", F.date_trunc("minute", "event_time")).filter(
        F.col("minute").isin(minutes)
    )
    agg = m.groupBy("wiki", "minute").agg(
        F.count(F.lit(1)).alias("edit_count"),
        F.sum(F.col("is_anon").cast("int")).alias("anon_count"),
        F.sum("byte_delta").alias("byte_delta_sum"),
        F.sum((F.col("risk_score") >= HIGH_RISK_THRESHOLD).cast("int")).alias("high_risk_count"),
    )
    if hot_df is not None:
        hot = (
            hot_df.filter("is_hot")
            .withColumn("minute", F.col("window_end") - F.expr("INTERVAL 1 MINUTE"))
            .groupBy("wiki", "minute")
            .agg(F.countDistinct("page_title").alias("hot_page_count"))
        )
        agg = agg.join(hot, ["wiki", "minute"], "left")
    else:
        agg = agg.withColumn("hot_page_count", F.lit(0))
    rows = []
    for r in agg.collect():
        d = r.asDict()
        d["hot_page_count"] = d.get("hot_page_count") or 0
        d["high_risk_count"] = d.get("high_risk_count") or 0
        rows.append(d)
    return rows


def process_batch(
    spark: SparkSession,
    batch_df: DataFrame,
    settings,
    http,
    conn,
    model=None,
    model_version: str | None = None,
) -> BatchStats:
    """Stage 2 cho một micro-batch.

    diff → feature rẻ → feature cửa sổ quá khứ → chấm điểm → MERGE silver →
    sự kiện nóng → upsert Postgres. Diff lỗi được đếm, không raise.
    """
    started = time.monotonic()
    stats = BatchStats()

    if "value" in batch_df.columns:
        raw_count = batch_df.count()
        parsed = parse_kafka_values(batch_df)
    else:
        raw_count = None
        parsed = batch_df
    records = [r.asDict() for r in parsed.select(*BRONZE_FIELDS).collect()]
    if raw_count is not None:
        stats.rejected = raw_count - len(records)
    if not records:
        stats.seconds = time.monotonic() - started
        return stats

    # 1. diff: gọi song song, giới hạn tần suất chung
    diffs = _fetch_all(records, settings, http)
    for rec, d in zip(records, diffs):
        rec["diff_added"], rec["diff_removed"], rec["diff_status"] = d.added, d.removed, d.status
        stats.diff_status_counts[d.status] = stats.diff_status_counts.get(d.status, 0) + 1
    counts = stats.diff_status_counts
    stats.diff_ok = sum(counts.get(s, 0) for s in (STATUS_OK, STATUS_NEW_PAGE, STATUS_TRUNCATED))
    stats.diff_timeout = counts.get(STATUS_TIMEOUT, 0)
    stats.diff_error = counts.get(STATUS_ERROR, 0)

    batch = add_cheap_features(spark.createDataFrame(records, schema=enriched_input_struct()))

    # 2. feature cửa sổ quá khứ, có lịch sử từ silver
    times = [r["event_time"] for r in records]
    t_min, t_max = min(times), max(times)
    batch = _with_history_window_features(
        batch, _read_silver(spark, settings.delta_root), t_min, t_max
    )

    # 3. chấm điểm + tóm tắt khuôn mẫu
    batch = score(batch, model, model_version)
    enriched = [r.asDict() for r in conform_silver(batch).collect()]
    for r in enriched:
        r["summary"] = template_summary(
            r.get("diff_added") or "", r.get("diff_removed") or "", r.get("byte_delta") or 0
        )
    stats.rows = len({(r["wiki"], r["rev_id"]) for r in enriched})

    # 4. MERGE silver (idempotent)
    merge_silver(spark, silver_frame(spark, enriched), settings.delta_root)
    silver = _read_silver(spark, settings.delta_root)

    # 5. sự kiện nóng trên 24 giờ gần nhất của các trang vừa bị sửa
    pages = sorted({r["page_title"] for r in enriched if r.get("page_title") is not None})
    hot_df = None
    hot_rows: list[dict] = []
    if pages:
        recent = silver_between(silver, t_min - timedelta(hours=24), t_max).filter(
            F.col("page_title").isin(pages)
        )
        hot_df = (
            burst.detect(recent, settings.burst_z_threshold, settings.burst_min_users)
            .filter(F.col("window_end") > F.lit(t_min))
            .cache()
        )
        latest_summary = {}
        for r in sorted(enriched, key=lambda x: x["event_time"]):
            latest_summary[(r["wiki"], r["page_title"])] = r["summary"]
        for h in hot_df.filter("is_hot").collect():
            d = h.asDict()
            d["summary"] = latest_summary.get((d["wiki"], d["page_title"]))
            hot_rows.append(d)
    stats.hot_pages = len({(h["wiki"], h["page_title"]) for h in hot_rows})

    # 6. tầng phục vụ, một transaction
    minutes = sorted({r["event_time"].replace(second=0, microsecond=0) for r in enriched})
    metrics = _metrics_rows(silver, minutes, hot_df)
    if hot_df is not None:
        hot_df.unpersist()
    with conn.transaction():
        serving.upsert_edits_live(conn, enriched)
        serving.upsert_risk_queue(conn, enriched, size=settings.risk_queue_size)
        serving.upsert_hot_pages(conn, hot_rows)
        serving.upsert_metrics_minute(conn, metrics)

    stats.seconds = time.monotonic() - started
    return stats


def main() -> int:  # pragma: no cover - cần Kafka + Postgres thật, chạy trên VM
    import psycopg

    from src.common.config import Settings
    from src.stream.diff_client import _default_http
    from src.stream.scoring import load_current

    settings = Settings.load()
    spark = build_spark()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze_stream = parse_kafka_values(raw).withWatermark(
        "event_time", f"{settings.watermark_minutes} minutes"
    )
    write_bronze(bronze_stream, settings.delta_root, f"{settings.checkpoint_root}/bronze")

    state = {"model": None, "version": None, "loaded_at": -1e9, "cleaned_at": -1e9}

    def foreach(batch_df, batch_id):
        now = time.monotonic()
        if now - state["loaded_at"] > 300:  # nạp lại model job batch vừa train
            state["model"], state["version"] = load_current(settings.delta_root)
            state["loaded_at"] = now
        with psycopg.connect(settings.pg_dsn, autocommit=True) as conn:
            stats = process_batch(
                spark, batch_df, settings, _default_http, conn,
                model=state["model"], model_version=state["version"],
            )
            if now - state["cleaned_at"] > CLEANUP_EVERY_SECONDS:
                print(f"[cleanup] {serving.cleanup_ttl(conn)}", flush=True)
                state["cleaned_at"] = now
        print(f"[batch {batch_id}] {stats}", flush=True)

    (
        raw.writeStream.foreachBatch(foreach)
        .option("checkpointLocation", f"{settings.checkpoint_root}/enrich")
        .trigger(processingTime=TRIGGER)
        .start()
    )
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
