"""Job streaming: Kafka ``rc.raw`` → Delta bronze → enrich → silver → Postgres.

Một job, hai stage (spec §3.1):
- stage 1 ghi bronze nguyên trạng, append, có checkpoint riêng
- stage 2 ``foreachBatch`` → ``process_batch``: diff, feature, scoring, MERGE
  silver, phát hiện sự kiện nóng, upsert Postgres

Chạy trên VM: ``spark-submit --packages ... -m src.stream.main`` (xem README).
"""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.schema import bronze_struct

BRONZE_TABLE = "bronze/edits"
SILVER_TABLE = "silver/edits_enriched"
TRIGGER = "10 seconds"


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
