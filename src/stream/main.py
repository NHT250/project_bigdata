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

from src.common.schema import bronze_struct, silver_struct

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
