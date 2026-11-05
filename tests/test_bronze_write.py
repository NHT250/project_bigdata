import json
from datetime import timedelta
from glob import glob

import pytest

from tests.rows import T0, bronze

pytest.importorskip("pyspark")

from src.stream.main import parse_kafka_values, write_bronze_batch  # noqa: E402


def bronze_count(spark, root):
    return spark.read.format("delta").load(f"{root}/bronze/edits").count()


def test_bronze_write_roundtrips_records(spark, tmp_path):
    write_bronze_batch(spark, [bronze() for _ in range(50)], str(tmp_path))
    assert bronze_count(spark, tmp_path) == 50


def test_bronze_is_partitioned_by_dt(spark, tmp_path):
    recs = [bronze(event_time=T0), bronze(event_time=T0 + timedelta(days=1))]
    write_bronze_batch(spark, recs, str(tmp_path))
    assert len(glob(f"{tmp_path}/bronze/edits/dt=*")) == 2


def test_bronze_keeps_duplicates(spark, tmp_path):
    r = bronze()
    write_bronze_batch(spark, [r, r], str(tmp_path))
    assert bronze_count(spark, tmp_path) == 2  # bronze là sự thật thô, không dedup


def test_kafka_json_values_parse_to_bronze_columns(spark):
    rec = bronze(page_title="Đảo \"Lý\" / Sơn", tags=["mw-undo"])
    value = json.dumps({**rec, "event_time": rec["event_time"].isoformat(),
                        "ingest_time": rec["ingest_time"].isoformat()}, ensure_ascii=False)
    raw = spark.createDataFrame([(value.encode("utf-8"),)], "value binary")
    row = parse_kafka_values(raw).collect()[0]
    assert row.page_title == "Đảo \"Lý\" / Sơn"
    assert row.tags == ["mw-undo"]
    assert row.event_time.replace(tzinfo=None) == T0.replace(tzinfo=None)
    assert str(row.dt) == "2026-11-04"


def test_unparseable_kafka_value_is_dropped_not_fatal(spark):
    raw = spark.createDataFrame([(b"not json",)], "value binary")
    assert parse_kafka_values(raw).count() == 0
