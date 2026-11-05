"""Phát lại file .jsonl.gz → bronze → silver → Postgres (spec §8.1)."""

import gzip
import json
import threading
from dataclasses import replace

import pytest

from tests.rows import at, bronze

pytest.importorskip("pyspark")

from src.common.config import Settings  # noqa: E402
from src.ingest.replay import replay_file  # noqa: E402
from src.stream.main import process_batch, write_bronze_from_values  # noqa: E402

COMPARE_OK = json.dumps(
    {"compare": {"*": '<tr><td class="diff-addedline"><div>hello world</div></td></tr>'}}
)


class FakeHttp:
    def __init__(self):
        self.calls = 0
        self._fail = {}
        self._lock = threading.Lock()

    def fail_nth(self, n, exc):
        self._fail[n] = exc

    def __call__(self, url, headers, timeout):
        assert "Mozilla" not in headers["User-Agent"]
        with self._lock:
            self.calls += 1
            n = self.calls
        if n in self._fail:
            raise self._fail[n]()
        return COMPARE_OK


class FakeProducer:
    def __init__(self):
        self.values = []

    def send(self, topic, key, value):
        self.values.append(value)

    def flush(self):
        pass


@pytest.fixture
def fake_http():
    return FakeHttp()


@pytest.fixture
def settings(tmp_path):
    return Settings(
        kafka_bootstrap="unused:9092",
        kafka_topic="rc.raw",
        pg_dsn="unused",
        delta_root=str(tmp_path / "delta"),
        wiki_contact="test@example.com",
        diff_max_rps=1000,
    )


def gz_fixture(path, n=200):
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for i in range(n):
            r = bronze(rev_id=5000 + i, page_title=f"Page {i % 7}",
                       user_hash=f"u{i % 13}", event_time=at(i * 0.25))
            r["event_time"] = r["event_time"].isoformat()
            r["ingest_time"] = r["ingest_time"].isoformat()
            r.pop("raw_json")
            r["raw"] = {"title": r["page_title"]}
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    return path


def kafka_batch(spark, tmp_path, n=200, extra_values=()):
    producer = FakeProducer()
    replay_file(str(gz_fixture(tmp_path / "day.jsonl.gz", n)), producer, "rc.raw")
    values = producer.values + list(extra_values)
    return spark.createDataFrame([(v,) for v in values], "value binary")


def count_delta(spark, settings, table):
    return spark.read.format("delta").load(f"{settings.delta_root}/{table}").count()


def pg_count(conn, table):
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def test_replay_end_to_end(spark, tmp_path, pg_conn, fake_http, settings):
    batch = kafka_batch(spark, tmp_path)
    write_bronze_from_values(spark, batch, settings.delta_root)
    stats = process_batch(spark, batch, settings, fake_http, pg_conn)

    assert count_delta(spark, settings, "bronze/edits") == 200
    assert count_delta(spark, settings, "silver/edits_enriched") == 200
    assert pg_count(pg_conn, "edits_live") == 200
    assert stats.rows == 200 and stats.diff_ok == 200
    assert pg_count(pg_conn, "metrics_minute") > 0


def test_replaying_twice_keeps_silver_and_postgres_unique(spark, tmp_path, pg_conn, fake_http, settings):
    batch = kafka_batch(spark, tmp_path, n=40)
    process_batch(spark, batch, settings, fake_http, pg_conn)
    process_batch(spark, batch, settings, fake_http, pg_conn)
    assert count_delta(spark, settings, "silver/edits_enriched") == 40
    assert pg_count(pg_conn, "edits_live") == 40


def test_one_timed_out_diff_does_not_fail_the_batch(spark, tmp_path, pg_conn, fake_http, settings):
    fake_http.fail_nth(7, TimeoutError)
    stats = process_batch(spark, kafka_batch(spark, tmp_path), settings, fake_http, pg_conn)
    assert stats.rows == 200 and stats.diff_timeout == 1
    silver = spark.read.format("delta").load(f"{settings.delta_root}/silver/edits_enriched")
    assert silver.filter("diff_status = 'timeout'").count() == 1


def test_rejected_events_are_counted_not_dropped_silently(spark, tmp_path, pg_conn, fake_http, settings):
    bad = [b"not json", json.dumps({"wiki": "enwiki"}).encode(), b"{\"rev_id\": 1}"]
    stats = process_batch(
        spark, kafka_batch(spark, tmp_path, n=10, extra_values=bad), settings, fake_http, pg_conn
    )
    assert stats.rejected == 3 and stats.rows == 10


def test_window_features_see_history_from_earlier_batches(spark, tmp_path, pg_conn, fake_http, settings):
    batch = kafka_batch(spark, tmp_path, n=20)
    first = batch.limit(10)
    process_batch(spark, first, settings, fake_http, pg_conn)
    rest = batch.subtract(first)
    process_batch(spark, rest, settings, fake_http, pg_conn)
    silver = spark.read.format("delta").load(f"{settings.delta_root}/silver/edits_enriched")
    last = silver.orderBy("event_time", ascending=False).first()
    # 20 edit trên 7 trang trong 5 phút: edit cuối phải thấy edit cùng trang ở batch trước
    assert last.page_edits_prev_1h >= 2


def test_model_scores_are_written_when_model_given(spark, tmp_path, pg_conn, fake_http, settings):
    class ConstModel:
        def transform(self, df):
            from pyspark.ml.linalg import Vectors, VectorUDT
            from pyspark.sql import functions as F
            vec = F.udf(lambda: Vectors.dense([0.2, 0.8]), VectorUDT())
            return df.withColumn("probability", vec())

    stats = process_batch(spark, kafka_batch(spark, tmp_path, n=5), settings, fake_http,
                          pg_conn, model=ConstModel(), model_version="gbt-x")
    assert stats.rows == 5
    rows = pg_conn.execute("SELECT risk_score, model_version FROM risk_queue").fetchall()
    assert len(rows) == 5 and all(r == (pytest.approx(0.8), "gbt-x") for r in rows)


def test_settings_user_agent_is_declared(settings):
    assert "VLU-BigData-RealtimeWiki" in replace(settings).user_agent
