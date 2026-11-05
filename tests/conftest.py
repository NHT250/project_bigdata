"""Fixture dùng chung.

``spark``: SparkSession local[2] có Delta, một phiên cho cả bộ test.
``pg_conn``: kết nối PostgreSQL đã nạp ``sql/schema.sql``, mỗi test một lược đồ
sạch. Lấy từ ``PG_DSN`` nếu có, nếu không thì thử khởi động ``pgserver``
(Postgres nhúng, cài bằng pip). Không có cả hai thì test bị bỏ qua.

Các test cần pyspark/psycopg tự bỏ qua khi thiếu gói, để bộ test thuần Python
vẫn chạy được trên máy không có Java.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("realtime-wiki-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.databricks.delta.snapshotPartitions", "2")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    del pyspark
    yield session
    session.stop()


_PG_SERVER = None


def _pg_dsn(tmp_root: Path) -> str | None:
    global _PG_SERVER
    dsn = os.environ.get("PG_DSN", "").strip()
    if dsn and "@postgres:" not in dsn:
        return dsn
    try:
        import pgserver  # type: ignore
    except ImportError:
        return None
    if _PG_SERVER is None:
        _PG_SERVER = pgserver.get_server(str(tmp_root / "pgdata"), cleanup_mode="stop")
    return _PG_SERVER.get_uri()


@pytest.fixture(scope="session")
def pg_dsn(tmp_path_factory):
    pytest.importorskip("psycopg")
    dsn = _pg_dsn(tmp_path_factory.mktemp("pg"))
    if not dsn:
        pytest.skip("không có PG_DSN và không có pgserver")
    return dsn


@pytest.fixture
def pg_conn(pg_dsn):
    """Kết nối autocommit tới một lược đồ mới cho từng test."""
    import uuid

    import psycopg

    schema = f"t_{uuid.uuid4().hex[:12]}"
    conn = psycopg.connect(pg_dsn, autocommit=True)
    conn.execute(f"CREATE SCHEMA {schema}")
    conn.execute(f"SET search_path TO {schema}")
    conn.execute((ROOT / "sql" / "schema.sql").read_text(encoding="utf-8"))
    try:
        yield conn
    finally:
        conn.execute(f"DROP SCHEMA {schema} CASCADE")
        conn.close()
