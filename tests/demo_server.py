"""Chạy website với dữ liệu giả, không cần Docker/Kafka/Spark.

    pip install pgserver uvicorn
    python -m tests.demo_server            # http://localhost:8000

Khởi động Postgres nhúng (pgserver), nạp sql/schema.sql, seed dữ liệu giả
rồi chạy API. Dùng cho Người C làm web song song và cho test Playwright.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def start_db() -> str:
    import pgserver
    import psycopg

    from tests.seed_pg import seed

    server = pgserver.get_server(tempfile.mkdtemp(prefix="rw-demo-"), cleanup_mode="stop")
    dsn = server.get_uri()
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute((ROOT / "sql" / "schema.sql").read_text(encoding="utf-8"))
        seed(conn)
    start_db.server = server  # giữ tham chiếu để server không bị dọn
    return dsn


def main() -> None:  # pragma: no cover
    import uvicorn

    from src.api.db import pool_connection_factory
    from src.api.main import create_app

    dsn = os.environ.get("PG_DSN") or start_db()
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(create_app(pool_connection_factory(dsn)), host="0.0.0.0", port=port)


if __name__ == "__main__":  # pragma: no cover
    main()
