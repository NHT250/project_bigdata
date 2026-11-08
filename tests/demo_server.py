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
    uvicorn.run(create_app(pool_connection_factory(dsn), **_demo_chat(dsn)), host="0.0.0.0", port=port)


def _demo_chat(dsn: str) -> dict:  # pragma: no cover
    """``DEMO_CHAT=1``: chat trả một câu SQL cố định để xem giao diện, không gọi LLM."""
    if os.environ.get("DEMO_CHAT") != "1":
        return {}
    from contextlib import contextmanager

    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute((ROOT / "sql" / "chat_role.sql").read_text(encoding="utf-8"))

    @contextmanager
    def readonly():
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute("SET ROLE wiki_chat")
            yield conn

    def fixed_llm(system, question):
        if "revert" in question.lower():
            return "NO_DATA: nhãn revert không có trong năm bảng phục vụ."
        if "xoá" in question.lower():
            return "DROP TABLE edits_live"
        return (
            'SELECT page_title AS "Trang", sum(edit_count) AS "Số edit" FROM page_hourly '
            "GROUP BY page_title ORDER BY 2 DESC"
        )

    return {"chat_connection": readonly, "llm": fixed_llm}


if __name__ == "__main__":  # pragma: no cover
    main()
