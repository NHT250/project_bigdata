"""Test cho Task 3. CẦN một Postgres đang chạy.

Container của Claude không có Postgres server nên những test này CHƯA được
chạy. Trên VM Oracle hoặc máy có Docker:

    make up
    PG_DSN=postgresql://wiki:wiki@localhost:5432/wiki pytest tests/test_schema_sql.py -v

Tự bỏ qua khi không có PG_DSN hoặc không có psycopg, để bộ test chung vẫn xanh.
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

psycopg = pytest.importorskip("psycopg", reason="psycopg chưa được cài")
PG_DSN = os.environ.get("PG_DSN")
pytestmark = pytest.mark.skipif(
    not PG_DSN, reason="PG_DSN chưa đặt — cần một Postgres đang chạy"
)

EXPECTED_TABLES = {
    "edits_live",
    "hot_pages",
    "risk_queue",
    "metrics_minute",
    "page_hourly",
}


@pytest.fixture
def conn():
    with psycopg.connect(PG_DSN) as connection:
        schema = (Path(__file__).resolve().parents[1] / "sql" / "schema.sql").read_text()
        connection.execute(schema)
        connection.commit()
        yield connection
        connection.rollback()


def test_all_five_tables_exist(conn):
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    ).fetchall()
    assert EXPECTED_TABLES <= {r[0] for r in rows}


def test_edits_live_upsert_keeps_one_row_per_rev(conn):
    conn.execute("DELETE FROM edits_live WHERE wiki = 'testwiki'")
    for score in (0.1, 0.9):
        conn.execute(
            """
            INSERT INTO edits_live (wiki, rev_id, page_title, risk_score, event_time)
            VALUES ('testwiki', 1, 'X', %s, now())
            ON CONFLICT (wiki, rev_id) DO UPDATE SET risk_score = EXCLUDED.risk_score
            """,
            (score,),
        )
    row = conn.execute(
        "SELECT count(*), max(risk_score) FROM edits_live WHERE wiki = 'testwiki'"
    ).fetchone()
    assert row == (1, 0.9)


def test_page_title_accepts_unicode_slash_quote_and_long_values(conn):
    title = "Bão/Cơn_bão–2026's record " + "x" * 400
    conn.execute("DELETE FROM edits_live WHERE wiki = 'testwiki'")
    conn.execute(
        "INSERT INTO edits_live (wiki, rev_id, page_title, event_time)"
        " VALUES ('testwiki', 2, %s, now())",
        (title,),
    )
    stored = conn.execute(
        "SELECT page_title FROM edits_live WHERE wiki = 'testwiki' AND rev_id = 2"
    ).fetchone()[0]
    assert stored == title


def test_indexes_exist_for_the_dashboard_queries(conn):
    rows = conn.execute(
        "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
    ).fetchall()
    names = {r[0] for r in rows}
    assert {
        "edits_live_event_time_idx",
        "hot_pages_z_idx",
        "risk_queue_score_idx",
        "metrics_minute_minute_idx",
        "page_hourly_title_idx",
    } <= names


def test_schema_is_idempotent(conn):
    schema = (Path(__file__).resolve().parents[1] / "sql" / "schema.sql").read_text()
    conn.execute(schema)
    conn.execute(schema)
    conn.commit()
