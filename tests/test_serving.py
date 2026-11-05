from datetime import datetime, timedelta, timezone

from src.stream.serving import (
    cleanup_ttl,
    upsert_edits_live,
    upsert_hot_pages,
    upsert_metrics_minute,
    upsert_risk_queue,
)

NOW = datetime.now(timezone.utc).replace(microsecond=0)


def row(rev_id=1, risk_score=0.5, event_time=None, **over):
    r = {
        "wiki": "enwiki",
        "rev_id": rev_id,
        "page_title": "Bão/Cơn_bão–2026's record",
        "user_hash": "abc",
        "is_anon": True,
        "comment": "x",
        "byte_delta": 12,
        "risk_score": risk_score,
        "model_version": "gbt-test",
        "summary": "Thêm 3 từ",
        "event_time": event_time or NOW,
        "ingest_time": NOW,
        "diff_added": "a",
        "diff_removed": "",
        "diff_status": "ok",
        "unrelated_column": "ignored",
    }
    r.update(over)
    return r


def count(conn, table="edits_live"):
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def fetch(conn, rev_id, table="edits_live"):
    cur = conn.execute(f"SELECT risk_score, page_title FROM {table} WHERE rev_id = %s", (rev_id,))
    return cur.fetchone()


def test_upsert_twice_keeps_one_row_and_latest_score(pg_conn):
    upsert_edits_live(pg_conn, [row(rev_id=1, risk_score=0.1)])
    upsert_edits_live(pg_conn, [row(rev_id=1, risk_score=0.9)])
    assert fetch(pg_conn, 1)[0] == 0.9 and count(pg_conn) == 1


def test_upsert_returns_row_count_and_accepts_empty(pg_conn):
    assert upsert_edits_live(pg_conn, []) == 0
    assert upsert_edits_live(pg_conn, [row(rev_id=i) for i in range(3)]) == 3


def test_tricky_titles_are_stored_verbatim(pg_conn):
    title = "Robert'); DROP TABLE edits_live;-- / Đà Nẵng " + "x" * 300
    upsert_edits_live(pg_conn, [row(rev_id=9, page_title=title)])
    assert fetch(pg_conn, 9)[1] == title


def test_cleanup_removes_rows_past_ttl(pg_conn):
    upsert_edits_live(pg_conn, [row(rev_id=1, event_time=NOW - timedelta(hours=3))])
    upsert_edits_live(pg_conn, [row(rev_id=2, event_time=NOW - timedelta(minutes=10))])
    deleted = cleanup_ttl(pg_conn)
    assert deleted["edits_live"] == 1 and count(pg_conn) == 1


def test_cleanup_reports_every_table(pg_conn):
    assert set(cleanup_ttl(pg_conn)) == {
        "edits_live", "hot_pages", "risk_queue", "metrics_minute", "page_hourly"
    }


def test_risk_queue_keeps_only_top_50(pg_conn):
    upsert_risk_queue(pg_conn, [row(rev_id=i, risk_score=i / 100) for i in range(80)])
    assert count(pg_conn, "risk_queue") == 50
    low = pg_conn.execute("SELECT min(risk_score) FROM risk_queue").fetchone()[0]
    assert low == 0.30


def test_risk_queue_skips_rows_without_score(pg_conn):
    upsert_risk_queue(pg_conn, [row(rev_id=1, risk_score=None), row(rev_id=2)])
    assert count(pg_conn, "risk_queue") == 1


def test_hot_pages_and_metrics_upsert(pg_conn):
    hot = {"wiki": "enwiki", "page_title": "P", "window_end": NOW, "edit_count": 9,
           "distinct_users": 5, "ewma": 1.0, "z_score": 8.0, "summary": None}
    upsert_hot_pages(pg_conn, [hot, {**hot, "z_score": 9.0}])
    assert pg_conn.execute("SELECT count(*), max(z_score) FROM hot_pages").fetchone() == (1, 9.0)

    m = {"wiki": "enwiki", "minute": NOW, "edit_count": 10, "anon_count": 2,
         "byte_delta_sum": 100, "hot_page_count": 1, "high_risk_count": 0}
    upsert_metrics_minute(pg_conn, [m])
    upsert_metrics_minute(pg_conn, [{**m, "edit_count": 12}])
    assert pg_conn.execute("SELECT edit_count FROM metrics_minute").fetchone()[0] == 12
