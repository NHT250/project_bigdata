"""Ghi tầng phục vụ PostgreSQL (spec §5.4).

Postgres là CACHE cho website, không phải nơi lưu trữ gốc. Mọi ghi là upsert
theo khoá chính, nên chạy lại một batch (sau crash, sau replay) không sinh dòng
trùng. Mọi giá trị đi qua tham số ``%s`` — tên cột/bảng là hằng số trong file
này, không bao giờ lấy từ dữ liệu.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence

EDITS_LIVE_COLS = (
    "wiki", "rev_id", "page_title", "user_hash", "is_anon", "comment", "byte_delta",
    "risk_score", "model_version", "summary", "event_time", "ingest_time",
)
HOT_PAGES_COLS = (
    "wiki", "page_title", "window_end", "edit_count", "distinct_users", "ewma",
    "z_score", "summary",
)
RISK_QUEUE_COLS = (
    "wiki", "rev_id", "page_title", "user_hash", "is_anon", "risk_score",
    "model_version", "diff_added", "diff_removed", "diff_status", "summary", "event_time",
)
METRICS_MINUTE_COLS = (
    "wiki", "minute", "edit_count", "anon_count", "byte_delta_sum", "hot_page_count",
    "high_risk_count",
)
PAGE_HOURLY_COLS = (
    "wiki", "page_title", "hour_bucket", "edit_count", "distinct_users",
    "total_byte_delta", "anon_ratio", "mean_risk_score", "max_risk_score",
)

TTL = {
    "edits_live": ("event_time", "2 hours"),
    "hot_pages": ("window_end", "1 hour"),
    "risk_queue": ("event_time", "6 hours"),
    "metrics_minute": ("minute", "24 hours"),
    "page_hourly": ("hour_bucket", "30 days"),
}

DEFAULT_RISK_QUEUE_SIZE = 50


def _upsert(
    conn,
    table: str,
    cols: Sequence[str],
    key: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
) -> int:
    values = [tuple(r.get(c) for c in cols) for r in rows]
    if not values:
        return 0
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in key)
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) "
        f"VALUES ({', '.join(['%s'] * len(cols))}) "
        f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {updates}"
    )
    with conn.cursor() as cur:
        cur.executemany(sql, values)
    return len(values)


def upsert_edits_live(conn, rows) -> int:
    return _upsert(conn, "edits_live", EDITS_LIVE_COLS, ("wiki", "rev_id"), rows)


def upsert_hot_pages(conn, rows) -> int:
    return _upsert(
        conn, "hot_pages", HOT_PAGES_COLS, ("wiki", "page_title", "window_end"), rows
    )


def upsert_metrics_minute(conn, rows) -> int:
    """Thay thế cả dòng: người gọi phải tính lại TRỌN phút từ silver."""
    return _upsert(conn, "metrics_minute", METRICS_MINUTE_COLS, ("wiki", "minute"), rows)


def upsert_page_hourly(conn, rows) -> int:
    return _upsert(
        conn, "page_hourly", PAGE_HOURLY_COLS, ("wiki", "page_title", "hour_bucket"), rows
    )


def upsert_risk_queue(conn, rows, size: int = DEFAULT_RISK_QUEUE_SIZE) -> int:
    """Upsert rồi chỉ giữ ``size`` edit có ``risk_score`` cao nhất."""
    scored = [r for r in rows if r.get("risk_score") is not None]
    n = _upsert(conn, "risk_queue", RISK_QUEUE_COLS, ("wiki", "rev_id"), scored)
    conn.execute(
        """
        DELETE FROM risk_queue WHERE (wiki, rev_id) NOT IN (
            SELECT wiki, rev_id FROM risk_queue
            ORDER BY risk_score DESC, event_time DESC LIMIT %s
        )
        """,
        (size,),
    )
    return n


def cleanup_ttl(conn, now: datetime | None = None) -> dict[str, int]:
    """Xoá dòng quá TTL ở mọi bảng, trả số dòng đã xoá theo bảng."""
    deleted: dict[str, int] = {}
    for table, (col, ttl) in TTL.items():
        if now is None:
            cur = conn.execute(
                f"DELETE FROM {table} WHERE {col} < now() - %s::interval", (ttl,)
            )
        else:
            cur = conn.execute(
                f"DELETE FROM {table} WHERE {col} < %s::timestamptz - %s::interval",
                (now, ttl),
            )
        deleted[table] = cur.rowcount
    return deleted
