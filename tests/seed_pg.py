"""Dữ liệu giả cho 5 bảng phục vụ — dùng cho test API/web và chạy demo local."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from src.stream import serving

TITLES = ["Typhoon Yagi", "Bão/Yagi", "2026 FIFA World Cup", "Đà Nẵng", "Python (programming language)"]


def seed(conn, now: datetime | None = None, seed_value: int = 1) -> None:
    rnd = random.Random(seed_value)
    now = (now or datetime.now(timezone.utc)).replace(second=0, microsecond=0)

    edits = []
    for i in range(120):
        t = now - timedelta(seconds=i * 50)
        title = TITLES[i % len(TITLES)]
        risk = round(rnd.random(), 3)
        edits.append({
            "wiki": "enwiki", "rev_id": 1_000_000 + i, "page_title": title,
            "user_hash": f"{rnd.getrandbits(64):016x}", "is_anon": i % 4 == 0,
            "comment": "copyedit" if i % 3 else "", "byte_delta": rnd.randint(-900, 900),
            "risk_score": risk, "model_version": "gbt-demo", "summary": f"Thêm {i % 30 + 1} từ",
            "event_time": t, "ingest_time": t + timedelta(seconds=2),
            "diff_added": "Some ADDED text" if i % 2 else "", "diff_removed": "old text",
            "diff_status": "ok",
        })
    with conn.transaction():
        serving.upsert_edits_live(conn, edits)
        serving.upsert_risk_queue(conn, edits)
        serving.upsert_hot_pages(conn, [
            {"wiki": "enwiki", "page_title": t, "window_end": now - timedelta(minutes=k),
             "edit_count": 12 - k, "distinct_users": 5, "ewma": 1.2, "z_score": 9.0 - k,
             "summary": "Thêm 40 từ"}
            for k, t in enumerate(TITLES[:3])
        ])
        serving.upsert_metrics_minute(conn, [
            {"wiki": "enwiki", "minute": now - timedelta(minutes=m), "edit_count": 20 + m % 7,
             "anon_count": 4 + m % 3, "byte_delta_sum": 1000 + m, "hot_page_count": m % 4,
             "high_risk_count": m % 5}
            for m in range(0, 360)
        ])
        serving.upsert_page_hourly(conn, [
            {"wiki": "enwiki", "page_title": t, "hour_bucket": (now - timedelta(hours=h)).replace(minute=0),
             "edit_count": 5 + (h * 7 + j) % 11, "distinct_users": 3, "total_byte_delta": 500,
             "anon_ratio": 0.25, "mean_risk_score": 0.3, "max_risk_score": 0.8}
            for j, t in enumerate(TITLES) for h in range(0, 24 * 7, 3)
        ])


if __name__ == "__main__":  # pragma: no cover - nạp dữ liệu demo: python -m tests.seed_pg
    import os

    import psycopg

    with psycopg.connect(os.environ["PG_DSN"], autocommit=True) as c:
        seed(c)
        print("seeded")
