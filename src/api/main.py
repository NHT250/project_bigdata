"""FastAPI: 6 endpoint đọc Postgres + serve website tĩnh (spec §7).

Chạy: ``uvicorn src.api.main:app --host 0.0.0.0 --port 8000`` (cần ``PG_DSN``).
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row

from src.api import queries as q

WEB_DIR = Path(__file__).resolve().parents[1] / "web"


def _envelope(**body) -> dict:
    return {"license": q.LICENSE, "license_url": q.LICENSE_URL, "source": q.SOURCE, **body}


def _clean(row: dict) -> dict:
    """``numeric`` của Postgres về ``float`` để JSON gọn."""
    return {k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()}


def create_app(connection) -> FastAPI:
    """``connection``: context manager trả một kết nối psycopg (pool hoặc test)."""
    app = FastAPI(title="WikiPulse", version="1.0")

    def fetch(sql: str, params: dict | None = None) -> list[dict]:
        with connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, params or {})
                return [_clean(r) for r in cur.fetchall()]

    @app.get("/api/metrics")
    def metrics(hours: int = Query(6, ge=1, le=48)):
        points = fetch(q.METRICS, {"hours": hours})
        summary = fetch(q.METRICS_SUMMARY, {"threshold": q.HIGH_RISK_THRESHOLD})[0]
        return _envelope(points=points, summary=summary, high_risk_threshold=q.HIGH_RISK_THRESHOLD)

    @app.get("/api/hot")
    def hot(limit: int = Query(20, ge=1, le=100)):
        items = fetch(q.HOT, {"limit": limit})
        for it in items:
            it["url"] = q.article_url(it["wiki"], it["page_title"])
        return _envelope(items=items)

    @app.get("/api/risk")
    def risk(limit: int = Query(50, ge=1, le=50)):
        items = fetch(q.RISK, {"limit": limit})
        for it in items:
            it["url"] = q.article_url(it["wiki"], it["page_title"])
            it["diff_url"] = q.diff_url(it["wiki"], it["rev_id"])
        return _envelope(items=items)

    @app.get("/api/live")
    def live(limit: int = Query(100, ge=1, le=200)):
        items = fetch(q.LIVE, {"limit": limit})
        for it in items:
            it["url"] = q.article_url(it["wiki"], it["page_title"])
            it["diff_url"] = q.diff_url(it["wiki"], it["rev_id"])
        return _envelope(items=items)

    @app.get("/api/page")
    def page(title: str = Query(..., min_length=1, max_length=512), days: int = Query(7, ge=1, le=30)):
        # Người dùng hay dán tên từ URL (Typhoon_Yagi); EventStreams dùng dấu cách.
        normalized = title.strip().replace("_", " ")
        params = {"title": normalized, "days": days}
        return _envelope(
            title=normalized,
            url=q.article_url("enwiki", normalized),
            points=fetch(q.PAGE_HOURLY, params),
            edits=fetch(q.PAGE_EDITS, params),
        )

    @app.get("/api/questions")
    def questions():
        return _envelope(items=[{"qid": k, "title": v[0]} for k, v in sorted(q.CANNED.items())])

    @app.get("/api/ask")
    def ask(qid: int = Query(...)):
        if qid not in q.CANNED:
            raise HTTPException(status_code=400, detail="qid phải trong khoảng 1..5")
        title, sql = q.CANNED[qid]
        rows = fetch(sql)
        columns = list(rows[0].keys()) if rows else []
        return _envelope(qid=qid, title=title, columns=columns, rows=[list(r.values()) for r in rows])

    if WEB_DIR.is_dir():
        app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")
    return app


def _app_from_env() -> FastAPI:  # pragma: no cover - chạy thật
    from src.api.db import pool_connection_factory

    return create_app(pool_connection_factory(os.environ["PG_DSN"]))


# uvicorn src.api.main:app — chỉ dựng khi có PG_DSN để test import được module.
app = _app_from_env() if os.environ.get("PG_DSN") else None
