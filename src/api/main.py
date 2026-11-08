"""FastAPI: 6 endpoint đọc Postgres + serve website tĩnh (spec §7).

Chạy: ``uvicorn src.api.main:app --host 0.0.0.0 --port 8000`` (cần ``PG_DSN``).
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import threading
import time
from collections import deque

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

from src.api import nl2sql
from src.api import queries as q

WEB_DIR = Path(__file__).resolve().parents[1] / "web"


def _envelope(**body) -> dict:
    return {"license": q.LICENSE, "license_url": q.LICENSE_URL, "source": q.SOURCE, **body}


def _clean(row: dict) -> dict:
    """``numeric`` của Postgres về ``float`` để JSON gọn."""
    return {k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()}


class ChatRequest(BaseModel):
    question: str = Field(min_length=1, max_length=500)


def create_app(
    connection,
    chat_connection=None,
    llm=None,
    chat_max_per_minute: int = 20,
) -> FastAPI:
    """``connection``: context manager trả một kết nối psycopg (pool hoặc test).

    ``chat_connection`` phải đăng nhập bằng role chỉ đọc ``wiki_chat``. Không có
    thì chat không chạy, tuyệt đối không dùng tạm ``connection``.
    """
    app = FastAPI(title="WikiPulse", version="1.0")
    chat_calls: deque[float] = deque()
    chat_lock = threading.Lock()

    def canned_questions() -> list[dict]:
        return [{"qid": k, "title": v[0]} for k, v in sorted(q.CANNED.items())]

    def allow_chat_call() -> bool:
        """Mỗi câu chat tốn tiền API và cổng 8000 mở công khai."""
        now = time.monotonic()
        with chat_lock:
            while chat_calls and now - chat_calls[0] >= 60:
                chat_calls.popleft()
            if len(chat_calls) >= chat_max_per_minute:
                return False
            chat_calls.append(now)
            return True

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

    @app.post("/api/chat")
    def chat(body: ChatRequest):
        if chat_connection is None or llm is None:
            result = {"sql": "", "rows": [], "columns": [], "error": nl2sql.UNAVAILABLE}
        else:
            if not allow_chat_call():
                raise HTTPException(
                    status_code=429,
                    detail=f"Chat đang nhận quá nhiều câu hỏi. Thử lại sau một phút. {nl2sql.HINT}",
                )
            with chat_connection() as conn:
                result = nl2sql.answer(body.question, conn, llm)
        return _envelope(**result, canned=canned_questions() if result["error"] else [])

    if WEB_DIR.is_dir():
        app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")
    return app


def _app_from_env() -> FastAPI:  # pragma: no cover - chạy thật
    from src.api.db import pool_connection_factory

    chat_dsn = os.environ.get("CHAT_PG_DSN", "").strip()
    return create_app(
        pool_connection_factory(os.environ["PG_DSN"]),
        chat_connection=pool_connection_factory(chat_dsn, max_size=2) if chat_dsn else None,
        llm=nl2sql.llm_from_env(),
    )


# uvicorn src.api.main:app — chỉ dựng khi có PG_DSN để test import được module.
app = _app_from_env() if os.environ.get("PG_DSN") else None
