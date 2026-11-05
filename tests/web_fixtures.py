"""Fixture Playwright: chạy API + website trên dữ liệu giả trong một luồng.

Cần ``playwright install chromium``. Thiếu trình duyệt thì test web bị bỏ qua.
"""

from __future__ import annotations

import socket
import threading
import time
from contextlib import contextmanager

import pytest


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_server(pg_dsn):
    uvicorn = pytest.importorskip("uvicorn")
    import psycopg

    from src.api.main import create_app
    from tests.conftest import ROOT
    from tests.seed_pg import seed

    schema = "web_" + str(_free_port())
    admin = psycopg.connect(pg_dsn, autocommit=True)
    admin.execute(f"CREATE SCHEMA {schema}")
    admin.execute(f"SET search_path TO {schema}")
    admin.execute((ROOT / "sql" / "schema.sql").read_text(encoding="utf-8"))
    seed(admin)

    @contextmanager
    def connection():
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(f"SET search_path TO {schema}")
            yield conn

    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(create_app(connection), port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=5)
    admin.execute(f"DROP SCHEMA {schema} CASCADE")
    admin.close()


@pytest.fixture(scope="module")
def browser():
    sync_api = pytest.importorskip("playwright.sync_api")
    with sync_api.sync_playwright() as p:
        try:
            b = p.chromium.launch()
        except Exception as exc:  # trình duyệt chưa cài
            pytest.skip(f"chromium chưa sẵn sàng: {exc.__class__.__name__}")
        yield b
        b.close()


@pytest.fixture
def page(browser):
    pg = browser.new_page()
    yield pg
    pg.close()
