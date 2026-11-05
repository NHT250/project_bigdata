"""Pool kết nối PostgreSQL cho API. API CHỈ đọc Postgres — không Spark trên
đường request (spec §3.2)."""

from __future__ import annotations

from contextlib import contextmanager


def pool_connection_factory(dsn: str, min_size: int = 1, max_size: int = 8):
    from psycopg_pool import ConnectionPool

    pool = ConnectionPool(
        dsn, min_size=min_size, max_size=max_size, kwargs={"autocommit": True}, open=True
    )

    @contextmanager
    def connection():
        with pool.connection() as conn:
            yield conn

    connection.pool = pool  # type: ignore[attr-defined]
    return connection
