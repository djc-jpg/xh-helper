from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from config import settings

pool: ConnectionPool | None = None


def init_pool() -> None:
    global pool
    if pool is None:
        pool = ConnectionPool(
            settings.database_url,
            min_size=1,
            max_size=10,
            kwargs={"autocommit": True},
            open=True,
        )


def close_pool() -> None:
    global pool
    if pool is not None:
        pool.close()
        pool = None


def _p() -> ConnectionPool:
    if pool is None:
        init_pool()
    assert pool is not None
    return pool


def execute(query: str, params: Any = None) -> int:
    p = _p()
    with p.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.rowcount


def fetchone(query: str, params: Any = None) -> dict[str, Any] | None:
    p = _p()
    with p.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params or ())
            return cur.fetchone()
