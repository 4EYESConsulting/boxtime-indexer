"""PostgreSQL init and query helpers."""

import asyncio
import logging
import ssl
from urllib.parse import urlparse
from typing import List, Optional, Sequence, Tuple

import asyncpg

logger = logging.getLogger(__name__)

_DB_CONNECT_RETRIES = 30
_DB_CONNECT_INTERVAL = 2  # seconds

_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "db"})

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS cointime (
    height    INTEGER PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    cbc       NUMERIC NOT NULL,
    cbd       NUMERIC NOT NULL,
    cbs       NUMERIC NOT NULL
);
"""

_UPSERT = """
INSERT INTO cointime (height, timestamp, cbc, cbd, cbs)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (height) DO UPDATE SET
    timestamp = EXCLUDED.timestamp,
    cbc       = EXCLUDED.cbc,
    cbd       = EXCLUDED.cbd,
    cbs       = EXCLUDED.cbs;
"""

# Row type: (height, timestamp, cbc, cbd, cbs)
Row = Tuple[int, int, int, int, int]


def _needs_ssl(database_url: str) -> bool:
    """Return True if the database URL points to a remote host."""
    parsed = urlparse(database_url)
    hostname = (parsed.hostname or "").lower()
    return hostname not in _LOCAL_HOSTS


async def init_db(database_url: str) -> asyncpg.Pool:
    """Create a connection pool and ensure the cointime table exists.

    Retries the connection to tolerate a database that is still starting up.
    Enables SSL automatically for remote hosts (required by providers
    like Supabase, Neon, etc.).
    """
    kwargs = {}
    if _needs_ssl(database_url):
        ssl_ctx = ssl.create_default_context()
        kwargs["ssl"] = ssl_ctx
        logger.info("SSL enabled for remote database connection")

    for attempt in range(1, _DB_CONNECT_RETRIES + 1):
        try:
            pool = await asyncpg.create_pool(database_url, **kwargs)
            async with pool.acquire() as conn:
                await conn.execute(_CREATE_TABLE)
            logger.info("Database initialized")
            return pool
        except (OSError, asyncpg.PostgresError) as exc:
            if attempt == _DB_CONNECT_RETRIES:
                raise
            logger.info(
                "Database not ready (attempt %d/%d): %s",
                attempt,
                _DB_CONNECT_RETRIES,
                exc,
            )
            await asyncio.sleep(_DB_CONNECT_INTERVAL)
    raise RuntimeError("Unreachable")


async def get_max_height(pool: asyncpg.Pool) -> Optional[int]:
    """Return the maximum indexed height, or None if the table is empty."""
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT MAX(height) FROM cointime")


async def get_missing_heights(pool: asyncpg.Pool) -> List[int]:
    """Return all heights missing between 1 and MAX(height)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT s.h
            FROM generate_series(1, (SELECT MAX(height) FROM cointime)) AS s(h)
            LEFT JOIN cointime c ON c.height = s.h
            WHERE c.height IS NULL
            ORDER BY s.h
            """
        )
    return [r["h"] for r in rows]


async def upsert_batch(pool: asyncpg.Pool, rows: Sequence[Row]) -> None:
    """Batch upsert rows into the cointime table."""
    if not rows:
        return
    async with pool.acquire() as conn:
        await conn.executemany(_UPSERT, rows)


async def delete_from_height(pool: asyncpg.Pool, height: int) -> int:
    """Delete all rows at or above the given height. Returns count deleted."""
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM cointime WHERE height >= $1", height
        )
    count = int(result.split()[-1])
    logger.info("Deleted %d rows from height %d onward", count, height)
    return count


async def insert_genesis(pool: asyncpg.Pool, timestamp: int) -> None:
    """Insert height 0 (genesis) with zeroed metrics if not present."""
    async with pool.acquire() as conn:
        await conn.execute(
            _UPSERT,
            0, timestamp, 0, 0, 0,
        )
    logger.info("Genesis row (height 0) ensured")
