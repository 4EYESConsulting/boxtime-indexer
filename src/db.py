"""PostgreSQL init and query helpers."""

import logging
from typing import List, Optional, Sequence, Tuple

import asyncpg

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS cointime (
    height    INTEGER PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    cbc       BIGINT NOT NULL,
    cbd       BIGINT NOT NULL,
    cbs       BIGINT NOT NULL
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


async def init_db(database_url: str) -> asyncpg.Pool:
    """Create a connection pool and ensure the cointime table exists."""
    pool = await asyncpg.create_pool(database_url)
    async with pool.acquire() as conn:
        await conn.execute(_CREATE_TABLE)
    logger.info("Database initialized")
    return pool


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
