"""Chunked processing loop with reorg detection."""

import asyncio
import logging
from typing import Optional

import aiohttp
import asyncpg

from src.config import Config
from src.db import (
    delete_from_height,
    get_max_height,
    get_missing_heights,
    upsert_batch,
)
from src.fetcher import fetch_chunk, _get_json
from src.price_fetcher import sync_latest_prices

logger = logging.getLogger(__name__)


async def _get_chain_height(session: aiohttp.ClientSession, node_url: str) -> int:
    """Fetch the current indexed height from the node.

    Uses indexedHeight (not fullHeight) because heights above the extra
    index frontier don't have data available via /blockchain/block/byHeaderId.
    """
    indexed = await _get_json(session, f"{node_url}/blockchain/indexedHeight")
    if indexed is None or indexed.get("indexedHeight") is None:
        raise RuntimeError("Failed to fetch indexed height from node")
    return indexed["indexedHeight"]


async def _get_block_header(
    session: aiohttp.ClientSession, node_url: str, height: int
) -> Optional[dict]:
    """Fetch the block header at a given height."""
    header_ids = await _get_json(session, f"{node_url}/blocks/at/{height}")
    if not header_ids:
        return None
    header_id = header_ids[0] if isinstance(header_ids, list) else header_ids
    return await _get_json(session, f"{node_url}/blocks/{header_id}/header")


async def gap_fill(
    session: aiohttp.ClientSession,
    pool: asyncpg.Pool,
    config: Config,
) -> None:
    """Find and fill any missing heights in the database."""
    missing = await get_missing_heights(pool)
    if not missing:
        return
    logger.info("Found %d missing heights, filling gaps", len(missing))
    for i in range(0, len(missing), config.chunk_size):
        chunk = missing[i : i + config.chunk_size]
        results = await fetch_chunk(
            session, config.node_url, chunk, config.max_concurrent
        )
        if results:
            await upsert_batch(pool, [r.as_row() for r in results])
            logger.info(
                "Gap-filled %d/%d heights", min(i + config.chunk_size, len(missing)), len(missing)
            )


async def backfill(
    session: aiohttp.ClientSession,
    pool: asyncpg.Pool,
    config: Config,
    shutdown_event: asyncio.Event,
) -> None:
    """Process all heights from the resume point to the chain tip."""
    max_height = await get_max_height(pool)
    start = (max_height + 1) if max_height is not None else config.start_height
    chain_height = await _get_chain_height(session, config.node_url)

    if start > chain_height:
        logger.info("Already up to date at height %d", chain_height)
        return

    total = chain_height - start + 1
    processed = 0
    logger.info("Backfilling heights %d–%d (%d total)", start, chain_height, total)

    for chunk_start in range(start, chain_height + 1, config.chunk_size):
        if shutdown_event.is_set():
            logger.info("Shutdown requested, stopping backfill")
            return

        chunk_end = min(chunk_start + config.chunk_size - 1, chain_height)
        heights = list(range(chunk_start, chunk_end + 1))

        results = await fetch_chunk(
            session, config.node_url, heights, config.max_concurrent
        )
        if results:
            await upsert_batch(pool, [r.as_row() for r in results])

        processed += len(heights)
        pct = processed * 100 / total
        logger.info(
            "Indexed heights %d–%d (%.1f%%)", chunk_start, chunk_end, pct
        )


async def _detect_reorg(
    session: aiohttp.ClientSession, node_url: str, height: int
) -> bool:
    """Check if there is a reorg at the given height.

    Compares the parentId of the block at `height` with the id of the
    block at `height - 1`. Returns True if a reorg is detected.
    """
    if height <= 1:
        return False

    header = await _get_block_header(session, node_url, height)
    prev_header = await _get_block_header(session, node_url, height - 1)

    if header is None or prev_header is None:
        logger.warning("Could not fetch headers for reorg check at height %d", height)
        return False

    if header["parentId"] != prev_header["id"]:
        logger.warning(
            "Reorg detected at height %d: parentId=%s != prev.id=%s",
            height,
            header["parentId"],
            prev_header["id"],
        )
        return True
    return False


async def poll_loop(
    session: aiohttp.ClientSession,
    pool: asyncpg.Pool,
    config: Config,
    shutdown_event: asyncio.Event,
) -> None:
    """Poll for new blocks and index them, with reorg detection."""
    logger.info("Entering poll loop (interval=%ds)", config.poll_interval)

    while not shutdown_event.is_set():
        try:
            max_height = await get_max_height(pool)
            if max_height is None:
                max_height = 0

            chain_height = await _get_chain_height(session, config.node_url)

            if chain_height <= max_height:
                await asyncio.sleep(config.poll_interval)
                continue

            next_height = max_height + 1

            # Check for reorg before indexing
            if await _detect_reorg(session, config.node_url, next_height):
                # Walk back to find the fork point
                fork_height = next_height
                while fork_height > 1:
                    fork_height -= 1
                    if not await _detect_reorg(
                        session, config.node_url, fork_height
                    ):
                        break
                logger.info("Rolling back to fork point at height %d", fork_height)
                await delete_from_height(pool, fork_height)
                continue  # Re-enter loop to re-process from fork point

            # Sync latest price data
            if config.coingecko_api_key:
                try:
                    await sync_latest_prices(pool, config)
                except Exception:
                    logger.exception("Error syncing price data")

            # Index new blocks in chunks
            heights = list(range(next_height, chain_height + 1))
            for i in range(0, len(heights), config.chunk_size):
                if shutdown_event.is_set():
                    return
                chunk = heights[i : i + config.chunk_size]
                results = await fetch_chunk(
                    session, config.node_url, chunk, config.max_concurrent
                )
                if results:
                    await upsert_batch(pool, [r.as_row() for r in results])
                    logger.info(
                        "Poll: indexed heights %d–%d",
                        chunk[0],
                        chunk[-1],
                    )

        except Exception:
            logger.exception("Error in poll loop")

        await asyncio.sleep(config.poll_interval)
