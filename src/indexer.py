"""Backfill indexing with price date limit."""

import asyncio
import logging
from datetime import date as date_type
from typing import List

import aiohttp

from src.config import Config
from src.csv_writer import get_max_height
from src.fetcher import HeightData, fetch_chunk, _get_json

logger = logging.getLogger(__name__)
_GENESIS_TIMESTAMP = 1561978800000


def _make_genesis_row() -> HeightData:
    """Build a synthetic genesis placeholder row for height 0."""
    return HeightData(
        height=0,
        timestamp=_GENESIS_TIMESTAMP,
        cbc=0,
        cbd=0,
        cbs=0,
    )


def _ensure_genesis_row(data: List[HeightData]) -> List[HeightData]:
    """Ensure output data contains a height-0 row exactly once by prepending."""
    if any(d.height == 0 for d in data):
        return data
    return [_make_genesis_row(), *data]


async def _get_chain_height(session: aiohttp.ClientSession, node_url: str) -> int:
    """Fetch the current indexed height from the node.

    Uses indexedHeight (not fullHeight) because heights above the extra
    index frontier don't have data available via /blockchain/block/byHeaderId.
    """
    indexed = await _get_json(session, f"{node_url}/blockchain/indexedHeight")
    if indexed is None or indexed.get("indexedHeight") is None:
        raise RuntimeError("Failed to fetch indexed height from node")
    return indexed["indexedHeight"]


async def _fetch_until_date(
    session: aiohttp.ClientSession,
    node_url: str,
    start_height: int,
    max_price_date: date_type,
    chunk_size: int,
    max_concurrent: int,
    shutdown_event: asyncio.Event,
) -> List[HeightData]:
    """Fetch blocks from start_height until block_date exceeds max_price_date."""
    chain_height = await _get_chain_height(session, node_url)

    if start_height > chain_height:
        logger.info("Already up to date at height %d", chain_height)
        return []

    all_results: List[HeightData] = []

    logger.info(
        "Backfilling heights %d–%d until block date %s",
        start_height,
        chain_height,
        max_price_date,
    )

    for chunk_start in range(start_height, chain_height + 1, chunk_size):
        if shutdown_event.is_set():
            logger.info("Shutdown requested, stopping backfill")
            break

        chunk_end = min(chunk_start + chunk_size - 1, chain_height)
        heights = list(range(chunk_start, chunk_end + 1))

        results = await fetch_chunk(session, node_url, heights, max_concurrent)

        if not results:
            continue

        filtered_results: List[HeightData] = []
        stop_backfill = False

        for r in results:
            if r.block_date > max_price_date:
                logger.info(
                    "Reached price date limit at height %d (block_date=%s > max_price_date=%s)",
                    r.height,
                    r.block_date,
                    max_price_date,
                )
                stop_backfill = True
                break
            filtered_results.append(r)

        all_results.extend(filtered_results)

        if stop_backfill:
            break

        logger.info("Indexed heights %d–%d", chunk_start, chunk_end)

    return all_results


async def run_backfill(
    session: aiohttp.ClientSession,
    config: Config,
    bootstrap_data: List[HeightData],
    max_price_date: date_type,
    shutdown_event: asyncio.Event,
) -> List[HeightData]:
    """Run the backfill process and return all data for output."""
    max_bootstrap_height = get_max_height(bootstrap_data)
    start = (max_bootstrap_height + 1) if max_bootstrap_height is not None else config.start_height
    fetch_start = max(start, 1)

    logger.info(
        "Starting backfill: bootstrap max height=%s, start=%d, fetch_start=%d, max_price_date=%s",
        max_bootstrap_height,
        start,
        fetch_start,
        max_price_date,
    )

    new_data = await _fetch_until_date(
        session=session,
        node_url=config.node_url,
        start_height=fetch_start,
        max_price_date=max_price_date,
        chunk_size=config.chunk_size,
        max_concurrent=config.max_concurrent,
        shutdown_event=shutdown_event,
    )
    all_data = bootstrap_data + new_data
    if config.start_height == 0:
        all_data = _ensure_genesis_row(all_data)

    logger.info(
        "Backfill complete: bootstrap=%d, new=%d, total=%d",
        len(bootstrap_data),
        len(new_data),
        len(all_data),
    )

    return all_data
