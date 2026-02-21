"""Backfill indexing with price date limit."""

import asyncio
import logging
from datetime import date as date_type
from typing import Dict, List, Optional

import aiohttp

from src.config import Config
from src.csv_writer import get_max_height, write_cointime_csv, write_prices_csv
from src.fetcher import HeightData, fetch_chunk, _get_json, find_first_height_by_date

logger = logging.getLogger(__name__)


def _format_eta(seconds: float) -> str:
    """Format seconds as human-readable ETA."""
    if seconds < 60:
        return f"{seconds:.0f} seconds"
    elif seconds < 3600:
        return f"{seconds/60:.1f} minutes"
    elif seconds < 86400:
        return f"{seconds/3600:.1f} hours"
    else:
        return f"{seconds/86400:.1f} days"


async def _get_chain_height(session: aiohttp.ClientSession, node_url: str) -> int:
    """Fetch the current indexed height from the node.

    Uses indexedHeight (not fullHeight) because heights above the extra
    index frontier don't have data available via /blockchain/block/byHeaderId.
    """
    indexed = await _get_json(session, f"{node_url}/blockchain/indexedHeight")
    if indexed is None or indexed.get("indexedHeight") is None:
        raise RuntimeError("Failed to fetch indexed height from node")
    return indexed["indexedHeight"]


async def _fetch_and_write_chunks(
    session: aiohttp.ClientSession,
    node_url: str,
    cointime_output_path: str,
    start_height: int,
    target_height: int,
    chunk_size: int,
    max_concurrent: int,
    shutdown_event: asyncio.Event,
) -> List[HeightData]:
    """Fetch blocks in chunks and write incrementally."""
    all_results: List[HeightData] = []
    start_time = asyncio.get_event_loop().time()

    logger.info(
        "Backfilling heights %d–%d",
        start_height,
        target_height,
    )

    for chunk_start in range(start_height, target_height + 1, chunk_size):
        if shutdown_event.is_set():
            logger.info("Shutdown requested, stopping backfill")
            break

        chunk_end = min(chunk_start + chunk_size - 1, target_height)
        heights = list(range(chunk_start, chunk_end + 1))

        results = await fetch_chunk(session, node_url, heights, max_concurrent)

        if not results:
            continue

        write_cointime_csv(cointime_output_path, results)
        all_results.extend(results)

        current_height = results[-1].height
        progress_pct = (current_height / target_height) * 100
        remaining = target_height - current_height
        
        elapsed = asyncio.get_event_loop().time() - start_time
        if elapsed > 0:
            blocks_processed = len(all_results)
            rate = blocks_processed / elapsed
            eta_seconds = remaining / rate if rate > 0 else 0
            eta_str = _format_eta(eta_seconds)
            
            logger.info(
                "Progress: %d / %d blocks (%.1f%%, ETA: %s at %.1f blocks/sec)",
                current_height,
                target_height,
                progress_pct,
                eta_str,
                rate,
            )
        else:
            logger.info(
                "Indexed heights %d–%d",
                chunk_start,
                results[-1].height,
            )

    return all_results


async def run_backfill(
    session: aiohttp.ClientSession,
    config: Config,
    max_price_date: date_type,
    shutdown_event: asyncio.Event,
    price_map: Dict[date_type, float],
) -> None:
    """Run the backfill process.

    Writes data incrementally to CSV as chunks are processed.
    End height is determined by the latest date in price data.
    """
    chain_height = await _get_chain_height(session, config.node_url)

    # Get max height from existing output file
    max_existing_height = get_max_height(config.cointime_output_path)
    start = (max_existing_height + 1) if max_existing_height is not None else config.start_height
    fetch_start = max(start, 1)

    # Find first height that corresponds to the target date
    target_height = await find_first_height_by_date(
        session, config.node_url, fetch_start, chain_height, max_price_date
    )

    logger.info(
        "Starting backfill: existing max height=%s, start=%d, fetch_start=%d, target_height=%d (max_price_date=%s)",
        max_existing_height,
        start,
        fetch_start,
        target_height,
        max_price_date,
    )

    if fetch_start > target_height:
        logger.info("Already up to date at target height %d", target_height)
        write_prices_csv(config.prices_output_path, price_map)
        return

    new_data = await _fetch_and_write_chunks(
        session=session,
        node_url=config.node_url,
        cointime_output_path=config.cointime_output_path,
        start_height=fetch_start,
        target_height=target_height,
        chunk_size=config.chunk_size,
        max_concurrent=config.max_concurrent,
        shutdown_event=shutdown_event,
    )

    # Write prices after all cointime data is written
    write_prices_csv(config.prices_output_path, price_map)

    logger.info(
        "Backfill complete: indexed %d heights from %d to %d",
        len(new_data),
        fetch_start,
        target_height,
    )
