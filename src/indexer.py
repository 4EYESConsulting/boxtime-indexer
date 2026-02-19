"""Backfill indexing with price date limit."""

import asyncio
import logging
from datetime import date as date_type
from typing import Dict, List

import aiohttp

from src.config import Config
from src.csv_writer import append_output, get_max_height, merge_with_prices
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


def _has_genesis_row(data: List[HeightData]) -> bool:
    """Check if data contains a height-0 row."""
    return any(d.height == 0 for d in data)


def _write_genesis_if_needed(csv_path: str) -> None:
    """Write genesis row to CSV if file doesn't exist or is empty."""
    from pathlib import Path
    path = Path(csv_path)
    
    if not path.exists() or path.stat().st_size == 0:
        genesis = _make_genesis_row()
        genesis.block_date = date_type(2019, 7, 1)  # Genesis date
        append_output(csv_path, [genesis])
        logger.info("Wrote genesis row (height 0) to %s", csv_path)


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
    csv_output_path: str,
    start_height: int,
    chain_height: int,
    max_price_date: date_type,
    chunk_size: int,
    max_concurrent: int,
    shutdown_event: asyncio.Event,
    price_map: Dict[date_type, float],
) -> List[HeightData]:
    """Fetch blocks in chunks, merge prices, and write incrementally."""
    all_results: List[HeightData] = []
    start_time = asyncio.get_event_loop().time()

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

        if filtered_results:
            # Merge prices before writing
            merged_chunk = merge_with_prices(filtered_results, price_map)
            
            # Write incrementally
            append_output(csv_output_path, merged_chunk)
            all_results.extend(merged_chunk)

            # Calculate progress
            current_height = filtered_results[-1].height
            progress_pct = (current_height / chain_height) * 100
            remaining = chain_height - current_height
            
            # Calculate rate and ETA
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > 0:
                blocks_processed = len(all_results)
                rate = blocks_processed / elapsed
                eta_seconds = remaining / rate if rate > 0 else 0
                eta_str = _format_eta(eta_seconds)
                
                logger.info(
                    "Progress: %d / %d blocks (%.1f%%, ETA: %s at %.1f blocks/sec)",
                    current_height,
                    chain_height,
                    progress_pct,
                    eta_str,
                    rate,
                )
            else:
                logger.info(
                    "Indexed heights %d–%d",
                    chunk_start,
                    filtered_results[-1].height,
                )

        if stop_backfill:
            break

    return all_results


async def run_backfill(
    session: aiohttp.ClientSession,
    config: Config,
    bootstrap_data: List[HeightData],
    max_price_date: date_type,
    shutdown_event: asyncio.Event,
    price_map: Dict[date_type, float],
) -> List[HeightData]:
    """Run the backfill process and return all data for output.
    
    Writes data incrementally to CSV as chunks are processed.
    """
    chain_height = await _get_chain_height(session, config.node_url)
    
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

    # Write genesis row if starting fresh and height 0 is needed
    if config.start_height == 0 and not _has_genesis_row(bootstrap_data):
        _write_genesis_if_needed(config.csv_output_path)
        genesis = _make_genesis_row()
        genesis.block_date = date_type(2019, 7, 1)
        bootstrap_data = [genesis] + bootstrap_data

    # If already up to date, nothing to do
    if fetch_start > chain_height:
        logger.info("Already up to date at height %d", chain_height)
        return bootstrap_data

    new_data = await _fetch_and_write_chunks(
        session=session,
        node_url=config.node_url,
        csv_output_path=config.csv_output_path,
        start_height=fetch_start,
        chain_height=chain_height,
        max_price_date=max_price_date,
        chunk_size=config.chunk_size,
        max_concurrent=config.max_concurrent,
        shutdown_event=shutdown_event,
        price_map=price_map,
    )
    
    all_data = bootstrap_data + new_data

    logger.info(
        "Backfill complete: bootstrap=%d, new=%d, total=%d",
        len(bootstrap_data),
        len(new_data),
        len(all_data),
    )

    return all_data
