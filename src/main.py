"""Entry point for the boxtime-indexer."""

import asyncio
import logging
import signal

import aiohttp

from src.config import load_config
from src.db import init_db, insert_genesis
from src.fetcher import _get_json
from src.indexer import backfill, gap_fill, poll_loop

logger = logging.getLogger(__name__)

# Ergo genesis block timestamp (mainnet block 0)
_GENESIS_TIMESTAMP = 1561978800000


async def _wait_for_node(session: aiohttp.ClientSession, node_url: str) -> None:
    """Block until the Ergo node is reachable and has an active extra index."""
    logger.info("Waiting for node at %s to be ready...", node_url)
    while True:
        try:
            info = await _get_json(session, f"{node_url}/info")
            if info and info.get("fullHeight"):
                indexed = await _get_json(
                    session, f"{node_url}/blockchain/indexedHeight"
                )
                if indexed and indexed.get("indexedHeight"):
                    indexed_height = indexed["indexedHeight"]
                    full_height = info["fullHeight"]
                    logger.info(
                        "Node ready: fullHeight=%d, indexedHeight=%d",
                        full_height,
                        indexed_height,
                    )
                    return
                logger.info("Node reachable but extra index not yet available")
        except Exception:
            logger.debug("Node not reachable yet")
        await asyncio.sleep(10)


async def run() -> None:
    """Main async entry point."""
    config = load_config()
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    connector = aiohttp.TCPConnector(limit=config.max_concurrent)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Wait for the Ergo node to be synced
        await _wait_for_node(session, config.node_url)

        # Initialize database
        pool = await init_db(config.database_url)
        try:
            # Insert genesis row
            await insert_genesis(pool, _GENESIS_TIMESTAMP)

            # Fill any gaps from previous runs
            await gap_fill(session, pool, config)

            # Backfill to chain tip
            await backfill(session, pool, config, shutdown_event)

            # Enter poll loop
            if not shutdown_event.is_set():
                await poll_loop(session, pool, config, shutdown_event)
        finally:
            await pool.close()
            logger.info("Database pool closed")


def main() -> None:
    """Sync entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
