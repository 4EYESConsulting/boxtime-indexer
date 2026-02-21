"""Entry point for the boxtime-indexer."""

import asyncio
import logging
import signal
from pathlib import Path

import aiohttp

from src.config import load_config
from src.csv_writer import load_prices
from src.fetcher import _get_json
from src.indexer import run_backfill

logger = logging.getLogger(__name__)


async def _wait_for_node(session: aiohttp.ClientSession, node_url: str) -> None:
    """Block until the Ergo node is reachable and has an active extra index."""
    logger.info("Waiting for node at %s to be ready...", node_url)
    while True:
        try:
            info = await _get_json(session, f"{node_url}/info")
            if info and info.get("fullHeight") is not None:
                indexed = await _get_json(
                    session, f"{node_url}/blockchain/indexedHeight"
                )
                if indexed and indexed.get("indexedHeight") is not None:
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
        await _wait_for_node(session, config.node_url)

        price_path = Path(config.price_csv_path)
        if not price_path.exists():
            raise FileNotFoundError(f"Price CSV not found: {config.price_csv_path}")

        logger.info("Loading price data from %s", config.price_csv_path)
        price_map, max_price_date = load_prices(config.price_csv_path)
        if max_price_date is None:
            raise ValueError("No price data found in CSV")

        # Run backfill - writes incrementally to CSV files
        await run_backfill(
            session=session,
            config=config,
            max_price_date=max_price_date,
            shutdown_event=shutdown_event,
            price_map=price_map,
        )

        logger.info("Indexing complete, exiting")


def main() -> None:
    """Sync entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
