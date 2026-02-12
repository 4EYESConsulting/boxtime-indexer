"""Fetch daily ERG/USD prices from CoinGecko and persist to the database."""

import datetime
import logging
from typing import List, Tuple

import asyncpg
from coingecko_sdk import AsyncCoingecko

from src.config import Config
from src.db import PriceRow, get_latest_price_date, upsert_prices_batch

logger = logging.getLogger(__name__)


def _create_client(config: Config) -> AsyncCoingecko:
    """Create a CoinGecko SDK client based on config."""
    if config.coingecko_pro:
        return AsyncCoingecko(pro_api_key=config.coingecko_api_key)
    return AsyncCoingecko(
        demo_api_key=config.coingecko_api_key, environment="demo"
    )


def _parse_prices(
    raw: List[List[float]],
) -> List[PriceRow]:
    """Convert CoinGecko [[timestamp_ms, price], ...] to [(date, price), ...].

    Deduplicates by date, keeping the last entry for each day.
    """
    by_date: dict[datetime.date, float] = {}
    for ts_ms, price in raw:
        dt = datetime.datetime.fromtimestamp(
            ts_ms / 1000, tz=datetime.timezone.utc
        )
        by_date[dt.date()] = price
    return sorted(by_date.items())


async def _fetch_market_chart(
    client: AsyncCoingecko, days: str
) -> List[PriceRow]:
    """Fetch daily ERG/USD prices from CoinGecko market chart endpoint."""
    response = await client.coins.market_chart.get(
        "ergo", days=days, vs_currency="usd", interval="daily"
    )
    if not response.prices:
        return []
    return _parse_prices(response.prices)


async def backfill_prices(
    pool: asyncpg.Pool, config: Config
) -> None:
    """Fetch all available daily price history and store in the database."""
    client = _create_client(config)

    logger.info("Starting price backfill (days=max)")
    rows = await _fetch_market_chart(client, "max")
    if not rows:
        logger.warning("No price data returned from CoinGecko")
        return

    await upsert_prices_batch(pool, rows)
    logger.info(
        "Price backfill complete: %d days (%s to %s)",
        len(rows),
        rows[0][0],
        rows[-1][0],
    )


async def sync_latest_prices(
    pool: asyncpg.Pool, config: Config
) -> None:
    """Fetch recent daily prices since the last stored date."""
    latest = await get_latest_price_date(pool)
    if latest is None:
        # No price data at all — run a full backfill instead.
        await backfill_prices(pool, config)
        return

    today = datetime.date.today()
    gap = (today - latest).days
    if gap <= 0:
        return

    client = _create_client(config)
    # Fetch enough days to cover the gap (add 1 for overlap).
    rows = await _fetch_market_chart(client, str(gap + 1))
    if not rows:
        return

    await upsert_prices_batch(pool, rows)
    logger.info("Price sync: upserted %d days up to %s", len(rows), rows[-1][0])
