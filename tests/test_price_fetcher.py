"""Tests for src.price_fetcher."""

import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import Config
from src.price_fetcher import (
    _create_client,
    _fetch_market_chart,
    _parse_prices,
    backfill_prices,
    sync_latest_prices,
)


def _make_config(api_key="test-key", pro=False):
    return Config(
        node_url="http://node:9053",
        database_url="postgresql://x",
        coingecko_api_key=api_key,
        coingecko_pro=pro,
        chunk_size=5000,
        max_concurrent=50,
        poll_interval=60,
        start_height=1,
    )


# ---------------------------------------------------------------------------
# _parse_prices
# ---------------------------------------------------------------------------


def test_parse_prices_basic():
    """Converts timestamp_ms/price pairs to (date, price) tuples."""
    raw = [
        [1609459200000.0, 1.50],  # 2021-01-01 00:00 UTC
        [1609545600000.0, 2.00],  # 2021-01-02 00:00 UTC
    ]
    result = _parse_prices(raw)
    assert result == [
        (datetime.date(2021, 1, 1), 1.50),
        (datetime.date(2021, 1, 2), 2.00),
    ]


def test_parse_prices_deduplicates_by_date():
    """Keeps the last entry when multiple timestamps fall on the same date."""
    raw = [
        [1609459200000.0, 1.50],  # 2021-01-01 00:00 UTC
        [1609502400000.0, 1.75],  # 2021-01-01 12:00 UTC
        [1609545600000.0, 2.00],  # 2021-01-02 00:00 UTC
    ]
    result = _parse_prices(raw)
    assert len(result) == 2
    assert result[0] == (datetime.date(2021, 1, 1), 1.75)
    assert result[1] == (datetime.date(2021, 1, 2), 2.00)


def test_parse_prices_empty():
    assert _parse_prices([]) == []


# ---------------------------------------------------------------------------
# _create_client
# ---------------------------------------------------------------------------


def test_create_client_demo():
    """Demo config creates client with demo_api_key."""
    with patch("src.price_fetcher.Coingecko") as MockCG:
        _create_client(_make_config(pro=False))
        MockCG.assert_called_once_with(
            demo_api_key="test-key", environment="demo"
        )


def test_create_client_pro():
    """Pro config creates client with pro_api_key."""
    with patch("src.price_fetcher.Coingecko") as MockCG:
        _create_client(_make_config(pro=True))
        MockCG.assert_called_once_with(pro_api_key="test-key")


# ---------------------------------------------------------------------------
# _fetch_market_chart
# ---------------------------------------------------------------------------


def test_fetch_market_chart_parses_response():
    """Calls market_chart.get and parses the prices list."""
    mock_client = MagicMock()
    mock_client.coins.market_chart.get.return_value = SimpleNamespace(
        prices=[
            [1609459200000.0, 1.50],
            [1609545600000.0, 2.00],
        ]
    )
    result = _fetch_market_chart(mock_client, "max")
    mock_client.coins.market_chart.get.assert_called_once_with(
        "ergo", days="max", vs_currency="usd", interval="daily"
    )
    assert len(result) == 2


def test_fetch_market_chart_empty_prices():
    """Returns empty list when prices is None."""
    mock_client = MagicMock()
    mock_client.coins.market_chart.get.return_value = SimpleNamespace(
        prices=None
    )
    assert _fetch_market_chart(mock_client, "30") == []


# ---------------------------------------------------------------------------
# backfill_prices
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_prices():
    """backfill_prices fetches max history and upserts to DB."""
    pool = AsyncMock()
    config = _make_config()

    fake_rows = [
        (datetime.date(2021, 1, 1), 1.50),
        (datetime.date(2021, 1, 2), 2.00),
    ]

    with patch(
        "src.price_fetcher._fetch_market_chart", return_value=fake_rows
    ) as mock_fetch, patch(
        "src.price_fetcher.upsert_prices_batch"
    ) as mock_upsert, patch(
        "src.price_fetcher._create_client"
    ):
        await backfill_prices(pool, config)

    mock_fetch.assert_called_once()
    assert mock_fetch.call_args[0][1] == "max"
    mock_upsert.assert_awaited_once_with(pool, fake_rows)


@pytest.mark.asyncio
async def test_backfill_prices_no_data():
    """backfill_prices handles empty response gracefully."""
    pool = AsyncMock()
    config = _make_config()

    with patch(
        "src.price_fetcher._fetch_market_chart", return_value=[]
    ), patch(
        "src.price_fetcher.upsert_prices_batch"
    ) as mock_upsert, patch(
        "src.price_fetcher._create_client"
    ):
        await backfill_prices(pool, config)

    mock_upsert.assert_not_awaited()


# ---------------------------------------------------------------------------
# sync_latest_prices
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_latest_prices_no_existing_data():
    """Falls back to backfill when no price data exists."""
    pool = AsyncMock()
    config = _make_config()

    with patch(
        "src.price_fetcher.get_latest_price_date", return_value=None
    ), patch(
        "src.price_fetcher.backfill_prices"
    ) as mock_backfill:
        await sync_latest_prices(pool, config)

    mock_backfill.assert_awaited_once_with(pool, config)


@pytest.mark.asyncio
async def test_sync_latest_prices_up_to_date():
    """No-op when latest date is today."""
    pool = AsyncMock()
    config = _make_config()

    with patch(
        "src.price_fetcher.get_latest_price_date",
        return_value=datetime.date.today(),
    ), patch(
        "src.price_fetcher._create_client"
    ) as mock_client:
        await sync_latest_prices(pool, config)

    mock_client.assert_not_called()


@pytest.mark.asyncio
async def test_sync_latest_prices_fetches_gap():
    """Fetches the gap between latest stored date and today."""
    pool = AsyncMock()
    config = _make_config()
    latest = datetime.date.today() - datetime.timedelta(days=5)

    fake_rows = [(datetime.date.today(), 3.00)]

    with patch(
        "src.price_fetcher.get_latest_price_date", return_value=latest
    ), patch(
        "src.price_fetcher._fetch_market_chart", return_value=fake_rows
    ) as mock_fetch, patch(
        "src.price_fetcher.upsert_prices_batch"
    ) as mock_upsert, patch(
        "src.price_fetcher._create_client"
    ):
        await sync_latest_prices(pool, config)

    mock_fetch.assert_called_once()
    # Should request gap + 1 days
    assert mock_fetch.call_args[0][1] == "6"
    mock_upsert.assert_awaited_once_with(pool, fake_rows)
