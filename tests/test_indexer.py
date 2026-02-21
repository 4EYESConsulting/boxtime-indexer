"""Tests for src.indexer."""

import asyncio
from datetime import date
from unittest.mock import patch

import aiohttp
import pytest
from aioresponses import aioresponses

from src.config import Config
from src.indexer import _get_chain_height, run_backfill

NODE = "http://test-node:9053"


def _make_config(**overrides) -> Config:
    defaults = dict(
        node_url=NODE,
        price_csv_path="input/erg_prices.csv",
        cointime_output_path="output/cointime.csv",
        prices_output_path="output/prices.csv",
        chunk_size=100,
        max_concurrent=5,
        start_height=1,
    )
    defaults.update(overrides)
    return Config(**defaults)


# ---------------------------------------------------------------------------
# _get_chain_height
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_chain_height_success():
    with aioresponses() as m:
        m.get(
            f"{NODE}/blockchain/indexedHeight",
            payload={"indexedHeight": 1700000, "fullHeight": 1720000},
        )
        async with aiohttp.ClientSession() as session:
            h = await _get_chain_height(session, NODE)
    assert h == 1700000


@pytest.mark.asyncio
async def test_get_chain_height_failure():
    with aioresponses() as m:
        m.get(f"{NODE}/blockchain/indexedHeight", status=500)
        async with aiohttp.ClientSession() as session:
            with pytest.raises(RuntimeError, match="Failed to fetch indexed height"):
                await _get_chain_height(session, NODE)


@pytest.mark.asyncio
async def test_get_chain_height_missing_field():
    with aioresponses() as m:
        m.get(f"{NODE}/blockchain/indexedHeight", payload={"fullHeight": 100})
        async with aiohttp.ClientSession() as session:
            with pytest.raises(RuntimeError, match="Failed to fetch indexed height"):
                await _get_chain_height(session, NODE)


# ---------------------------------------------------------------------------
# run_backfill
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_backfill_no_existing_data():
    """Backfill from start_height when no existing data."""
    config = _make_config(start_height=1)
    shutdown = asyncio.Event()
    price_map = {date(2025, 1, 1): 1.0}

    with aioresponses() as m:
        m.get(f"{NODE}/blockchain/indexedHeight", payload={"indexedHeight": 10})
        with patch("src.indexer.get_max_height", return_value=None), \
             patch("src.indexer.find_first_height_by_date", return_value=10), \
             patch("src.indexer._fetch_and_write_chunks") as mock_fetch, \
             patch("src.indexer.write_prices_csv"):
            async with aiohttp.ClientSession() as session:
                await run_backfill(
                    session=session,
                    config=config,
                    max_price_date=date(2025, 1, 1),
                    shutdown_event=shutdown,
                    price_map=price_map,
                )

    mock_fetch.assert_called_once()
    call_kwargs = mock_fetch.call_args[1]
    assert call_kwargs["start_height"] == 1


@pytest.mark.asyncio
async def test_run_backfill_with_existing_data():
    """Backfill resumes from max existing height."""
    config = _make_config(start_height=1)
    shutdown = asyncio.Event()
    price_map = {date(2025, 1, 1): 1.0}

    with aioresponses() as m:
        m.get(f"{NODE}/blockchain/indexedHeight", payload={"indexedHeight": 10})
        with patch("src.indexer.get_max_height", return_value=5), \
             patch("src.indexer.find_first_height_by_date", return_value=10), \
             patch("src.indexer._fetch_and_write_chunks") as mock_fetch, \
             patch("src.indexer.write_prices_csv"):
            async with aiohttp.ClientSession() as session:
                await run_backfill(
                    session=session,
                    config=config,
                    max_price_date=date(2025, 1, 1),
                    shutdown_event=shutdown,
                    price_map=price_map,
                )

    mock_fetch.assert_called_once()
    call_kwargs = mock_fetch.call_args[1]
    assert call_kwargs["start_height"] == 6


@pytest.mark.asyncio
async def test_run_backfill_already_up_to_date():
    """Skip backfill when already at target height."""
    config = _make_config(start_height=1)
    shutdown = asyncio.Event()
    price_map = {date(2025, 1, 1): 1.0}

    with aioresponses() as m:
        m.get(f"{NODE}/blockchain/indexedHeight", payload={"indexedHeight": 1})
        with patch("src.indexer.get_max_height", return_value=1), \
             patch("src.indexer.find_first_height_by_date", return_value=1), \
             patch("src.indexer._fetch_and_write_chunks") as mock_fetch, \
             patch("src.indexer.write_prices_csv") as mock_prices:
            async with aiohttp.ClientSession() as session:
                await run_backfill(
                    session=session,
                    config=config,
                    max_price_date=date(2025, 1, 1),
                    shutdown_event=shutdown,
                    price_map=price_map,
                )

    mock_fetch.assert_not_called()
    mock_prices.assert_called_once()
