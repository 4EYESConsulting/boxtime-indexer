"""Tests for src.indexer."""

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from aioresponses import aioresponses

from src.config import Config
from src.fetcher import HeightData
from src.indexer import _get_chain_height, _has_genesis_row, _make_genesis_row, _write_genesis_if_needed, run_backfill

NODE = "http://test-node:9053"


def _make_config(**overrides) -> Config:
    defaults = dict(
        node_url=NODE,
        price_csv_path="input/erg_prices.csv",
        bootstrap_csv_path="input/cointime.csv",
        csv_output_path="output/cointime.csv",
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
# Genesis helpers
# ---------------------------------------------------------------------------


def test_make_genesis_row():
    """Genesis row has correct values."""
    row = _make_genesis_row()
    assert row.height == 0
    assert row.cbc == 0
    assert row.cbd == 0
    assert row.cbs == 0


def test_has_genesis_row_true():
    """Detects genesis row in data."""
    data = [
        HeightData(height=0, timestamp=1000, cbc=0, cbd=0, cbs=0),
        HeightData(height=1, timestamp=2000, cbc=100, cbd=10, cbs=90),
    ]
    assert _has_genesis_row(data) is True


def test_has_genesis_row_false():
    """Returns False when no genesis row."""
    data = [
        HeightData(height=1, timestamp=2000, cbc=100, cbd=10, cbs=90),
    ]
    assert _has_genesis_row(data) is False


def test_has_genesis_row_empty():
    """Returns False for empty data."""
    assert _has_genesis_row([]) is False


# ---------------------------------------------------------------------------
# run_backfill
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_backfill_no_bootstrap():
    """Backfill does not synthesize genesis row when start_height > 0."""
    config = _make_config(start_height=1)
    shutdown = asyncio.Event()
    price_map = {date(2025, 1, 1): 1.0}

    mock_results = [
        HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
    ]
    mock_results[0].block_date = date(2025, 1, 1)

    with aioresponses() as m:
        m.get(f"{NODE}/blockchain/indexedHeight", payload={"indexedHeight": 10})
        with patch("src.indexer.get_max_height", return_value=None), \
             patch("src.indexer._fetch_and_write_chunks", return_value=mock_results) as mock_fetch, \
             patch("src.indexer._has_genesis_row", return_value=False), \
             patch("src.indexer._write_genesis_if_needed"):
            async with aiohttp.ClientSession() as session:
                results = await run_backfill(
                    session=session,
                    config=config,
                    bootstrap_data=[],
                    max_price_date=date(2025, 1, 1),
                    shutdown_event=shutdown,
                    price_map=price_map,
                )

    mock_fetch.assert_called_once()
    assert len(results) == 1
    assert results[0].height == 1


@pytest.mark.asyncio
async def test_run_backfill_with_bootstrap():
    """Backfill resumes from max bootstrap height."""
    config = _make_config(start_height=1)
    shutdown = asyncio.Event()
    price_map = {date(2025, 1, 1): 1.0}

    bootstrap = [
        HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
    ]

    new_data = [
        HeightData(height=2, timestamp=1562065200000, cbc=100, cbd=10, cbs=90),
    ]
    new_data[0].block_date = date(2025, 1, 1)

    with aioresponses() as m:
        m.get(f"{NODE}/blockchain/indexedHeight", payload={"indexedHeight": 10})
        with patch("src.indexer.get_max_height", return_value=1), \
             patch("src.indexer._fetch_and_write_chunks", return_value=new_data) as mock_fetch, \
             patch("src.indexer._has_genesis_row", return_value=False):
            async with aiohttp.ClientSession() as session:
                results = await run_backfill(
                    session=session,
                    config=config,
                    bootstrap_data=bootstrap,
                    max_price_date=date(2025, 1, 1),
                    shutdown_event=shutdown,
                    price_map=price_map,
                )

    assert mock_fetch.call_args[1]["start_height"] == 2
    assert len(results) == 2
    assert results[0].height == 1
    assert results[1].height == 2


@pytest.mark.asyncio
async def test_run_backfill_already_up_to_date():
    """Returns bootstrap data when already at chain height."""
    config = _make_config(start_height=1)
    shutdown = asyncio.Event()
    price_map = {date(2025, 1, 1): 1.0}

    bootstrap = [
        HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
    ]

    with aioresponses() as m:
        m.get(f"{NODE}/blockchain/indexedHeight", payload={"indexedHeight": 1})
        with patch("src.indexer.get_max_height", return_value=1), \
             patch("src.indexer._fetch_and_write_chunks") as mock_fetch, \
             patch("src.indexer._has_genesis_row", return_value=False):
            async with aiohttp.ClientSession() as session:
                results = await run_backfill(
                    session=session,
                    config=config,
                    bootstrap_data=bootstrap,
                    max_price_date=date(2025, 1, 1),
                    shutdown_event=shutdown,
                    price_map=price_map,
                )

    mock_fetch.assert_not_called()
    assert len(results) == 1
    assert results[0].height == 1


@pytest.mark.asyncio
async def test_run_backfill_with_genesis():
    """Backfill with start_height=0 includes genesis row."""
    config = _make_config(start_height=0)
    shutdown = asyncio.Event()
    price_map = {date(2019, 7, 1): 1.0}

    new_data = [
        HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
    ]
    new_data[0].block_date = date(2019, 7, 1)

    with aioresponses() as m:
        m.get(f"{NODE}/blockchain/indexedHeight", payload={"indexedHeight": 10})
        with patch("src.indexer.get_max_height", return_value=None), \
             patch("src.indexer._fetch_and_write_chunks", return_value=new_data), \
             patch("src.indexer._has_genesis_row", return_value=False), \
             patch("src.indexer._write_genesis_if_needed") as mock_genesis:
            async with aiohttp.ClientSession() as session:
                results = await run_backfill(
                    session=session,
                    config=config,
                    bootstrap_data=[],
                    max_price_date=date(2025, 1, 1),
                    shutdown_event=shutdown,
                    price_map=price_map,
                )

    mock_genesis.assert_called_once()
    assert len(results) == 2  # genesis + new data
    assert results[0].height == 0
    assert results[1].height == 1


@pytest.mark.asyncio
async def test_run_backfill_does_not_duplicate_genesis():
    """Backfill does not duplicate existing genesis row."""
    config = _make_config(start_height=0)
    shutdown = asyncio.Event()
    price_map = {date(2019, 7, 1): 1.0}

    bootstrap = [
        HeightData(height=0, timestamp=1561978800000, cbc=0, cbd=0, cbs=0),
    ]

    new_data = [
        HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
    ]
    new_data[0].block_date = date(2019, 7, 1)

    with aioresponses() as m:
        m.get(f"{NODE}/blockchain/indexedHeight", payload={"indexedHeight": 10})
        with patch("src.indexer.get_max_height", return_value=0), \
             patch("src.indexer._fetch_and_write_chunks", return_value=new_data), \
             patch("src.indexer._has_genesis_row", return_value=True), \
             patch("src.indexer._write_genesis_if_needed") as mock_genesis:
            async with aiohttp.ClientSession() as session:
                results = await run_backfill(
                    session=session,
                    config=config,
                    bootstrap_data=bootstrap,
                    max_price_date=date(2025, 1, 1),
                    shutdown_event=shutdown,
                    price_map=price_map,
                )

    mock_genesis.assert_not_called()
    assert [r.height for r in results] == [0, 1]
    assert len([r for r in results if r.height == 0]) == 1
