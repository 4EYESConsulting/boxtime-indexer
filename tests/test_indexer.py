"""Tests for src.indexer."""

import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest
from aioresponses import aioresponses

from src.config import Config
from src.fetcher import HeightData
from src.indexer import _get_chain_height, _fetch_until_date, run_backfill

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
# _fetch_until_date
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_until_date_already_up_to_date():
    """Returns empty list when already at chain height."""
    with aioresponses() as m:
        m.get(
            f"{NODE}/blockchain/indexedHeight",
            payload={"indexedHeight": 100},
        )
        async with aiohttp.ClientSession() as session:
            results = await _fetch_until_date(
                session=session,
                node_url=NODE,
                start_height=100,
                max_price_date=date(2025, 1, 1),
                chunk_size=10,
                max_concurrent=5,
                shutdown_event=asyncio.Event(),
            )
    assert results == []


@pytest.mark.asyncio
async def test_fetch_until_date_stops_at_price_date():
    """Stops fetching when block_date exceeds max_price_date."""
    mock_results = [
        HeightData(height=10, timestamp=1561939200000, cbc=100, cbd=10, cbs=90),
        HeightData(height=11, timestamp=1562025600000, cbc=100, cbd=10, cbs=90),
        HeightData(height=12, timestamp=1563840000000, cbc=100, cbd=10, cbs=90),
    ]

    with aioresponses() as m:
        m.get(
            f"{NODE}/blockchain/indexedHeight",
            payload={"indexedHeight": 12},
        )
        with patch("src.indexer.fetch_chunk", return_value=mock_results):
            async with aiohttp.ClientSession() as session:
                results = await _fetch_until_date(
                    session=session,
                    node_url=NODE,
                    start_height=10,
                    max_price_date=date(2019, 7, 22),
                    chunk_size=10,
                    max_concurrent=5,
                    shutdown_event=asyncio.Event(),
                )

    assert len(results) == 2
    assert results[0].height == 10
    assert results[1].height == 11


@pytest.mark.asyncio
async def test_fetch_until_date_respects_shutdown():
    """Stops fetching when shutdown_event is set."""
    shutdown = asyncio.Event()
    shutdown.set()

    with aioresponses() as m:
        m.get(
            f"{NODE}/blockchain/indexedHeight",
            payload={"indexedHeight": 100},
        )
        with patch("src.indexer.fetch_chunk") as mock_fetch:
            async with aiohttp.ClientSession() as session:
                results = await _fetch_until_date(
                    session=session,
                    node_url=NODE,
                    start_height=1,
                    max_price_date=date(2030, 1, 1),
                    chunk_size=10,
                    max_concurrent=5,
                    shutdown_event=shutdown,
                )

    mock_fetch.assert_not_called()
    assert results == []


# ---------------------------------------------------------------------------
# run_backfill
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_backfill_no_bootstrap():
    """Backfill does not synthesize genesis row when start_height > 0."""
    config = _make_config(start_height=1)
    shutdown = asyncio.Event()

    mock_results = [
        HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
    ]

    with patch("src.indexer.get_max_height", return_value=None), patch(
        "src.indexer._fetch_until_date", return_value=mock_results
    ) as mock_fetch:
        async with aiohttp.ClientSession() as session:
            results = await run_backfill(
                session=session,
                config=config,
                bootstrap_data=[],
                max_price_date=date(2025, 1, 1),
                shutdown_event=shutdown,
            )

    mock_fetch.assert_called_once()
    assert len(results) == 1
    assert results[0].height == 1


@pytest.mark.asyncio
async def test_run_backfill_with_bootstrap():
    """Backfill resumes from max bootstrap height."""
    config = _make_config(start_height=1)
    shutdown = asyncio.Event()

    bootstrap = [
        HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
    ]

    new_data = [
        HeightData(height=2, timestamp=1562065200000, cbc=100, cbd=10, cbs=90),
    ]

    with patch("src.indexer.get_max_height", return_value=1), patch(
        "src.indexer._fetch_until_date", return_value=new_data
    ) as mock_fetch:
        async with aiohttp.ClientSession() as session:
            results = await run_backfill(
                session=session,
                config=config,
                bootstrap_data=bootstrap,
                max_price_date=date(2025, 1, 1),
                shutdown_event=shutdown,
            )

    assert mock_fetch.call_args[1]["start_height"] == 2
    assert len(results) == 2
    assert results[0].height == 1
    assert results[1].height == 2


@pytest.mark.asyncio
async def test_run_backfill_clamps_start_height_to_one():
    """Backfill clamps effective fetch start to 1 when configured start is 0 and synthesizes genesis row."""
    config = _make_config(start_height=0)
    shutdown = asyncio.Event()

    with patch("src.indexer.get_max_height", return_value=None), patch(
        "src.indexer._fetch_until_date", return_value=[]
    ) as mock_fetch:
        async with aiohttp.ClientSession() as session:
            results = await run_backfill(
                session=session,
                config=config,
                bootstrap_data=[],
                max_price_date=date(2025, 1, 1),
                shutdown_event=shutdown,
            )

    assert mock_fetch.call_args[1]["start_height"] == 1
    assert len(results) == 1
    assert results[0].height == 0
    assert results[0].cbc == 0
    assert results[0].cbd == 0
    assert results[0].cbs == 0


@pytest.mark.asyncio
async def test_run_backfill_does_not_duplicate_existing_genesis_row():
    """Backfill preserves existing height-0 bootstrap row without duplication."""
    config = _make_config(start_height=1)
    shutdown = asyncio.Event()

    bootstrap = [
        HeightData(height=0, timestamp=1561978800000, cbc=0, cbd=0, cbs=0),
        HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
    ]
    new_data = [
        HeightData(height=2, timestamp=1562065200000, cbc=100, cbd=10, cbs=90),
    ]

    with patch("src.indexer.get_max_height", return_value=1), patch(
        "src.indexer._fetch_until_date", return_value=new_data
    ):
        async with aiohttp.ClientSession() as session:
            results = await run_backfill(
                session=session,
                config=config,
                bootstrap_data=bootstrap,
                max_price_date=date(2025, 1, 1),
                shutdown_event=shutdown,
            )

    assert [r.height for r in results] == [0, 1, 2]
    assert len([r for r in results if r.height == 0]) == 1


@pytest.mark.asyncio
async def test_run_backfill_preserves_bootstrap_genesis_when_start_height_gt_zero():
    """Backfill keeps bootstrap genesis row even when configured start_height is > 0."""
    config = _make_config(start_height=100)
    shutdown = asyncio.Event()

    bootstrap = [
        HeightData(height=0, timestamp=1561978800000, cbc=0, cbd=0, cbs=0),
        HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
    ]

    with patch("src.indexer.get_max_height", return_value=1), patch(
        "src.indexer._fetch_until_date", return_value=[]
    ) as mock_fetch:
        async with aiohttp.ClientSession() as session:
            results = await run_backfill(
                session=session,
                config=config,
                bootstrap_data=bootstrap,
                max_price_date=date(2025, 1, 1),
                shutdown_event=shutdown,
            )

    assert mock_fetch.call_args[1]["start_height"] == 2
    assert [r.height for r in results] == [0, 1]


@pytest.mark.asyncio
async def test_run_backfill_start_height_zero_with_existing_bootstrap_genesis():
    """Backfill with start_height=0 and bootstrap genesis keeps a single height-0 row."""
    config = _make_config(start_height=0)
    shutdown = asyncio.Event()

    bootstrap = [
        HeightData(height=0, timestamp=1561978800000, cbc=0, cbd=0, cbs=0),
        HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
    ]

    with patch("src.indexer.get_max_height", return_value=1), patch(
        "src.indexer._fetch_until_date", return_value=[]
    ) as mock_fetch:
        async with aiohttp.ClientSession() as session:
            results = await run_backfill(
                session=session,
                config=config,
                bootstrap_data=bootstrap,
                max_price_date=date(2025, 1, 1),
                shutdown_event=shutdown,
            )

    assert mock_fetch.call_args[1]["start_height"] == 2
    assert [r.height for r in results] == [0, 1]
    assert len([r for r in results if r.height == 0]) == 1
