"""Tests for src.indexer."""

import asyncio
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest
from aioresponses import aioresponses

from src.config import Config
from src.indexer import _detect_reorg, _get_block_header, _get_chain_height, backfill

NODE = "http://test-node:9053"


def _make_config(**overrides) -> Config:
    defaults = dict(
        node_url=NODE,
        database_url="postgresql://x",
        chunk_size=100,
        max_concurrent=5,
        poll_interval=10,
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
# _get_block_header
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_block_header_success():
    with aioresponses() as m:
        m.get(f"{NODE}/blocks/at/10", payload=["hdr_abc"])
        m.get(
            f"{NODE}/blocks/hdr_abc/header",
            payload={"id": "hdr_abc", "parentId": "hdr_parent", "timestamp": 999},
        )
        async with aiohttp.ClientSession() as session:
            header = await _get_block_header(session, NODE, 10)

    assert header["id"] == "hdr_abc"
    assert header["parentId"] == "hdr_parent"


@pytest.mark.asyncio
async def test_get_block_header_not_found():
    with aioresponses() as m:
        m.get(f"{NODE}/blocks/at/99999", status=404)
        async with aiohttp.ClientSession() as session:
            header = await _get_block_header(session, NODE, 99999)
    assert header is None


# ---------------------------------------------------------------------------
# _detect_reorg
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_detect_reorg_no_reorg():
    with aioresponses() as m:
        m.get(f"{NODE}/blocks/at/10", payload=["hdr10"])
        m.get(
            f"{NODE}/blocks/hdr10/header",
            payload={"id": "hdr10", "parentId": "hdr9"},
        )
        m.get(f"{NODE}/blocks/at/9", payload=["hdr9"])
        m.get(
            f"{NODE}/blocks/hdr9/header",
            payload={"id": "hdr9", "parentId": "hdr8"},
        )
        async with aiohttp.ClientSession() as session:
            assert await _detect_reorg(session, NODE, 10) is False


@pytest.mark.asyncio
async def test_detect_reorg_detected():
    with aioresponses() as m:
        m.get(f"{NODE}/blocks/at/10", payload=["hdr10"])
        m.get(
            f"{NODE}/blocks/hdr10/header",
            payload={"id": "hdr10", "parentId": "hdr9_fork"},
        )
        m.get(f"{NODE}/blocks/at/9", payload=["hdr9"])
        m.get(
            f"{NODE}/blocks/hdr9/header",
            payload={"id": "hdr9", "parentId": "hdr8"},
        )
        async with aiohttp.ClientSession() as session:
            assert await _detect_reorg(session, NODE, 10) is True


@pytest.mark.asyncio
async def test_detect_reorg_at_height_1():
    """No reorg possible at height 1."""
    async with aiohttp.ClientSession() as session:
        assert await _detect_reorg(session, NODE, 1) is False


@pytest.mark.asyncio
async def test_detect_reorg_header_unavailable():
    """Returns False when headers can't be fetched."""
    with aioresponses() as m:
        m.get(f"{NODE}/blocks/at/10", status=500)
        m.get(f"{NODE}/blocks/at/9", status=500)
        async with aiohttp.ClientSession() as session:
            assert await _detect_reorg(session, NODE, 10) is False


# ---------------------------------------------------------------------------
# backfill
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_already_up_to_date():
    """backfill returns immediately when DB is ahead of chain."""
    config = _make_config()
    pool = AsyncMock()
    shutdown = asyncio.Event()

    with (
        patch("src.indexer.get_max_height", return_value=500),
        patch("src.indexer._get_chain_height", return_value=500),
        patch("src.indexer.fetch_chunk") as mock_fetch,
    ):
        async with aiohttp.ClientSession() as session:
            await backfill(session, pool, config, shutdown)

    mock_fetch.assert_not_called()


@pytest.mark.asyncio
async def test_backfill_processes_heights():
    """backfill fetches and upserts missing heights."""
    config = _make_config(chunk_size=5)
    pool = AsyncMock()
    shutdown = asyncio.Event()

    from src.fetcher import HeightData

    mock_results = [HeightData(h, 1000, 100, 10, 90) for h in range(6, 11)]

    with (
        patch("src.indexer.get_max_height", return_value=5),
        patch("src.indexer._get_chain_height", return_value=10),
        patch("src.indexer.fetch_chunk", return_value=mock_results) as mock_fetch,
        patch("src.indexer.upsert_batch") as mock_upsert,
    ):
        async with aiohttp.ClientSession() as session:
            await backfill(session, pool, config, shutdown)

    mock_fetch.assert_called_once()
    mock_upsert.assert_called_once()


@pytest.mark.asyncio
async def test_backfill_respects_shutdown():
    """backfill stops when shutdown_event is set."""
    config = _make_config(chunk_size=2)
    pool = AsyncMock()
    shutdown = asyncio.Event()
    shutdown.set()

    with (
        patch("src.indexer.get_max_height", return_value=0),
        patch("src.indexer._get_chain_height", return_value=100),
        patch("src.indexer.fetch_chunk") as mock_fetch,
    ):
        async with aiohttp.ClientSession() as session:
            await backfill(session, pool, config, shutdown)

    mock_fetch.assert_not_called()
