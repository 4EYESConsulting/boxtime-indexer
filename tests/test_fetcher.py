"""Tests for src.fetcher."""

import asyncio

import aiohttp
import pytest
from aioresponses import aioresponses

from src.fetcher import (
    HeightData,
    _EMISSION_ERGO_TREE,
    _get_json,
    fetch_chunk,
    fetch_height,
    fetch_height_with_retry,
)

NODE = "http://test-node:9053"


# ---------------------------------------------------------------------------
# HeightData
# ---------------------------------------------------------------------------


def test_height_data_as_row():
    hd = HeightData(height=10, timestamp=1000, cbc=500, cbd=100, cbs=400)
    assert hd.as_row() == (10, 1000, 500, 100, 400)


# ---------------------------------------------------------------------------
# _get_json
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_json_success():
    with aioresponses() as m:
        m.get(f"{NODE}/info", payload={"fullHeight": 100})
        async with aiohttp.ClientSession() as session:
            result = await _get_json(session, f"{NODE}/info")
    assert result == {"fullHeight": 100}


@pytest.mark.asyncio
async def test_get_json_error_status():
    with aioresponses() as m:
        m.get(f"{NODE}/bad", status=500)
        async with aiohttp.ClientSession() as session:
            result = await _get_json(session, f"{NODE}/bad")
    assert result is None


# ---------------------------------------------------------------------------
# fetch_height
# ---------------------------------------------------------------------------

# Reusable mock block with blockTransactions schema
_BLOCK_BT_SCHEMA = {
    "header": {"timestamp": 1561978800000},
    "blockTransactions": {
        "transactions": [
            {
                "inputs": [
                    {"value": 1000, "inclusionHeight": 5},
                    {"value": 2000, "inclusionHeight": 8},
                ]
            }
        ]
    },
}

# Mock block with flat transactions schema (v6.0.2) and an emission box input
_BLOCK_FLAT_SCHEMA = {
    "header": {"timestamp": 1561978800000},
    "transactions": [
        {
            "inputs": [
                # Emission contract box — should be excluded from CBD
                {
                    "value": 93409132500000000,
                    "inclusionHeight": 9,
                    "ergoTree": _EMISSION_ERGO_TREE,
                },
                {"value": 1000, "inclusionHeight": 5},
            ]
        },
        {
            "inputs": [
                {"value": 2000, "inclusionHeight": 8},
            ]
        },
    ],
}


def _mock_fetch_height_endpoints(m, height=10, block=None):
    """Register the 3 endpoints used by fetch_height."""
    m.get(
        f"{NODE}/emission/at/{height}",
        payload={"totalCoinsIssued": 75000000000},
    )
    m.get(f"{NODE}/blocks/at/{height}", payload=["abc123"])
    m.get(
        f"{NODE}/blockchain/block/byHeaderId/abc123",
        payload=block or _BLOCK_BT_SCHEMA,
    )


@pytest.mark.asyncio
async def test_fetch_height_blockTransactions_schema():
    """fetch_height works with the blockTransactions response schema."""
    with aioresponses() as m:
        _mock_fetch_height_endpoints(m, height=10)
        async with aiohttp.ClientSession() as session:
            hd = await fetch_height(session, NODE, 10)

    assert hd.height == 10
    assert hd.cbc == 75000000000
    assert hd.timestamp == 1561978800000
    # CBD = 1000*(10-5) + 2000*(10-8) = 5000 + 4000 = 9000
    assert hd.cbd == 9000
    assert hd.cbs == 75000000000 - 9000


@pytest.mark.asyncio
async def test_fetch_height_flat_transactions_schema():
    """fetch_height works with the flat transactions schema (v6.0.2) and
    excludes the emission contract box from CBD."""
    with aioresponses() as m:
        _mock_fetch_height_endpoints(m, height=10, block=_BLOCK_FLAT_SCHEMA)
        async with aiohttp.ClientSession() as session:
            hd = await fetch_height(session, NODE, 10)

    assert hd.height == 10
    assert hd.cbc == 75000000000
    assert hd.timestamp == 1561978800000
    # Emission box (value=93409132500000000, inclusionHeight=9) is excluded.
    # CBD = 1000*(10-5) + 2000*(10-8) = 5000 + 4000 = 9000
    assert hd.cbd == 9000
    assert hd.cbs == 75000000000 - 9000


@pytest.mark.asyncio
async def test_fetch_height_emission_box_excluded():
    """Emission contract box input is excluded from CBD even when it is the
    only input in a transaction."""
    block = {
        "header": {"timestamp": 2000},
        "blockTransactions": {
            "transactions": [
                {
                    "inputs": [
                        {
                            "value": 50000000000000000,
                            "inclusionHeight": 1,
                            "ergoTree": _EMISSION_ERGO_TREE,
                        },
                    ]
                }
            ]
        },
    }
    with aioresponses() as m:
        _mock_fetch_height_endpoints(m, height=5, block=block)
        async with aiohttp.ClientSession() as session:
            hd = await fetch_height(session, NODE, 5)

    # The only input is the emission box — CBD should be 0
    assert hd.cbd == 0
    assert hd.cbs == hd.cbc


@pytest.mark.asyncio
async def test_fetch_height_no_inclusion_height():
    """Inputs without inclusionHeight are skipped in CBD computation."""
    block = {
        "header": {"timestamp": 1000},
        "blockTransactions": {
            "transactions": [
                {"inputs": [{"value": 500}]},  # no inclusionHeight
            ]
        },
    }
    with aioresponses() as m:
        _mock_fetch_height_endpoints(m, height=5, block=block)
        async with aiohttp.ClientSession() as session:
            hd = await fetch_height(session, NODE, 5)

    assert hd.cbd == 0
    assert hd.cbs == hd.cbc


@pytest.mark.asyncio
async def test_fetch_height_emission_failure():
    """fetch_height raises when emission endpoint fails."""
    with aioresponses() as m:
        m.get(f"{NODE}/emission/at/1", status=500)
        async with aiohttp.ClientSession() as session:
            with pytest.raises(RuntimeError, match="Failed to fetch emission"):
                await fetch_height(session, NODE, 1)


@pytest.mark.asyncio
async def test_fetch_height_header_ids_failure():
    """fetch_height raises when blocks/at endpoint fails."""
    with aioresponses() as m:
        m.get(f"{NODE}/emission/at/1", payload={"totalCoinsIssued": 100})
        m.get(f"{NODE}/blocks/at/1", status=404)
        async with aiohttp.ClientSession() as session:
            with pytest.raises(RuntimeError, match="Failed to fetch header ID"):
                await fetch_height(session, NODE, 1)


@pytest.mark.asyncio
async def test_fetch_height_block_failure():
    """fetch_height raises when indexed block endpoint fails."""
    with aioresponses() as m:
        m.get(f"{NODE}/emission/at/1", payload={"totalCoinsIssued": 100})
        m.get(f"{NODE}/blocks/at/1", payload=["hdr1"])
        m.get(f"{NODE}/blockchain/block/byHeaderId/hdr1", status=400)
        async with aiohttp.ClientSession() as session:
            with pytest.raises(RuntimeError, match="Failed to fetch indexed block"):
                await fetch_height(session, NODE, 1)


# ---------------------------------------------------------------------------
# fetch_height_with_retry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_height_with_retry_success():
    """Returns HeightData on first success."""
    sem = asyncio.Semaphore(10)
    with aioresponses() as m:
        _mock_fetch_height_endpoints(m, height=10)
        async with aiohttp.ClientSession() as session:
            result = await fetch_height_with_retry(session, NODE, 10, sem)

    assert result is not None
    assert result.height == 10


@pytest.mark.asyncio
async def test_fetch_height_with_retry_exhausted():
    """Returns None after all retries fail."""
    sem = asyncio.Semaphore(10)
    with aioresponses() as m:
        # All 3 attempts fail on emission
        for _ in range(3):
            m.get(f"{NODE}/emission/at/1", status=500)
        async with aiohttp.ClientSession() as session:
            result = await fetch_height_with_retry(session, NODE, 1, sem)

    assert result is None


# ---------------------------------------------------------------------------
# fetch_chunk
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_chunk_partial_success():
    """fetch_chunk returns only successful results."""
    with aioresponses() as m:
        # Height 10 succeeds
        _mock_fetch_height_endpoints(m, height=10)
        # Height 11 fails all 3 retries
        for _ in range(3):
            m.get(f"{NODE}/emission/at/11", status=500)

        async with aiohttp.ClientSession() as session:
            results = await fetch_chunk(session, NODE, [10, 11], max_concurrent=5)

    assert len(results) == 1
    assert results[0].height == 10
