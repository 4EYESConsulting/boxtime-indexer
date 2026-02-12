"""Tests for src.main."""

import asyncio

import aiohttp
import pytest
from aioresponses import aioresponses

from src.main import _wait_for_node

NODE = "http://test-node:9053"


# ---------------------------------------------------------------------------
# _wait_for_node
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wait_for_node_ready_immediately():
    """Returns when the node has fullHeight and indexedHeight."""
    with aioresponses() as m:
        m.get(f"{NODE}/info", payload={"fullHeight": 1720000})
        m.get(
            f"{NODE}/blockchain/indexedHeight",
            payload={"indexedHeight": 1700000, "fullHeight": 1720000},
        )
        async with aiohttp.ClientSession() as session:
            # Should return without looping
            await _wait_for_node(session, NODE)


@pytest.mark.asyncio
async def test_wait_for_node_retries_until_ready():
    """Retries when the node is not ready, then succeeds."""
    with aioresponses() as m:
        # First call: node not reachable
        m.get(f"{NODE}/info", status=500)
        # Second call: node reachable but no indexedHeight yet
        m.get(f"{NODE}/info", payload={"fullHeight": 1720000})
        m.get(f"{NODE}/blockchain/indexedHeight", payload={})
        # Third call: ready
        m.get(f"{NODE}/info", payload={"fullHeight": 1720000})
        m.get(
            f"{NODE}/blockchain/indexedHeight",
            payload={"indexedHeight": 1700000, "fullHeight": 1720000},
        )

        async with aiohttp.ClientSession() as session:
            # Patch sleep to avoid waiting
            original_sleep = asyncio.sleep
            call_count = 0

            async def fast_sleep(seconds):
                nonlocal call_count
                call_count += 1
                await original_sleep(0)

            asyncio.sleep = fast_sleep
            try:
                await _wait_for_node(session, NODE)
            finally:
                asyncio.sleep = original_sleep

    assert call_count == 2  # slept twice before success


@pytest.mark.asyncio
async def test_wait_for_node_no_full_height():
    """Keeps waiting if fullHeight is missing from /info."""
    with aioresponses() as m:
        # First: no fullHeight
        m.get(f"{NODE}/info", payload={"name": "ergo-node"})
        # Second: has fullHeight + indexedHeight
        m.get(f"{NODE}/info", payload={"fullHeight": 100})
        m.get(
            f"{NODE}/blockchain/indexedHeight",
            payload={"indexedHeight": 100, "fullHeight": 100},
        )

        async with aiohttp.ClientSession() as session:
            original_sleep = asyncio.sleep

            async def fast_sleep(seconds):
                await original_sleep(0)

            asyncio.sleep = fast_sleep
            try:
                await _wait_for_node(session, NODE)
            finally:
                asyncio.sleep = original_sleep
