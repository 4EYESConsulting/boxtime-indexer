"""Async Ergo node fetching for CBC/CBD/CBS per height."""

import asyncio
import datetime
import logging
from dataclasses import dataclass
from typing import List, Optional

import aiohttp

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE = 1.0  # seconds

# Ergo mainnet emission contract ergoTree. This box holds the unissued supply
# and is consumed/recreated in every block's coinbase transaction. It must be
# excluded from CBD computation because it is not part of the circulating supply.
# When emissions eventually run out the box will no longer appear as an input
# and this filter will simply match nothing.
_EMISSION_ERGO_TREE = (
    "101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2"
    "815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f603"
    "0580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a3"
    "8cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019"
    "683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b"
    "730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f"
)


@dataclass
class HeightData:
    """Computed cointime data for a single block height."""

    height: int
    timestamp: int
    cbc: int
    cbd: int
    cbs: int
    _block_date: datetime.date = None

    def as_row(self) -> tuple:
        return (self.height, self.timestamp, self.cbc, self.cbd, self.cbs)

    @property
    def block_date(self) -> datetime.date:
        """Compute block date from timestamp (UTC), or return stored value if set."""
        if self._block_date is not None:
            return self._block_date
        return datetime.datetime.fromtimestamp(
            self.timestamp / 1000, tz=datetime.timezone.utc
        ).date()

    @block_date.setter
    def block_date(self, value: datetime.date) -> None:
        """Allow setting block_date for CSV loading."""
        self._block_date = value


async def _get_json(
    session: aiohttp.ClientSession, url: str
) -> Optional[dict | list]:
    """GET a URL and return parsed JSON, or None on error."""
    async with session.get(url) as resp:
        if resp.status != 200:
            logger.warning("GET %s returned %d", url, resp.status)
            return None
        return await resp.json()


async def fetch_height(
    session: aiohttp.ClientSession,
    node_url: str,
    height: int,
) -> HeightData:
    """Fetch CBC, CBD, CBS, and timestamp for a single height.

    Makes 3 HTTP calls:
    1. GET /emission/at/{h} -> CBC (totalCoinsIssued)
    2. GET /blocks/at/{h} -> header_id
    3. GET /blockchain/block/byHeaderId/{header_id} -> indexed block
       -> parse inputs for CBD + extract timestamp

    Raises on any failure (caller handles retries).
    """
    # 1. CBC from emission endpoint
    emission = await _get_json(session, f"{node_url}/emission/at/{height}")
    if emission is None:
        raise RuntimeError(f"Failed to fetch emission at height {height}")
    cbc: int = emission["totalCoinsIssued"]

    # 2. Header ID from blocks/at
    header_ids = await _get_json(session, f"{node_url}/blocks/at/{height}")
    if not header_ids:
        raise RuntimeError(f"Failed to fetch header ID at height {height}")
    header_id = header_ids[0] if isinstance(header_ids, list) else header_ids

    # 3. Indexed block with full transactions
    block = await _get_json(
        session, f"{node_url}/blockchain/block/byHeaderId/{header_id}"
    )
    if block is None:
        raise RuntimeError(
            f"Failed to fetch indexed block for {header_id} at height {height}"
        )

    # Extract timestamp from block header
    timestamp: int = block["header"]["timestamp"]

    # Compute CBD from indexed transaction inputs, excluding the emission
    # contract box which carries the unissued supply and is not circulating.
    cbd = 0
    txs = block.get("blockTransactions", {}).get("transactions") or block.get("transactions", [])
    for tx in txs:
        for inp in tx["inputs"]:
            if inp.get("ergoTree") == _EMISSION_ERGO_TREE:
                continue
            inclusion_height = inp.get("inclusionHeight")
            if inclusion_height is None:
                continue
            value: int = inp["value"]
            lifespan = height - inclusion_height
            cbd += value * lifespan

    cbs = cbc - cbd
    return HeightData(
        height=height, timestamp=timestamp, cbc=cbc, cbd=cbd, cbs=cbs
    )


async def fetch_height_with_retry(
    session: aiohttp.ClientSession,
    node_url: str,
    height: int,
    semaphore: asyncio.Semaphore,
) -> Optional[HeightData]:
    """Fetch a height with retries and exponential backoff.

    Returns None if all retries are exhausted.
    """
    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                return await fetch_height(session, node_url, height)
            except Exception:
                if attempt < MAX_RETRIES - 1:
                    delay = BACKOFF_BASE * (2**attempt)
                    logger.warning(
                        "Height %d attempt %d failed, retrying in %.1fs",
                        height,
                        attempt + 1,
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        "Height %d failed after %d attempts",
                        height,
                        MAX_RETRIES,
                    )
        return None


# fetch_chunk is the primary batch fetching primitive used throughout the indexer.
# It fetches a list of heights concurrently (bounded by max_concurrent), with
# automatic retries and exponential backoff for each height.
#
# Used by:
# - gap_fill: to fill missing heights detected on startup
# - backfill: to process chunks during initial sync from resume point to chain tip
# - poll_loop: to index new blocks discovered during continuous polling
async def fetch_chunk(
    session: aiohttp.ClientSession,
    node_url: str,
    heights: List[int],
    max_concurrent: int,
) -> List[HeightData]:
    """Fetch a list of heights concurrently, bounded by a semaphore.

    Returns only successfully fetched heights (failures are logged and skipped).
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = [
        fetch_height_with_retry(session, node_url, h, semaphore)
        for h in heights
    ]
    results = await asyncio.gather(*tasks)
    return [r for r in results if r is not None]
