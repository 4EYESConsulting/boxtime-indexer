"""CSV file operations for price data and cointime output."""

import csv
import datetime
import logging
from datetime import date as date_type
from pathlib import Path
from typing import Dict, List, Optional

from src.fetcher import HeightData

logger = logging.getLogger(__name__)

CSV_FIELDNAMES = [
    "blockheight",
    "blockheight_timestamp",
    "blockheight_date",
    "coinblocks_created",
    "coinblocks_destroyed",
    "coinblocks_stored",
    "price_date",
    "price_close",
]

def _resolve_price_columns(fieldnames: list[str] | None) -> tuple[str, str]:
    if fieldnames is None:
        raise ValueError("Price CSV is missing a header row")

    columns = set(fieldnames)
    if {"snapped_at", "price"}.issubset(columns):
        return ("snapped_at", "price")
    if {"Date", "Close"}.issubset(columns):
        return ("Date", "Close")

    raise ValueError(
        "Unsupported price CSV columns. Expected snapped_at/price "
        f"or Date/Close, got: {', '.join(fieldnames)}"
    )


def load_prices(csv_path: str) -> tuple[Dict[date_type, float], Optional[date_type]]:
    """Load price data from CSV file.
    Expected CSV format:
        snapped_at,price,market_cap,total_volume
        2019-07-02 00:00:00 UTC,3.72,3011057.31,234258.35
        2019-07-01,0.50
        ...

    Returns:
        Tuple of (price_map, max_date) where price_map is {date: price_close}
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Price CSV not found: {csv_path}")

    price_map: Dict[date_type, float] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        date_col, close_col = _resolve_price_columns(reader.fieldnames)
        for row in reader:
            date_str = (row.get(date_col) or "").strip()
            close_str = (row.get(close_col) or "").strip()
            try:
                date_part = date_str.split(" ", 1)[0]
                dt = datetime.datetime.strptime(date_part, "%Y-%m-%d").date()
                price = float(close_str)
                price_map[dt] = price
            except ValueError as e:
                logger.warning("Skipping invalid row: %s - %s", row, e)

    max_date = max(price_map.keys()) if price_map else None
    logger.info("Loaded %d price records, max date: %s", len(price_map), max_date)
    return price_map, max_date


def load_bootstrap(csv_path: str) -> List[HeightData]:
    """Load existing cointime data from CSV for bootstrapping.

    Expected CSV format:
        blockheight,blockheight_timestamp,blockheight_date,coinblocks_created,coinblocks_destroyed,coinblocks_stored,price_date,price_close

    Returns:
        List of HeightData objects
    """
    path = Path(csv_path)
    if not path.exists():
        logger.info("Bootstrap CSV not found: %s", csv_path)
        return []

    rows: List[HeightData] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                height = int(row["blockheight"])
                timestamp = int(row["blockheight_timestamp"])
                block_date = datetime.datetime.strptime(
                    row["blockheight_date"], "%Y-%m-%d"
                ).date()
                cbc = int(row["coinblocks_created"])
                cbd = int(row["coinblocks_destroyed"])
                cbs = int(row["coinblocks_stored"])

                data = HeightData(
                    height=height,
                    timestamp=timestamp,
                    cbc=cbc,
                    cbd=cbd,
                    cbs=cbs,
                )
                data.block_date = block_date
                rows.append(data)
            except (ValueError, KeyError) as e:
                logger.warning("Skipping invalid bootstrap row: %s - %s", row, e)

    # Deduplicate to handle partial chunks from crashes
    rows = deduplicate_by_height(rows)
    
    max_height = rows[-1].height if rows else None
    logger.info(
        "Loaded %d bootstrap records, max height: %s", len(rows), max_height
    )
    return rows


def get_max_height(data: List[HeightData]) -> Optional[int]:
    """Get the maximum height from a list of HeightData."""
    if not data:
        return None
    return max(d.height for d in data)


def merge_with_prices(
    data: List[HeightData], price_map: Dict[date_type, float]
) -> List[HeightData]:
    """Merge HeightData with price information.

    Each HeightData gets annotated with price_date and price_close based on
    its block_date.
    """
    for d in data:
        if hasattr(d, "block_date"):
            price = price_map.get(d.block_date)
            if price is not None:
                d.price_date = d.block_date
                d.price_close = price
            else:
                d.price_date = None
                d.price_close = None
    return data


def _height_data_to_row(d: HeightData) -> dict:
    """Convert HeightData to CSV row dict."""
    return {
        "blockheight": d.height,
        "blockheight_timestamp": d.timestamp,
        "blockheight_date": (
            d.block_date.isoformat() if hasattr(d, "block_date") and d.block_date else ""
        ),
        "coinblocks_created": d.cbc,
        "coinblocks_destroyed": d.cbd,
        "coinblocks_stored": d.cbs,
        "price_date": (
            d.price_date.isoformat()
            if hasattr(d, "price_date") and d.price_date else ""
        ),
        "price_close": (
            d.price_close if hasattr(d, "price_close") and d.price_close is not None else ""
        ),
    }


def write_output(csv_path: str, data: List[HeightData]) -> None:
    """Write complete output CSV.

    Creates the output directory if it doesn't exist and writes all data
    as a complete rewrite.
    """
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()

        for d in data:
            writer.writerow(_height_data_to_row(d))

    logger.info("Wrote %d rows to %s", len(data), csv_path)


def append_output(csv_path: str, data: List[HeightData]) -> None:
    """Append data to output CSV.

    Writes header if file doesn't exist or is empty.
    Data should already have prices merged.
    """
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    file_exists = path.exists()
    file_empty = file_exists and path.stat().st_size == 0

    mode = "a" if file_exists else "w"
    write_header = not file_exists or file_empty

    with open(path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        if write_header:
            writer.writeheader()

        for d in data:
            writer.writerow(_height_data_to_row(d))

    logger.info("Appended %d rows to %s", len(data), csv_path)


def deduplicate_by_height(data: List[HeightData]) -> List[HeightData]:
    """Remove duplicate heights, keeping the last occurrence.

    This handles partial chunks from crashes where some heights
    were written before the crash.
    """
    if not data:
        return []

    seen: dict[int, HeightData] = {}
    for d in data:
        seen[d.height] = d  # Overwrite duplicates, keeping last

    if len(seen) < len(data):
        removed = len(data) - len(seen)
        logger.info("Removed %d duplicate heights during deduplication", removed)

    return list(seen.values())


def get_last_height(csv_path: str) -> Optional[int]:
    """Get the maximum height from existing CSV file.

    Reads all rows to find the maximum height value.
    """
    path = Path(csv_path)
    if not path.exists():
        return None

    max_height: Optional[int] = None
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    height = int(row["blockheight"])
                    if max_height is None or height > max_height:
                        max_height = height
                except (ValueError, KeyError):
                    continue
    except Exception as e:
        logger.warning("Error reading CSV for last height: %s", e)
        return None

    return max_height
