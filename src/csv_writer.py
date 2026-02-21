"""CSV file operations for price data and output."""

import csv
import datetime
import logging
from datetime import date as date_type
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.fetcher import HeightData

logger = logging.getLogger(__name__)

COINTIME_FIELDNAMES = [
    "blockheight",
    "blockheight_timestamp",
    "coinblocks_created",
    "coinblocks_destroyed",
    "coinblocks_stored",
]

PRICES_FIELDNAMES = [
    "price_date",
    "price_timestamp",
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


def load_prices(csv_path: str) -> Tuple[Dict[date_type, float], Optional[date_type]]:
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


def write_cointime_csv(csv_path: str, data: List[HeightData]) -> None:
    """Write cointime data to CSV.
    
    Creates the output directory if it doesn't exist.
    """
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    file_exists = path.exists()
    file_empty = file_exists and path.stat().st_size == 0

    mode = "a" if file_exists else "w"
    write_header = not file_exists or file_empty

    with open(path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COINTIME_FIELDNAMES)
        if write_header:
            writer.writeheader()

        for d in data:
            writer.writerow({
                "blockheight": d.height,
                "blockheight_timestamp": d.timestamp,
                "coinblocks_created": d.cbc,
                "coinblocks_destroyed": d.cbd,
                "coinblocks_stored": d.cbs,
            })

    logger.info("Appended %d rows to %s", len(data), csv_path)


def _date_to_utc_timestamp(date: date_type) -> int:
    """Convert date to UTC midnight timestamp in milliseconds."""
    dt = datetime.datetime.combine(date, datetime.time(0, 0), tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def write_prices_csv(csv_path: str, price_map: Dict[date_type, float]) -> None:
    """Write deduplicated price data to CSV.
    
    Creates the output directory if it doesn't exist.
    """
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PRICES_FIELDNAMES)
        writer.writeheader()

        for date in sorted(price_map.keys()):
            timestamp = _date_to_utc_timestamp(date)
            writer.writerow({
                "price_date": date.isoformat(),
                "price_timestamp": timestamp,
                "price_close": price_map[date],
            })

    logger.info("Wrote %d price rows to %s", len(price_map), csv_path)


def get_max_height(csv_path: str) -> Optional[int]:
    """Get the maximum height from a cointime CSV file.

    Returns None if file doesn't exist or is empty.
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
        logger.warning("Error reading CSV for max height: %s", e)
        return None

    return max_height


def deduplicate_cointime_csv(csv_path: str) -> int:
    """Read, deduplicate by height, and rewrite cointime CSV if needed.
    
    Returns the number of duplicates removed. Only rewrites file if duplicates exist.
    """
    path = Path(csv_path)
    if not path.exists():
        return 0
    
    rows: List[dict] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    
    if not rows:
        return 0
    
    # Check for duplicates by keeping track of seen heights
    seen: dict[int, dict] = {}
    for row in rows:
        try:
            height = int(row["blockheight"])
            seen[height] = row
        except (ValueError, KeyError):
            continue
    
    if len(seen) == len(rows):
        return 0  # No duplicates
    
    # Rewrite with deduplication
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COINTIME_FIELDNAMES)
        writer.writeheader()
        for height in sorted(seen.keys()):
            writer.writerow(seen[height])
    
    removed = len(rows) - len(seen)
    logger.info("Deduplicated %d rows from %s", removed, csv_path)
    return removed
