"""Status CLI for boxtime-indexer."""

import argparse
import csv
import os
import sys
from datetime import date as date_type
from pathlib import Path
from typing import Optional, Tuple

import aiohttp

from src.fetcher import find_first_height_by_date


def load_cointime_csv(csv_path: str) -> list[dict]:
    """Load cointime CSV and return list of row dicts."""
    path = Path(csv_path)
    if not path.exists():
        return []
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def load_prices_csv(csv_path: str) -> list[dict]:
    """Load prices CSV and return list of row dicts."""
    path = Path(csv_path)
    if not path.exists():
        return []
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def get_max_height(rows: list[dict]) -> int | None:
    """Get max height from cointime rows."""
    if not rows:
        return None
    return max(int(r["blockheight"]) for r in rows)


def get_date_range(rows: list[dict]) -> tuple[str, str]:
    """Get min and max block dates from cointime rows."""
    if not rows:
        return ("N/A", "N/A")
    dates = []
    for row in rows:
        try:
            ts = int(row["blockheight_timestamp"])
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date()
            dates.append(str(dt))
        except (ValueError, KeyError):
            continue
    if not dates:
        return ("N/A", "N/A")
    return (min(dates), max(dates))


def load_price_csv(csv_path: str) -> Tuple[Optional[date_type], str]:
    """Load price CSV and return max date and status message."""
    path = Path(csv_path)
    if not path.exists():
        return None, f"Price CSV not found: {csv_path}"
    
    dates = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                date_str = row.get("Date") or row.get("snapped_at", "").split()[0]
                if date_str:
                    try:
                        from datetime import datetime
                        dt = datetime.strptime(date_str, "%Y-%m-%d").date()
                        dates.append(dt)
                    except ValueError:
                        continue
    except Exception as e:
        return None, f"Error reading price CSV: {e}"
    
    if not dates:
        return None, "No valid dates found in price CSV"
    
    return max(dates), "OK"


async def fetch_chain_height(session: aiohttp.ClientSession, node_url: str) -> Optional[int]:
    """Fetch current indexed height from Ergo node."""
    try:
        async with session.get(
            f"{node_url}/blockchain/indexedHeight",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("indexedHeight")
    except Exception:
        return None


async def main_async():
    parser = argparse.ArgumentParser(description="Check boxtime-indexer sync status")
    parser.add_argument(
        "--cointime-csv",
        default=os.environ.get("COINTIME_OUTPUT_PATH", "output/cointime.csv"),
        help="Path to cointime CSV (default: COINTIME_OUTPUT_PATH env var or output/cointime.csv)",
    )
    parser.add_argument(
        "--prices-csv",
        default=os.environ.get("PRICES_OUTPUT_PATH", "output/prices.csv"),
        help="Path to prices CSV (default: PRICES_OUTPUT_PATH env var or output/prices.csv)",
    )
    parser.add_argument(
        "--node-url",
        default=os.environ.get("NODE_URL", "http://localhost:9053"),
        help="Ergo node URL (default: NODE_URL env var or http://localhost:9053)",
    )
    parser.add_argument(
        "--price-csv",
        default=os.environ.get("PRICE_CSV_PATH", "input/erg_prices.csv"),
        help="Path to input price CSV (default: PRICE_CSV_PATH env var or input/erg_prices.csv)",
    )
    args = parser.parse_args()

    cointime_rows = load_cointime_csv(args.cointime_csv)
    prices_rows = load_prices_csv(args.prices_csv)
    cointime_max = get_max_height(cointime_rows)
    min_date, max_date = get_date_range(cointime_rows)
    total_cointime_rows = len(cointime_rows)
    total_prices_rows = len(prices_rows)

    max_price_date, price_status = load_price_csv(args.price_csv)

    async with aiohttp.ClientSession() as session:
        chain_height = await fetch_chain_height(session, args.node_url)

        if chain_height and max_price_date:
            search_start = max((cointime_max or 0) + 1, 1)
            if chain_height >= search_start:
                target_height = await find_first_height_by_date(
                    session,
                    args.node_url,
                    search_start,
                    chain_height,
                    max_price_date,
                )
            else:
                target_height = cointime_max or 0
        else:
            target_height = None

    print(f"Sync Status")
    print(f"=" * 40)

    if not cointime_rows and not cointime_max:
        print("No cointime data found. Run the indexer to start syncing.")
        if chain_height:
            print(f"\nChain height: {chain_height:,}")
            if target_height is not None:
                print(f"Target height: {target_height:,} (from price data)")
        sys.exit(0)

    print(f"Cointime file: {args.cointime_csv}")
    print(f"  - Max height: {cointime_max or 'N/A'}")
    print(f"  - Total rows: {total_cointime_rows:,}")
    print(f"  - Date range: {min_date} to {max_date}")
    print()

    print(f"Prices file: {args.prices_csv}")
    print(f"  - Total rows: {total_prices_rows:,}")
    print()

    if chain_height:
        print(f"Chain status:")
        print(f"  - Current chain height: {chain_height:,}")

    if max_price_date and target_height is not None:
        print(f"Price data status: {price_status}")
        print(f"  - Max price date: {max_price_date}")
        print(f"  - Target height: {target_height:,}")

        if cointime_max is not None and target_height > cointime_max:
            remaining = target_height - cointime_max
            print(f"  - Remaining: {remaining:,} blocks")
        elif cointime_max is not None and target_height <= cointime_max:
            print(f"  - Status: Up to date with price data")
    elif max_price_date:
        print(f"Price data status: {price_status}")
        print(f"  - Max price date: {max_price_date}")
        print("  - Warning: Could not determine target height")
    else:
        print(f"Price data status: {price_status}")
        print("  - Warning: Cannot determine target height without price data")


def main():
    import asyncio
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
