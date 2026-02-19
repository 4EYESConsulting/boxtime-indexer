"""Status CLI for boxtime-indexer."""

import argparse
import csv
import os
import sys
from datetime import date as date_type
from pathlib import Path
from typing import Optional, Tuple

import aiohttp

from src.fetcher import find_height_by_date


def load_output_csv(csv_path: str) -> list[dict]:
    """Load output CSV and return list of row dicts."""
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
    """Get max height from rows."""
    if not rows:
        return None
    return max(int(r["blockheight"]) for r in rows)


def get_date_range(rows: list[dict]) -> tuple[str, str]:
    """Get min and max block dates from rows."""
    if not rows:
        return ("N/A", "N/A")
    dates = [r["blockheight_date"] for r in rows if r.get("blockheight_date")]
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


def format_eta(seconds: float) -> str:
    """Format seconds as human-readable ETA."""
    if seconds < 60:
        return f"{seconds:.0f} seconds"
    elif seconds < 3600:
        return f"{seconds/60:.1f} minutes"
    elif seconds < 86400:
        return f"{seconds/3600:.1f} hours"
    else:
        return f"{seconds/86400:.1f} days"


async def main_async():
    parser = argparse.ArgumentParser(description="Check boxtime-indexer sync status")
    parser.add_argument(
        "--output-csv",
        default="output/cointime.csv",
        help="Path to output CSV (default: output/cointime.csv)",
    )
    parser.add_argument(
        "--node-url",
        default=os.environ.get("NODE_URL", "http://localhost:9053"),
        help="Ergo node URL (default: NODE_URL env var or http://localhost:9053)",
    )
    parser.add_argument(
        "--price-csv",
        default=os.environ.get("PRICE_CSV_PATH", "input/erg_prices.csv"),
        help="Path to price CSV (default: PRICE_CSV_PATH env var or input/erg_prices.csv)",
    )
    args = parser.parse_args()

    output_rows = load_output_csv(args.output_csv)
    output_max = get_max_height(output_rows)
    min_date, max_date = get_date_range(output_rows)
    total_rows = len(output_rows)

    max_price_date, price_status = load_price_csv(args.price_csv)

    async with aiohttp.ClientSession() as session:
        chain_height = await fetch_chain_height(session, args.node_url)

        if chain_height and max_price_date:
            search_start = max((output_max or 0) + 1, 1)
            if chain_height >= search_start:
                target_height = await find_height_by_date(
                    session,
                    args.node_url,
                    search_start,
                    chain_height,
                    max_price_date,
                )
            else:
                target_height = output_max or 0
        else:
            target_height = None

    print(f"Sync Status")
    print(f"=" * 40)

    if not output_rows and not output_max:
        print("No data found. Run the indexer to start syncing.")
        if chain_height:
            print(f"\nChain height: {chain_height:,}")
            if target_height is not None:
                print(f"Target height: {target_height:,} (from price data)")
        sys.exit(0)

    print(f"Output file: {args.output_csv}")
    print(f"  - Max height: {output_max or 'N/A'}")
    print(f"  - Total rows: {total_rows:,}")
    print(f"  - Date range: {min_date} to {max_date}")
    print()

    if chain_height:
        print(f"Chain status:")
        print(f"  - Current chain height: {chain_height:,}")

    if max_price_date and target_height is not None:
        print(f"Price data status: {price_status}")
        print(f"  - Max price date: {max_price_date}")
        print(f"  - Target height: {target_height:,}")

        if output_max is not None and target_height > output_max:
            remaining = target_height - output_max
            print(f"  - Remaining: {remaining:,} blocks")

            if output_rows and len(output_rows) > 1:
                try:
                    latest_timestamp = int(output_rows[-1]["blockheight_timestamp"])
                    first_timestamp = int(output_rows[0]["blockheight_timestamp"])
                    time_diff_seconds = (latest_timestamp - first_timestamp) / 1000

                    if time_diff_seconds > 0:
                        heights_processed = len(output_rows)
                        rate = heights_processed / time_diff_seconds
                        eta_seconds = remaining / rate if rate > 0 else 0
                        eta_str = format_eta(eta_seconds)
                        print(f"  - ETA: {eta_str} at current rate ({rate:.1f} blocks/sec)")
                except (KeyError, ValueError, IndexError):
                    pass
        elif output_max is not None and target_height <= output_max:
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
