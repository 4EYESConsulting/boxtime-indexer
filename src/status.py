"""Status CLI for boxtime-indexer."""

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path


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


def load_bootstrap_csv(csv_path: str) -> list[dict]:
    """Load bootstrap CSV and return list of row dicts."""
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


def main():
    parser = argparse.ArgumentParser(description="Check boxtime-indexer sync status")
    parser.add_argument(
        "--output-csv",
        default="output/cointime.csv",
        help="Path to output CSV (default: output/cointime.csv)",
    )
    parser.add_argument(
        "--bootstrap-csv",
        default="input/cointime.csv",
        help="Path to bootstrap CSV (default: input/cointime.csv)",
    )
    args = parser.parse_args()

    bootstrap_rows = load_bootstrap_csv(args.bootstrap_csv)
    output_rows = load_output_csv(args.output_csv)

    bootstrap_max = get_max_height(bootstrap_rows)
    output_max = get_max_height(output_rows)

    max_synced = max(bootstrap_max or 0, output_max or 0)

    if max_synced == 0:
        print("No data found. Run the indexer to start syncing.")
        sys.exit(1)

    bootstrap_date_range = get_date_range(bootstrap_rows)
    output_date_range = get_date_range(output_rows)

    min_date = bootstrap_date_range[0] if bootstrap_date_range[0] != "N/A" else output_date_range[0]
    max_date = output_date_range[1] if output_date_range[1] != "N/A" else bootstrap_date_range[1]

    total_rows = len(bootstrap_rows) + len(output_rows)

    print(f"Sync Status")
    print(f"=" * 40)
    print(f"Bootstrap file: {args.bootstrap_csv}")
    print(f"  - Max height: {bootstrap_max or 'N/A'}")
    print(f"  - Rows: {len(bootstrap_rows)}")
    print(f"")
    print(f"Output file: {args.output_csv}")
    print(f"  - Max height: {output_max or 'N/A'}")
    print(f"  - Rows: {len(output_rows)}")
    print(f"")
    print(f"Combined:")
    print(f"  - Max synced height: {max_synced}")
    print(f"  - Total rows: {total_rows}")
    print(f"  - Date range: {min_date} to {max_date}")

    if output_rows:
        try:
            latest_timestamp = int(output_rows[-1]["blockheight_timestamp"])
            first_timestamp = int(output_rows[0]["blockheight_timestamp"])
            time_diff_seconds = latest_timestamp // 1000 - first_timestamp // 1000
            if time_diff_seconds > 0:
                heights_processed = len(output_rows)
                rate = heights_processed / time_diff_seconds
                print(f"  - Rate: {rate:.2f} blocks/second")
        except (KeyError, ValueError, IndexError):
            pass


if __name__ == "__main__":
    main()
