"""Tests for src.status."""

import csv
import sys
from pathlib import Path

from src.status import main


def _write_status_rows(path: Path, rows: list[dict]) -> None:
    fieldnames = ["blockheight", "blockheight_timestamp", "blockheight_date"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_status_uses_output_as_combined_source(tmp_path, capsys):
    bootstrap_csv = tmp_path / "bootstrap.csv"
    output_csv = tmp_path / "output.csv"

    rows = [
        {
            "blockheight": "1",
            "blockheight_timestamp": "1561978800000",
            "blockheight_date": "2019-07-01",
        },
        {
            "blockheight": "2",
            "blockheight_timestamp": "1562065200000",
            "blockheight_date": "2019-07-02",
        },
    ]
    _write_status_rows(bootstrap_csv, rows)
    _write_status_rows(output_csv, rows)

    argv = sys.argv
    sys.argv = [
        "status",
        "--bootstrap-csv",
        str(bootstrap_csv),
        "--output-csv",
        str(output_csv),
    ]
    try:
        main()
    finally:
        sys.argv = argv

    output = capsys.readouterr().out
    assert "Max synced height: 2" in output
    assert "Total rows: 2" in output
    assert "Date range: 2019-07-01 to 2019-07-02" in output


def test_status_falls_back_to_bootstrap_when_output_missing(tmp_path, capsys):
    bootstrap_csv = tmp_path / "bootstrap.csv"
    output_csv = tmp_path / "missing_output.csv"

    rows = [
        {
            "blockheight": "10",
            "blockheight_timestamp": "1561978800000",
            "blockheight_date": "2019-07-01",
        },
        {
            "blockheight": "11",
            "blockheight_timestamp": "1562065200000",
            "blockheight_date": "2019-07-02",
        },
    ]
    _write_status_rows(bootstrap_csv, rows)

    argv = sys.argv
    sys.argv = [
        "status",
        "--bootstrap-csv",
        str(bootstrap_csv),
        "--output-csv",
        str(output_csv),
    ]
    try:
        main()
    finally:
        sys.argv = argv

    output = capsys.readouterr().out
    assert "Max synced height: 11" in output
    assert "Total rows: 2" in output
    assert "Date range: 2019-07-01 to 2019-07-02" in output
