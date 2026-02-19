"""Tests for src.status."""

import csv
import sys
from pathlib import Path
from unittest.mock import patch

import aiohttp
import pytest
from aioresponses import aioresponses

from src.status import fetch_chain_height, load_output_csv, get_max_height, get_date_range, main_async


def _write_status_rows(path: Path, rows: list[dict]) -> None:
    fieldnames = ["blockheight", "blockheight_timestamp", "blockheight_date"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


class TestLoadOutputCsv:
    """Tests for load_output_csv()."""

    def test_load_output_csv_success(self, tmp_path):
        """Successfully loads output CSV."""
        csv_path = tmp_path / "output.csv"
        rows = [
            {"blockheight": "1", "blockheight_timestamp": "1561978800000", "blockheight_date": "2019-07-01"},
            {"blockheight": "2", "blockheight_timestamp": "1562065200000", "blockheight_date": "2019-07-02"},
        ]
        _write_status_rows(csv_path, rows)

        result = load_output_csv(str(csv_path))

        assert len(result) == 2
        assert result[0]["blockheight"] == "1"
        assert result[1]["blockheight"] == "2"

    def test_load_output_csv_file_not_found(self):
        """Returns empty list when file doesn't exist."""
        result = load_output_csv("/nonexistent/path.csv")
        assert result == []


class TestGetMaxHeight:
    """Tests for get_max_height()."""

    def test_get_max_height_with_data(self):
        """Returns max height from rows."""
        rows = [
            {"blockheight": "1"},
            {"blockheight": "5"},
            {"blockheight": "3"},
        ]
        result = get_max_height(rows)
        assert result == 5

    def test_get_max_height_empty(self):
        """Returns None for empty list."""
        result = get_max_height([])
        assert result is None


class TestGetDateRange:
    """Tests for get_date_range()."""

    def test_get_date_range_with_data(self):
        """Returns min and max dates."""
        rows = [
            {"blockheight_date": "2019-07-01"},
            {"blockheight_date": "2019-07-05"},
            {"blockheight_date": "2019-07-03"},
        ]
        result = get_date_range(rows)
        assert result == ("2019-07-01", "2019-07-05")

    def test_get_date_range_empty(self):
        """Returns N/A for empty list."""
        result = get_date_range([])
        assert result == ("N/A", "N/A")


class TestFetchChainHeight:
    """Tests for fetch_chain_height()."""

    @pytest.mark.asyncio
    async def test_fetch_chain_height_success(self):
        """Successfully fetches chain height."""
        with aioresponses() as m:
            m.get(
                "http://test-node:9053/blockchain/indexedHeight",
                payload={"indexedHeight": 1750000, "fullHeight": 1750000},
            )
            async with aiohttp.ClientSession() as session:
                result = await fetch_chain_height(session, "http://test-node:9053")
                assert result == 1750000

    @pytest.mark.asyncio
    async def test_fetch_chain_height_failure(self):
        """Returns None on failure."""
        with aioresponses() as m:
            m.get("http://test-node:9053/blockchain/indexedHeight", status=500)
            async with aiohttp.ClientSession() as session:
                result = await fetch_chain_height(session, "http://test-node:9053")
                assert result is None


class TestMainAsync:
    """Tests for main_async()."""

    @pytest.mark.asyncio
    async def test_main_shows_output_stats(self, tmp_path, capsys):
        """Shows output file stats."""
        output_csv = tmp_path / "output.csv"
        rows = [
            {"blockheight": "1", "blockheight_timestamp": "1561978800000", "blockheight_date": "2019-07-01"},
            {"blockheight": "2", "blockheight_timestamp": "1562065200000", "blockheight_date": "2019-07-02"},
        ]
        _write_status_rows(output_csv, rows)

        argv = sys.argv
        sys.argv = [
            "status",
            "--output-csv",
            str(output_csv),
            "--node-url",
            "http://test-node:9053",
        ]
        try:
            with aioresponses() as m:
                m.get(
                    "http://test-node:9053/blockchain/indexedHeight",
                    payload={"indexedHeight": 100},
                )
                with patch("src.status.find_height_by_date", return_value=50):
                    await main_async()
        finally:
            sys.argv = argv

        output = capsys.readouterr().out
        assert "Max height: 2" in output
        assert "Total rows: 2" in output
        assert "Date range: 2019-07-01 to 2019-07-02" in output
        assert "Target height: 50" in output

    @pytest.mark.asyncio
    async def test_main_no_data(self, tmp_path, capsys):
        """Shows message when no data found."""
        output_csv = tmp_path / "output.csv"

        argv = sys.argv
        sys.argv = [
            "status",
            "--output-csv",
            str(output_csv),
            "--node-url",
            "http://test-node:9053",
        ]
        try:
            with aioresponses() as m:
                m.get(
                    "http://test-node:9053/blockchain/indexedHeight",
                    payload={"indexedHeight": 1000},
                )
                with patch("src.status.find_height_by_date", return_value=500):
                    with pytest.raises(SystemExit) as exc_info:
                        await main_async()
                    assert exc_info.value.code == 0
        finally:
            sys.argv = argv

        output = capsys.readouterr().out
        assert "No data found" in output
        assert "Chain height: 1,000" in output
