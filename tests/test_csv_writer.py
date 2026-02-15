"""Tests for src.csv_writer."""

import csv
import datetime
import os
import tempfile
from datetime import date
from pathlib import Path

import pytest

from src.csv_writer import (
    CSV_FIELDNAMES,
    get_max_height,
    load_bootstrap,
    load_prices,
    merge_with_prices,
    write_output,
)
from src.fetcher import HeightData


class TestLoadPrices:
    """Tests for load_prices()."""

    def test_load_prices_success(self):
        """Successfully loads price data from CSV."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            writer = csv.writer(f)
            writer.writerow(["Date", "Close"])
            writer.writerow(["2019-07-01", "1.50"])
            writer.writerow(["2019-07-02", "2.00"])
            writer.writerow(["2019-07-03", "2.50"])
            temp_path = f.name

        try:
            price_map, max_date = load_prices(temp_path)
            assert max_date == date(2019, 7, 3)
            assert price_map[date(2019, 7, 1)] == 1.50
            assert price_map[date(2019, 7, 2)] == 2.00
            assert price_map[date(2019, 7, 3)] == 2.50
        finally:
            os.unlink(temp_path)

    def test_load_prices_file_not_found(self):
        """Raises FileNotFoundError when CSV doesn't exist."""
        with pytest.raises(FileNotFoundError):
            load_prices("/nonexistent/path/prices.csv")

    def test_load_prices_invalid_row(self):
        """Skips invalid rows and logs warning."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            writer = csv.writer(f)
            writer.writerow(["Date", "Close"])
            writer.writerow(["2019-07-01", "1.50"])
            writer.writerow(["invalid-date", "invalid-price"])
            writer.writerow(["2019-07-03", "2.50"])
            temp_path = f.name

        try:
            price_map, max_date = load_prices(temp_path)
            assert max_date == date(2019, 7, 3)
            assert len(price_map) == 2
            assert date(2019, 7, 1) in price_map
            assert date(2019, 7, 3) in price_map
        finally:
            os.unlink(temp_path)

    def test_load_prices_empty(self):
        """Handles empty price file."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            writer = csv.writer(f)
            writer.writerow(["Date", "Close"])
            temp_path = f.name

        try:
            price_map, max_date = load_prices(temp_path)
            assert max_date is None
            assert price_map == {}
        finally:
            os.unlink(temp_path)


class TestLoadBootstrap:
    """Tests for load_bootstrap()."""

    def test_load_bootstrap_success(self):
        """Successfully loads bootstrap data from CSV."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            writer.writerow({
                "blockheight": "1",
                "blockheight_timestamp": "1561978800000",
                "blockheight_date": "2019-07-01",
                "coinblocks_created": "1000000000",
                "coinblocks_destroyed": "100000000",
                "coinblocks_stored": "900000000",
                "price_date": "2019-07-01",
                "price_close": "1.50",
            })
            writer.writerow({
                "blockheight": "2",
                "blockheight_timestamp": "1562065200000",
                "blockheight_date": "2019-07-02",
                "coinblocks_created": "1001000000",
                "coinblocks_destroyed": "100200000",
                "coinblocks_stored": "900800000",
                "price_date": "2019-07-02",
                "price_close": "2.00",
            })
            temp_path = f.name

        try:
            rows = load_bootstrap(temp_path)
            assert len(rows) == 2
            assert rows[0].height == 1
            assert rows[0].cbc == 1000000000
            assert rows[1].height == 2
            assert rows[1].cbc == 1001000000
        finally:
            os.unlink(temp_path)

    def test_load_bootstrap_file_not_found(self):
        """Returns empty list when bootstrap CSV doesn't exist."""
        rows = load_bootstrap("/nonexistent/path/bootstrap.csv")
        assert rows == []

    def test_load_bootstrap_invalid_row(self):
        """Skips invalid rows and logs warning."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            writer.writerow({
                "blockheight": "1",
                "blockheight_timestamp": "1561978800000",
                "blockheight_date": "2019-07-01",
                "coinblocks_created": "1000000000",
                "coinblocks_destroyed": "100000000",
                "coinblocks_stored": "900000000",
                "price_date": "2019-07-01",
                "price_close": "1.50",
            })
            writer.writerow({
                "blockheight": "invalid",
                "blockheight_timestamp": "invalid",
                "blockheight_date": "2019-07-02",
                "coinblocks_created": "1001000000",
                "coinblocks_destroyed": "100200000",
                "coinblocks_stored": "900800000",
                "price_date": "2019-07-02",
                "price_close": "2.00",
            })
            temp_path = f.name

        try:
            rows = load_bootstrap(temp_path)
            assert len(rows) == 1
            assert rows[0].height == 1
        finally:
            os.unlink(temp_path)


class TestGetMaxHeight:
    """Tests for get_max_height()."""

    def test_get_max_height_empty(self):
        """Returns None for empty list."""
        assert get_max_height([]) is None

    def test_get_max_height_single(self):
        """Returns height for single item."""
        data = [HeightData(height=10, timestamp=1000, cbc=100, cbd=10, cbs=90)]
        assert get_max_height(data) == 10

    def test_get_max_height_multiple(self):
        """Returns max height from multiple items."""
        data = [
            HeightData(height=5, timestamp=1000, cbc=100, cbd=10, cbs=90),
            HeightData(height=10, timestamp=2000, cbc=100, cbd=10, cbs=90),
            HeightData(height=3, timestamp=3000, cbc=100, cbd=10, cbs=90),
        ]
        assert get_max_height(data) == 10


class TestMergeWithPrices:
    """Tests for merge_with_prices()."""

    def test_merge_with_prices_matching_dates(self):
        """Merges price data for matching dates."""
        data = [
            HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
            HeightData(height=2, timestamp=1562065200000, cbc=100, cbd=10, cbs=90),
        ]
        for d in data:
            d.block_date = date(2019, 7, 1) if d.height == 1 else date(2019, 7, 2)

        price_map = {
            date(2019, 7, 1): 1.50,
            date(2019, 7, 2): 2.00,
        }

        result = merge_with_prices(data, price_map)

        assert result[0].price_date == date(2019, 7, 1)
        assert result[0].price_close == 1.50
        assert result[1].price_date == date(2019, 7, 2)
        assert result[1].price_close == 2.00

    def test_merge_with_prices_missing_dates(self):
        """Sets price to None when no matching date."""
        data = [
            HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
        ]
        data[0].block_date = date(2019, 7, 1)

        price_map = {
            date(2019, 8, 1): 1.50,
        }

        result = merge_with_prices(data, price_map)

        assert result[0].price_date is None
        assert result[0].price_close is None

    def test_merge_with_prices_no_explicit_block_date(self):
        """Uses computed block_date when not explicitly set."""
        data = [
            HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
        ]

        price_map = {date(2019, 7, 1): 1.50}

        result = merge_with_prices(data, price_map)

        assert result[0].price_date == date(2019, 7, 1)
        assert result[0].price_close == 1.50


class TestWriteOutput:
    """Tests for write_output()."""

    def test_write_output_success(self):
        """Writes complete CSV output."""
        data = [
            HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
            HeightData(height=2, timestamp=1562065200000, cbc=100, cbd=10, cbs=90),
        ]
        for d in data:
            d.block_date = date(2019, 7, 1) if d.height == 1 else date(2019, 7, 2)
            d.price_date = d.block_date
            d.price_close = 1.50 if d.height == 1 else 2.00

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"
            write_output(str(output_path), data)

            assert output_path.exists()
            with open(output_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 2
            assert rows[0]["blockheight"] == "1"
            assert rows[0]["coinblocks_created"] == "100"
            assert rows[0]["price_close"] == "1.5"
            assert rows[1]["blockheight"] == "2"
            assert rows[1]["price_close"] == "2.0"

    def test_write_output_creates_directory(self):
        """Creates output directory if it doesn't exist."""
        data = [
            HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "nested" / "dir" / "output.csv"
            write_output(str(output_path), data)

            assert output_path.exists()

    def test_write_output_empty(self):
        """Writes header only for empty data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"
            write_output(str(output_path), [])

            assert output_path.exists()
            with open(output_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 0
