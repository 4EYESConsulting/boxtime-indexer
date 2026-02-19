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
    append_output,
    deduplicate_by_height,
    get_last_height,
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
    def test_load_prices_coingecko_snapped_at_schema(self):
        """Successfully loads price data from CoinGecko snapped_at/price export format."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            writer = csv.writer(f)
            writer.writerow(["snapped_at", "price", "market_cap", "total_volume"])
            writer.writerow(["2019-07-01 00:00:00 UTC", "1.50", "100", "10"])
            writer.writerow(["2019-07-02 00:00:00 UTC", "2.00", "200", "20"])
            temp_path = f.name

        try:
            price_map, max_date = load_prices(temp_path)
            assert max_date == date(2019, 7, 2)
            assert price_map[date(2019, 7, 1)] == 1.50
            assert price_map[date(2019, 7, 2)] == 2.00
        finally:
            os.unlink(temp_path)

    def test_load_prices_unsupported_columns(self):
        """Raises ValueError when the price CSV has unsupported columns."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            writer = csv.writer(f)
            writer.writerow(["foo", "bar"])
            writer.writerow(["x", "y"])
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="Unsupported price CSV columns"):
                load_prices(temp_path)
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


class TestAppendOutput:
    """Tests for append_output()."""

    def test_append_output_creates_file_with_header(self):
        """Creates new file with header when file doesn't exist."""
        data = [
            HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
        ]
        data[0].block_date = date(2019, 7, 1)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"
            append_output(str(output_path), data)

            assert output_path.exists()
            with open(output_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 1
            assert rows[0]["blockheight"] == "1"

    def test_append_output_appends_to_existing_file(self):
        """Appends data to existing file without rewriting header."""
        data1 = [
            HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
        ]
        data1[0].block_date = date(2019, 7, 1)

        data2 = [
            HeightData(height=2, timestamp=1562065200000, cbc=200, cbd=20, cbs=180),
        ]
        data2[0].block_date = date(2019, 7, 2)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"
            
            # First append
            append_output(str(output_path), data1)
            # Second append
            append_output(str(output_path), data2)

            with open(output_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 2
            assert rows[0]["blockheight"] == "1"
            assert rows[1]["blockheight"] == "2"


class TestDeduplicateByHeight:
    """Tests for deduplicate_by_height()."""

    def test_deduplicate_keeps_last_occurrence(self):
        """Keeps the last occurrence of each height."""
        data = [
            HeightData(height=1, timestamp=1000, cbc=100, cbd=10, cbs=90),
            HeightData(height=2, timestamp=2000, cbc=200, cbd=20, cbs=180),
            HeightData(height=1, timestamp=1500, cbc=150, cbd=15, cbs=135),  # Duplicate
        ]

        result = deduplicate_by_height(data)

        assert len(result) == 2
        heights = [d.height for d in result]
        assert sorted(heights) == [1, 2]
        # Check that the last occurrence was kept (timestamp 1500, not 1000)
        height_1 = next(d for d in result if d.height == 1)
        assert height_1.timestamp == 1500

    def test_deduplicate_empty_list(self):
        """Returns empty list for empty input."""
        result = deduplicate_by_height([])
        assert result == []

    def test_deduplicate_no_duplicates(self):
        """Returns same data when no duplicates."""
        data = [
            HeightData(height=1, timestamp=1000, cbc=100, cbd=10, cbs=90),
            HeightData(height=2, timestamp=2000, cbc=200, cbd=20, cbs=180),
        ]

        result = deduplicate_by_height(data)

        assert len(result) == 2
        assert result[0].height == 1
        assert result[1].height == 2


class TestGetLastHeight:
    """Tests for get_last_height()."""

    def test_get_last_height_from_existing_file(self):
        """Returns max height from existing CSV."""
        data = [
            HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
            HeightData(height=5, timestamp=1562065200000, cbc=200, cbd=20, cbs=180),
            HeightData(height=3, timestamp=1562151600000, cbc=300, cbd=30, cbs=270),
        ]
        for d in data:
            d.block_date = date(2019, 7, 1)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"
            write_output(str(output_path), data)

            last_height = get_last_height(str(output_path))
            assert last_height == 5

    def test_get_last_height_file_not_found(self):
        """Returns None when file doesn't exist."""
        last_height = get_last_height("/nonexistent/path.csv")
        assert last_height is None

    def test_get_last_height_empty_file(self):
        """Returns None for empty file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"
            write_output(str(output_path), [])

            last_height = get_last_height(str(output_path))
            assert last_height is None
