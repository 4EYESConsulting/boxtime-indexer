"""Tests for src.csv_writer."""

import csv
import os
import tempfile
from datetime import date
from pathlib import Path

import pytest

from src.csv_writer import (
    COINTIME_FIELDNAMES,
    PRICES_FIELDNAMES,
    get_max_height,
    load_prices,
    write_cointime_csv,
    write_prices_csv,
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


class TestWriteCointimeCsv:
    """Tests for write_cointime_csv()."""

    def test_write_cointime_csv_creates_file_with_header(self):
        """Creates new file with header when file doesn't exist."""
        data = [
            HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
            HeightData(height=2, timestamp=1562065200000, cbc=200, cbd=20, cbs=180),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "cointime.csv"
            write_cointime_csv(str(output_path), data)

            assert output_path.exists()
            with open(output_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 2
            assert rows[0]["blockheight"] == "1"
            assert rows[0]["blockheight_timestamp"] == "1561978800000"
            assert rows[0]["coinblocks_created"] == "100"
            assert rows[0]["coinblocks_destroyed"] == "10"
            assert rows[0]["coinblocks_stored"] == "90"
            assert rows[1]["blockheight"] == "2"

    def test_write_cointime_csv_appends_to_existing_file(self):
        """Appends data to existing file without rewriting header."""
        data1 = [
            HeightData(height=1, timestamp=1561978800000, cbc=100, cbd=10, cbs=90),
        ]
        data2 = [
            HeightData(height=2, timestamp=1562065200000, cbc=200, cbd=20, cbs=180),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "cointime.csv"
            
            # First write
            write_cointime_csv(str(output_path), data1)
            # Second write (append)
            write_cointime_csv(str(output_path), data2)

            with open(output_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 2
            assert rows[0]["blockheight"] == "1"
            assert rows[1]["blockheight"] == "2"


class TestWritePricesCsv:
    """Tests for write_prices_csv()."""

    def test_write_prices_csv_success(self):
        """Writes deduplicated price data with timestamps."""
        price_map = {
            date(2019, 7, 1): 1.50,
            date(2019, 7, 2): 2.00,
            date(2019, 7, 3): 2.50,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "prices.csv"
            write_prices_csv(str(output_path), price_map)

            assert output_path.exists()
            with open(output_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 3
            assert rows[0]["price_date"] == "2019-07-01"
            assert rows[0]["price_close"] == "1.5"
            # Check UTC timestamp is present
            assert "price_timestamp" in rows[0]
            assert int(rows[0]["price_timestamp"]) > 0

    def test_write_prices_csv_sorted_by_date(self):
        """Writes prices sorted by date."""
        # Insert in reverse order
        price_map = {
            date(2019, 7, 3): 2.50,
            date(2019, 7, 1): 1.50,
            date(2019, 7, 2): 2.00,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "prices.csv"
            write_prices_csv(str(output_path), price_map)

            with open(output_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert rows[0]["price_date"] == "2019-07-01"
            assert rows[1]["price_date"] == "2019-07-02"
            assert rows[2]["price_date"] == "2019-07-03"


class TestGetMaxHeight:
    """Tests for get_max_height()."""

    def test_get_max_height_file_not_found(self):
        """Returns None when file doesn't exist."""
        result = get_max_height("/nonexistent/path.csv")
        assert result is None

    def test_get_max_height_empty_file(self):
        """Returns None for empty file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "cointime.csv"
            # Create empty file with just header
            with open(output_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=COINTIME_FIELDNAMES)
                writer.writeheader()

            result = get_max_height(str(output_path))
            assert result is None

    def test_get_max_height_single(self):
        """Returns height for single item."""
        data = [
            HeightData(height=10, timestamp=1000, cbc=100, cbd=10, cbs=90),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "cointime.csv"
            write_cointime_csv(str(output_path), data)

            result = get_max_height(str(output_path))
            assert result == 10

    def test_get_max_height_multiple(self):
        """Returns max height from multiple items."""
        data = [
            HeightData(height=5, timestamp=1000, cbc=100, cbd=10, cbs=90),
            HeightData(height=10, timestamp=2000, cbc=100, cbd=10, cbs=90),
            HeightData(height=3, timestamp=3000, cbc=100, cbd=10, cbs=90),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "cointime.csv"
            write_cointime_csv(str(output_path), data)

            result = get_max_height(str(output_path))
            assert result == 10
