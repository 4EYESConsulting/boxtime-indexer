"""Environment-based configuration."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """Application configuration loaded from environment variables."""

    node_url: str
    price_csv_path: str
    cointime_output_path: str
    prices_output_path: str
    chunk_size: int
    max_concurrent: int
    start_height: int


def load_config() -> Config:
    """Load configuration from environment variables with defaults."""
    start_height = int(os.environ.get("START_HEIGHT", "1"))
    if start_height < 1:
        raise ValueError("START_HEIGHT must be >= 1")

    return Config(
        node_url=os.environ.get("NODE_URL", "http://node:9053"),
        price_csv_path=os.environ.get("PRICE_CSV_PATH", "input/erg_prices.csv"),
        cointime_output_path=os.environ.get("COINTIME_OUTPUT_PATH", "output/cointime.csv"),
        prices_output_path=os.environ.get("PRICES_OUTPUT_PATH", "output/prices.csv"),
        chunk_size=int(os.environ.get("CHUNK_SIZE", "5000")),
        max_concurrent=int(os.environ.get("MAX_CONCURRENT", "50")),
        start_height=start_height,
    )
