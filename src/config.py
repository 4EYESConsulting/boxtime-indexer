"""Environment-based configuration."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Config:
    """Application configuration loaded from environment variables."""

    node_url: str
    price_csv_path: str
    bootstrap_csv_path: Optional[str]
    csv_output_path: str
    chunk_size: int
    max_concurrent: int
    poll_interval: int
    start_height: int


def load_config() -> Config:
    """Load configuration from environment variables with defaults."""
    return Config(
        node_url=os.environ.get("NODE_URL", "http://node:9053"),
        price_csv_path=os.environ.get("PRICE_CSV_PATH", "input/erg_prices.csv"),
        bootstrap_csv_path=os.environ.get("BOOTSTRAP_CSV_PATH", "input/cointime.csv"),
        csv_output_path=os.environ.get("CSV_OUTPUT_PATH", "output/cointime.csv"),
        chunk_size=int(os.environ.get("CHUNK_SIZE", "5000")),
        max_concurrent=int(os.environ.get("MAX_CONCURRENT", "50")),
        poll_interval=int(os.environ.get("POLL_INTERVAL", "60")),
        start_height=int(os.environ.get("START_HEIGHT", "1")),
    )
