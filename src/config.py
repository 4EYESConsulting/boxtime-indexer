"""Environment-based configuration."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Config:
    """Application configuration loaded from environment variables."""

    node_url: str
    database_url: str
    coingecko_api_key: Optional[str]
    coingecko_pro: bool
    chunk_size: int
    max_concurrent: int
    poll_interval: int
    start_height: int


def load_config() -> Config:
    """Load configuration from environment variables with defaults."""
    return Config(
        node_url=os.environ.get("NODE_URL", "http://node:9053"),
        database_url=os.environ.get(
            "DATABASE_URL", "postgresql://boxtime:boxtime@db:5432/boxtime"
        ),
        coingecko_api_key=os.environ.get("COINGECKO_API_KEY"),
        coingecko_pro=os.environ.get("COINGECKO_PRO", "false").lower() == "true",
        chunk_size=int(os.environ.get("CHUNK_SIZE", "5000")),
        max_concurrent=int(os.environ.get("MAX_CONCURRENT", "50")),
        poll_interval=int(os.environ.get("POLL_INTERVAL", "60")),
        start_height=int(os.environ.get("START_HEIGHT", "1")),
    )
