"""Tests for src.config."""

import os
from unittest.mock import patch

from src.config import Config, load_config


def test_load_config_defaults():
    """load_config returns sensible defaults when no env vars are set."""
    env = {
        k: v
        for k, v in os.environ.items()
        if k
        not in {
            "NODE_URL",
            "PRICE_CSV_PATH",
            "COINTIME_OUTPUT_PATH",
            "PRICES_OUTPUT_PATH",
            "CHUNK_SIZE",
            "MAX_CONCURRENT",
            "START_HEIGHT",
        }
    }
    with patch.dict(os.environ, env, clear=True):
        cfg = load_config()

    assert cfg.node_url == "http://node:9053"
    assert cfg.price_csv_path == "input/erg_prices.csv"
    assert cfg.cointime_output_path == "output/cointime.csv"
    assert cfg.prices_output_path == "output/prices.csv"
    assert cfg.chunk_size == 5000
    assert cfg.max_concurrent == 50
    assert cfg.start_height == 1


def test_load_config_from_env():
    """load_config reads from environment variables."""
    env = {
        "NODE_URL": "http://mynode:9053",
        "PRICE_CSV_PATH": "custom/prices.csv",
        "COINTIME_OUTPUT_PATH": "custom/cointime.csv",
        "PRICES_OUTPUT_PATH": "custom/prices.csv",
        "CHUNK_SIZE": "100",
        "MAX_CONCURRENT": "10",
        "START_HEIGHT": "500",
    }
    with patch.dict(os.environ, env, clear=True):
        cfg = load_config()

    assert cfg.node_url == "http://mynode:9053"
    assert cfg.price_csv_path == "custom/prices.csv"
    assert cfg.cointime_output_path == "custom/cointime.csv"
    assert cfg.prices_output_path == "custom/prices.csv"
    assert cfg.chunk_size == 100
    assert cfg.max_concurrent == 10
    assert cfg.start_height == 500


def test_config_is_frozen():
    """Config is immutable."""
    cfg = Config(
        node_url="http://x",
        price_csv_path="p.csv",
        cointime_output_path="c.csv",
        prices_output_path="pp.csv",
        chunk_size=1,
        max_concurrent=1,
        start_height=1,
    )
    try:
        cfg.node_url = "http://y"
        assert False, "Should have raised FrozenInstanceError"
    except AttributeError:
        pass


def test_load_config_start_height_must_be_positive():
    """START_HEIGHT < 1 raises ValueError."""
    env = {
        "START_HEIGHT": "0",
    }
    with patch.dict(os.environ, env, clear=True):
        try:
            load_config()
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "START_HEIGHT must be >= 1" in str(e)


def test_load_config_start_height_negative_raises():
    """START_HEIGHT < 0 raises ValueError."""
    env = {
        "START_HEIGHT": "-1",
    }
    with patch.dict(os.environ, env, clear=True):
        try:
            load_config()
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "START_HEIGHT must be >= 1" in str(e)
