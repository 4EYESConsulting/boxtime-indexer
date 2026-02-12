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
            "DATABASE_URL",
            "CHUNK_SIZE",
            "MAX_CONCURRENT",
            "POLL_INTERVAL",
            "START_HEIGHT",
        }
    }
    with patch.dict(os.environ, env, clear=True):
        cfg = load_config()

    assert cfg.node_url == "http://node:9053"
    assert cfg.database_url == "postgresql://boxtime:boxtime@db:5432/boxtime"
    assert cfg.chunk_size == 5000
    assert cfg.max_concurrent == 50
    assert cfg.poll_interval == 60
    assert cfg.start_height == 1


def test_load_config_from_env():
    """load_config reads from environment variables."""
    env = {
        "NODE_URL": "http://mynode:9053",
        "DATABASE_URL": "postgresql://u:p@host/db",
        "CHUNK_SIZE": "100",
        "MAX_CONCURRENT": "10",
        "POLL_INTERVAL": "30",
        "START_HEIGHT": "500",
    }
    with patch.dict(os.environ, env, clear=True):
        cfg = load_config()

    assert cfg.node_url == "http://mynode:9053"
    assert cfg.database_url == "postgresql://u:p@host/db"
    assert cfg.chunk_size == 100
    assert cfg.max_concurrent == 10
    assert cfg.poll_interval == 30
    assert cfg.start_height == 500


def test_config_is_frozen():
    """Config is immutable."""
    cfg = Config(
        node_url="http://x",
        database_url="postgresql://x",
        chunk_size=1,
        max_concurrent=1,
        poll_interval=1,
        start_height=1,
    )
    try:
        cfg.node_url = "http://y"
        assert False, "Should have raised FrozenInstanceError"
    except AttributeError:
        pass
