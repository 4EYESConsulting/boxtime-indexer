# AGENTS.md

## Project Overview

Cointime Economics ETL/indexer for the Ergo blockchain. Computes coinblocks created (CBC), destroyed (CBD), and stored (CBS) per block height, outputs to CSV.

## Build Commands

- `pixi install` — Install dependencies
- `pixi run test` — Run test suite
- `docker compose --profile local-node up -d` — Start indexer with local node
- `docker compose up -d` — Start indexer with remote node (set NODE_URL in .env)

## Testing

Run tests before committing: `pixi run test`

## Code Style

- Python 3.12+
- Use dataclasses for data structures
- Async/await for I/O operations (aiohttp)
- Type hints required
- No comments unless explaining complex logic

## Key Patterns

- Config via `src/config.py` — environment variables with defaults
- Fetching via `src/fetcher.py` — async HTTP to Ergo node
- CSV handling via `src/csv_writer.py` — price data, bootstrap, output
- Entry point: `src/main.py` → `src/indexer.py`

## Ergo Node

Requires v6.0.1+ with `extraIndex = true`. REST API on port 9053.
