# AGENTS.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview
boxtime-indexer is the ETL/indexer companion to the [boxtime](https://github.com/4EYESConsulting/boxtime) library. It connects to Ergo blockchain nodes, computes Cointime Economics metrics (coinblocks created, destroyed, stored) for each block height, and persists the results to a SQLite database. The boxtime library then queries this database for instant lookups instead of making live HTTP calls to nodes.

## Architecture
- **Indexer**: walks Ergo block heights, computes CBC/CBD/CBS per height using the boxtime library's async internals, and writes results to SQLite.
- **Database**: SQLite file with one row per block height. Key columns: `height`, `timestamp`, `cbc`, `cbd`, `cbs`.
- **Incremental sync**: on each run, the indexer picks up from `MAX(height)` in the DB and processes only new blocks.
- **Backfill**: initial population covers ~1.7M+ heights. Uses concurrent fetching with multiple Ergo nodes for throughput.

## Key Domain Concepts
- **Coinblocks Created (CBC)**: total circulating supply at a block height (nanoERGs). Sourced from the node's `/emission/at/{height}` endpoint.
- **Coinblocks Destroyed (CBD)**: `sum(input.value × (height - input.inclusion_height))` for all transaction inputs in a block.
- **Coinblocks Stored (CBS)**: `CBC - CBD`.
- All values are in **nanoERGs** (1 ERG = 1,000,000,000 nanoERG).

## Ergo Node Requirements
Nodes must have `ergo.node.extraIndex = true` enabled. The REST API typically runs on port 9053 (not the P2P port 9030). Known public indexed nodes include the eutxo.de cluster (e.g. `https://ergo-node-1.eutxo.de`).
