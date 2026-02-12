# AGENTS.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview
boxtime-indexer is the ETL/indexer companion to the [boxtime](https://github.com/4EYESConsulting/boxtime) library. It connects to a local Ergo blockchain node, computes Cointime Economics metrics (coinblocks created, destroyed, stored) for each block height, and persists the results to a PostgreSQL database.

## Architecture
- **Ergo Node**: local node running in Docker with `extraIndex = true`. Requires v6.0.1+ for the `/blockchain/block/byHeaderId/{headerId}` endpoint.
- **Indexer**: walks Ergo block heights, computes CBC/CBD/CBS per height via 3 HTTP calls to the local node, and writes results to PostgreSQL.
- **Database**: PostgreSQL with one row per block height. Columns: `height`, `timestamp`, `cbc`, `cbd`, `cbs`. All values in nanoERGs (NUMERIC for cbc/cbd/cbs as CBD can exceed BIGINT range).
- **Incremental sync**: on each run, the indexer fills gaps, resumes from `MAX(height)`, and processes new blocks.
- **Backfill**: initial population covers ~1.7M+ heights. Uses concurrent fetching bounded by a semaphore.
- **Reorg handling**: poll loop verifies parent hash continuity and rolls back on chain reorganizations.

## Key Domain Concepts
- **Coinblocks Created (CBC)**: total circulating supply at a block height (nanoERGs). Sourced from the node's `GET /emission/at/{height}` endpoint.
- **Coinblocks Destroyed (CBD)**: `sum(input.value × (height - input.inclusion_height))` for all transaction inputs in a block, **excluding the emission contract box** (identified by its constant ergoTree). The emission box carries the unissued supply and is consumed/recreated each block; including it would massively inflate CBD. Sourced from `GET /blockchain/block/byHeaderId/{headerId}` which returns indexed transactions with full input data.
- **Coinblocks Stored (CBS)**: `CBC - CBD`.
- All values are in **nanoERGs** (1 ERG = 1,000,000,000 nanoERG).

## Ergo Node Requirements
Node must be v6.0.1+ with `ergo.node.extraIndex = true` enabled. The REST API runs on port 9053. The docker-compose setup includes a local node by default.

## Workflow Rules
1. Each part of the plan must have a corresponding issue in the GitHub repository.
2. Each issue must be worked on in a separate branch with name starting with `ldgaetano/`.
3. Create commits accordingly and then a PR for each issue.
