# boxtime-indexer

Cointime Economics ETL/indexer for the [Ergo](https://ergoplatform.org) blockchain. Pre-computes coinblocks created (CBC), coinblocks destroyed (CBD), and coinblocks stored (CBS) for every block height and stores the results in PostgreSQL.

Companion to the [boxtime](https://github.com/4EYESConsulting/boxtime) library.

## Quickstart

```bash
git clone https://github.com/4EYESConsulting/boxtime-indexer.git
cd boxtime-indexer
cp .env.example .env
docker compose up -d
```

The Ergo node will sync the blockchain first (this takes time on first run). Once synced, the indexer automatically backfills all historical blocks and then polls for new ones.

## Restoring from a snapshot

To skip the full backfill (~1.7M+ blocks), download a pre-built database snapshot:

```bash
# Download the latest snapshot from GitHub Releases
# Then restore it:
./scripts/snapshot.sh restore boxtime-pgdata-<height>.tar.gz
docker compose up -d
```

The indexer resumes from `MAX(height)` automatically.

## Configuration

All settings are environment variables with sensible defaults for the docker-compose setup. Edit `.env` to customize:

| Variable | Default | Description |
|---|---|---|
| `NODE_URL` | `http://node:9053` | Ergo node API URL |
| `DATABASE_URL` | `postgresql://boxtime:boxtime@db:5432/boxtime` | PostgreSQL connection string |
| `CHUNK_SIZE` | `5000` | Heights per batch during backfill |
| `MAX_CONCURRENT` | `50` | Maximum concurrent requests to the node |
| `POLL_INTERVAL` | `60` | Seconds between chain-tip checks after backfill |
| `START_HEIGHT` | `1` | Height to start from if DB is empty |

## Architecture

Three Docker services:

- **node** — Ergo full node (`ergoplatform/ergo`) with `extraIndex = true` (required for the indexed block API). Must be v6.0.1+.
- **db** — PostgreSQL 17, stores one row per block height.
- **indexer** — Python async service that fetches data from the node and writes to the database.

### Database schema

```sql
CREATE TABLE cointime (
    height    INTEGER PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    cbc       BIGINT NOT NULL,
    cbd       BIGINT NOT NULL,
    cbs       BIGINT NOT NULL
);
```

All cointime values are in **nanoERGs** (1 ERG = 1,000,000,000 nanoERG).

### How it works

**Per-height fetch** (3 HTTP calls to the Ergo node):

1. `GET /emission/at/{h}` → `totalCoinsIssued` = CBC
2. `GET /blocks/at/{h}` → header ID
3. `GET /blockchain/block/byHeaderId/{id}` → indexed block with full transaction inputs → compute CBD + extract timestamp

CBS = CBC − CBD.

**Startup sequence:**

1. Wait for the Ergo node to be synced (checks `fullHeight` vs `indexedHeight`)
2. Insert genesis row (height 0)
3. Fill any gaps from previous incomplete runs
4. Backfill from resume point to chain tip (chunked, concurrent)
5. Enter poll loop — checks for new blocks every `POLL_INTERVAL` seconds

**Reorg handling:** Before indexing a new block, the poll loop verifies parent hash continuity. On mismatch, it walks back to the fork point, deletes stale rows, and re-indexes.

**Graceful shutdown:** On SIGTERM/SIGINT, the indexer finishes the current database write and closes connections cleanly.

## Database snapshots

Produce a snapshot after backfill completes:

```bash
./scripts/snapshot.sh produce <height>
```

This stops containers, tars the PostgreSQL data volume, and outputs a `boxtime-pgdata-<height>.tar.gz` file ready to upload as a GitHub Release.

## Development

Requires [pixi](https://pixi.sh). To run locally (outside Docker):

```bash
pixi install
pixi run start
```

You'll need a running Ergo node and PostgreSQL instance. Set `NODE_URL` and `DATABASE_URL` in `.env` accordingly.

## License

See [LICENSE](LICENSE).