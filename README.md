# boxtime-indexer

Cointime Economics ETL/indexer for the [Ergo](https://ergoplatform.org) blockchain. Pre-computes coinblocks created (CBC), coinblocks destroyed (CBD), and coinblocks stored (CBS) for every block height and stores the results in PostgreSQL.

Companion to the [boxtime](https://github.com/4EYESConsulting/boxtime) library.

## Quickstart

```bash
git clone https://github.com/4EYESConsulting/boxtime-indexer.git
cd boxtime-indexer
cp .env.example .env
```

**Fully local** (local DB + bundled Ergo node — syncs the chain locally, takes time on first run):

```bash
task up
```

**With your own Ergo node** (local DB + remote node, must be v6.0.1+ with `extraIndex = true`):

```bash
# Edit .env and set NODE_URL to your node
task up:remote-node
```

**With a remote database** (e.g. Supabase — see [Remote Database](#remote-database) below):

```bash
# Edit .env and set DATABASE_URL to your remote Postgres
task up:remote        # remote DB + remote node
task up:remote-db     # remote DB + local node
```

Once the node is synced, the indexer automatically backfills all historical blocks and then polls for new ones.

## Restoring from a snapshot

To skip the full backfill (~1.7M+ blocks), download a pre-built database snapshot:

```bash
# Download the latest snapshot from GitHub Releases
# Then restore it:
task snapshot:restore TARBALL=boxtime-pgdata-<height>.tar.gz
task up
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
| `COINGECKO_API_KEY` | *(none)* | CoinGecko API key for daily ERG/USD prices (optional) |
| `COINGECKO_PRO` | `false` | Set to `true` for paid CoinGecko plans |

## Architecture

Docker services:

- **node** *(optional, `local-node` profile)* — Ergo full node (`ergoplatform/ergo`) with `extraIndex = true`. Only needed if you don't have your own node.
- **db** *(optional, `local-db` profile)* — PostgreSQL 17, stores one row per block height. Not needed when using a remote database.
- **indexer** — Python async service that fetches data from the node and writes to the database.

The Ergo node (local or external) must be v6.0.1+ with `extraIndex = true` for the indexed block API.

### Remote Database

The indexer can write to any PostgreSQL database — local or remote. To use a hosted database (e.g. Supabase, Neon, or any managed Postgres):

1. Set `DATABASE_URL` in `.env` to your remote connection string.
2. Use the **connection pooler** endpoint when available (e.g. Supabase port `6543` in transaction mode) — this handles many concurrent clients efficiently.
3. SSL is enabled automatically for remote hosts.

Example with Supabase:

```bash
# .env
DATABASE_URL=postgresql://postgres.[ref]:[password]@aws-0-[region].pooler.supabase.com:6543/postgres
NODE_URL=https://your-ergo-node:9053
```

```bash
task up:remote
```

### Database schema

```sql
CREATE TABLE cointime (
    height    INTEGER PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    cbc       NUMERIC NOT NULL,
    cbd       NUMERIC NOT NULL,
    cbs       NUMERIC NOT NULL
);

CREATE TABLE erg_prices (
    date      DATE PRIMARY KEY,
    price_usd DOUBLE PRECISION NOT NULL
);
```

All cointime values are in **nanoERGs** (1 ERG = 1,000,000,000 nanoERG). The `erg_prices` table stores one row per day (daily close from CoinGecko) and is only populated when a CoinGecko API key is configured.

### How it works

**Per-height fetch** (3 HTTP calls to the Ergo node):

1. `GET /emission/at/{h}` → `totalCoinsIssued` = CBC
2. `GET /blocks/at/{h}` → header ID
3. `GET /blockchain/block/byHeaderId/{id}` → indexed block with full transaction inputs → compute CBD + extract timestamp

CBD excludes the **emission contract box**, which carries the unissued supply and is consumed/recreated every block. Including it would massively inflate CBD. The emission box is identified by its constant ergoTree; when emissions eventually run out, the filter simply matches nothing.

CBS = CBC − CBD.

**Startup sequence:**

1. Wait for the Ergo node to be synced (checks `fullHeight` vs `indexedHeight`)
2. Insert genesis row (height 0)
3. Fill any gaps from previous incomplete runs
4. Backfill from resume point to chain tip (chunked, concurrent)
5. Backfill daily ERG/USD prices from CoinGecko (if API key is configured)
6. Enter poll loop — checks for new blocks and syncs latest prices every `POLL_INTERVAL` seconds

**Reorg handling:** Before indexing a new block, the poll loop verifies parent hash continuity. On mismatch, it walks back to the fork point, deletes stale rows, and re-indexes.

**Graceful shutdown:** On SIGTERM/SIGINT, the indexer finishes the current database write and closes connections cleanly.

## Database snapshots

Produce a snapshot after backfill completes:

```bash
task snapshot:produce HEIGHT=1700000
```

Restore a previously produced snapshot:

```bash
task snapshot:restore TARBALL=boxtime-pgdata-1700000.tar.gz
task up
```

This stops containers, tars the PostgreSQL data volume, and outputs a `boxtime-pgdata-<height>.tar.gz` file ready to upload as a GitHub Release.

## Development

Requires [pixi](https://pixi.sh) and (optionally) [Task](https://taskfile.dev). A `Taskfile.yml` provides shortcuts for common operations:

| Task | Description |
|---|---|
| `task up` | Start with local DB and local node (default) |
| `task up:remote-node` | Start with local DB and remote node |
| `task up:remote-db` | Start with remote DB and local node |
| `task up:remote` | Start with remote DB and remote node |
| `task down` | Stop all containers |
| `task clean` | Remove all containers, images, and volumes |
| `task status` | Print indexing status from the database |
| `task install` | Install Python dependencies with pixi |
| `task test` | Run the test suite |
| `task snapshot:produce HEIGHT=<h>` | Produce a DB snapshot at the given height |
| `task snapshot:restore TARBALL=<file>` | Restore a DB snapshot from a tarball |

## License

See [LICENSE](LICENSE).