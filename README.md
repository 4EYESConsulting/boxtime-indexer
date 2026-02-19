# boxtime-indexer

Cointime Economics ETL/indexer for the [Ergo](https://ergoplatform.org) blockchain. Pre-computes coinblocks created (CBC), coinblocks destroyed (CBD), and coinblocks stored (CBS) for every block height and outputs the results to a CSV file.

Companion to the [boxtime](https://github.com/4EYESConsulting/boxtime) library.

## Requirements

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) — runs the indexer and (optionally) an Ergo node
- [pixi](https://pixi.sh) — Python dependency management (needed for `install`, `test`, and local development)
- [Task](https://taskfile.dev) *(optional)* — provides `Taskfile.yml` shortcuts (`task up`, `task test`, etc.)

## Quickstart

```bash
git clone https://github.com/4EYESConsulting/boxtime-indexer.git
cd boxtime-indexer
cp .env.example .env
```

A default `input/erg_prices.csv` is already included in this repository (see [Price Data](#price-data) below). Replace it with a freshly downloaded file when you need newer price coverage.

Run the indexer:

```bash
task up
```

The indexer will backfill all blocks up to the last date in the price CSV, then exit. The output is written to `output/cointime.csv`.

## Price Data

The indexer requires a CSV file with ERG/USD price data. A default `input/erg_prices.csv` is included in this repository; to refresh or extend price coverage, download an updated file from CoinGecko:

1. Go to [CoinGecko Ergo Price History](https://www.coingecko.com/en/coins/ergo/historical_data)
2. Select your date range (start: 2019-07-01, end: desired end date)
3. Click the **.csv** button to download
4. Save the file as `input/erg_prices.csv`

The CSV should have columns from CoinGecko's historical export such as:

- `snapped_at`
- `price`

Additional columns like `market_cap` and `total_volume` are ignored.

### Updating Price Data

To update with new price data:

1. Download a new CSV from CoinGecko with the extended date range
2. Replace `input/erg_prices.csv`
3. Optionally copy `output/cointime.csv` to `input/cointime.csv` to bootstrap from the previous run
4. Run the indexer again

## Folder Structure

```
.
├── input/
│   ├── erg_prices.csv    # Price data from CoinGecko (required)
│   └── cointime.csv      # Previous output for bootstrap (optional)
├── output/
│   └── cointime.csv      # Final merged output
├── src/
│   └── ...
└── ...
```

## Configuration

All settings are environment variables with sensible defaults. Edit `.env` to customize:

| Variable | Default | Description |
|---|---|---|
| `NODE_URL` | `http://node:9053` | Ergo node API URL |
| `PRICE_CSV_PATH` | `input/erg_prices.csv` | Path to price CSV |
| `BOOTSTRAP_CSV_PATH` | `input/cointime.csv` | Path to bootstrap CSV (optional) |
| `CSV_OUTPUT_PATH` | `output/cointime.csv` | Path for output CSV |
| `CHUNK_SIZE` | `5000` | Heights per batch during backfill |
| `MAX_CONCURRENT` | `50` | Maximum concurrent requests to the node |
| `START_HEIGHT` | `1` | Requested start height if no bootstrap data (network fetch is clamped to `>= 1`) |

When `START_HEIGHT=0`, output includes a synthetic height-0 genesis placeholder row (`cbc=0`, `cbd=0`, `cbs=0`). If bootstrap data already contains height 0, it is preserved without adding a duplicate row.

## Output CSV Format

```csv
blockheight,blockheight_timestamp,blockheight_date,coinblocks_created,coinblocks_destroyed,coinblocks_stored,price_date,price_close
0,1561978800000,2019-07-01,0,0,0,,
1,1561978800000,2019-07-01,0,0,0,2019-07-01,0.50
...
```

All cointime values are in **nanoERGs** (1 ERG = 1,000,000,000 nanoERG).

## Architecture

Docker services:

- **node** *(optional, `local-node` profile)* — Ergo full node (`ergoplatform/ergo`) with `extraIndex = true`. Only needed if you don't have your own node.
- **indexer** — Python async service that fetches data from the node and writes to CSV files.

The Ergo node (local or external) must be v6.0.1+ with `extraIndex = true` for the indexed block API.

Data flow:
1. Wait for Ergo node to sync
2. Load price data from CSV
3. Load bootstrap data if available (previous output)
4. Backfill from resume point until block date exceeds max price date
5. Merge with prices and write output CSV
6. Exit

### How it works

**Per-height fetch** (3 HTTP calls to the Ergo node):

1. `GET /emission/at/{h}` → `totalCoinsIssued` = CBC
2. `GET /blocks/at/{h}` → header ID
3. `GET /blockchain/block/byHeaderId/{id}` → indexed block with full transaction inputs → compute CBD + extract timestamp

CBD excludes the **emission contract box**, which carries the unissued supply and is consumed/recreated every block. Including it would massively inflate CBD. The emission box is identified by its constant ergoTree; when emissions eventually run out, the filter simply matches nothing.

CBS = CBC − CBD.

## Development

Requires [pixi](https://pixi.sh) for install/test workflows and (optionally) [Task](https://taskfile.dev). A `Taskfile.yml` provides shortcuts for common operations:

| Task | Description |
|---|---|
| `task up` | Start with local node (default) |
| `task up:remote-node` | Start with remote node |
| `task down` | Stop all containers |
| `task clean` | Remove all containers, images, and volumes |
| `task install` | Install Python dependencies with pixi |
| `task test` | Run the test suite |
| `task status` | Check indexer sync status (shows chain height, progress %, ETA)

## License

See [LICENSE](LICENSE).
