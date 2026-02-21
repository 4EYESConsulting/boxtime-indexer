# boxtime-indexer

Cointime Economics ETL/indexer for the [Ergo](https://ergoplatform.org) blockchain. Pre-computes coinblocks created (CBC), coinblocks destroyed (CBD), and coinblocks stored (CBS) for every block height and outputs the results to CSV files.

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

The indexer will backfill all blocks up to the last date in the price CSV, then exit. The output is written to `output/cointime.csv` and `output/prices.csv`.

## Price Data

**Price data MUST be historical.** The indexer requires price data that predates or matches the blocks being indexed. This ensures every block timestamp has a corresponding price record for accurate cointime economics calculations.

The indexer requires a CSV file with ERG/USD price data. A default `input/erg_prices.csv` is included in this repository; to refresh or extend price coverage, download an updated file from CoinGecko:

1. Go to [CoinGecko Ergo Price History](https://www.coingecko.com/en/coins/ergo/historical_data)
2. Select your date range (start: 2019-07-01, end: desired end date)
3. Click the **.csv** button to download
4. Save the file as `input/erg_prices.csv`

The CSV should have columns from CoinGecko's historical export such as:

- `snapped_at`
- `price`

Additional columns like `market_cap` and `total_volume` are ignored.

### End Height Determination

The indexer determines the end height dynamically based on price data availability:

1. **Latest price date**: Reads the maximum date from the price CSV (`PRICE_CSV_PATH`)
2. **Binary search**: Uses binary search against the Ergo node to find the first block height where `block_date == max_price_date`
3. **Target height**: This height becomes the indexing target

This approach ensures:
- Accurate progress % throughout the sync
- No blocks are indexed beyond available price data
- Clean stop at the price data boundary

Example:
```
Price CSV contains data up to: 2024-01-15
Chain height: 1,200,000

Binary search finds: block 987,654 is first block on 2024-01-15
Target height: 987,654

Indexer syncs from START_HEIGHT → 987,654
Progress shows: "45% complete" (accurate against real target)
```

## Folder Structure

```
.
├── input/
│   └── erg_prices.csv    # Price data from CoinGecko (required)
├── output/
│   ├── cointime.csv      # Cointime data (height, timestamp, CBC, CBD, CBS)
│   └── prices.csv        # Deduplicated daily prices
├── src/
│   └── ...
└── ...
```

## Configuration

All settings are environment variables with sensible defaults. Edit `.env` to customize:

| Variable | Default | Description |
|---|---|---|
| `NODE_URL` | `http://node:9053` | Ergo node API URL |
| `PRICE_CSV_PATH` | `input/erg_prices.csv` | Path to input price CSV |
| `COINTIME_OUTPUT_PATH` | `output/cointime.csv` | Path for cointime output CSV |
| `PRICES_OUTPUT_PATH` | `output/prices.csv` | Path for prices output CSV |
| `CHUNK_SIZE` | `5000` | Heights per batch during backfill |
| `MAX_CONCURRENT` | `50` | Maximum concurrent requests to the node |
| `START_HEIGHT` | `1` | Requested start height (must be >= 1) |

## Output CSV Formats

### cointime.csv

```csv
blockheight,blockheight_timestamp,coinblocks_created,coinblocks_destroyed,coinblocks_stored
1,1561978800000,0,0,0
2,1561978800000,67500000000000000,0,67500000000000000
...
```

All cointime values are in **nanoERGs** (1 ERG = 1,000,000,000 nanoERG).

### prices.csv

```csv
price_date,price_timestamp,price_close
2019-07-01,1561939200000,0.50
2019-07-02,1562025600000,0.52
...
```

Prices are deduplicated (one row per unique date) and sorted chronologically. The `price_timestamp` is UTC midnight for each date.

## Architecture

Docker services:

- **node** *(optional, `local-node` profile)* — Ergo full node (`ergoplatform/ergo`) with `extraIndex = true`. Only needed if you don't have your own node.
- **indexer** — Python async service that fetches data from the node and writes to CSV files.

The Ergo node (local or external) must be v6.0.1+ with `extraIndex = true` for the indexed block API.

Data flow:
1. Wait for Ergo node to sync
2. Load price data from CSV to determine target date
3. Check existing cointime.csv for duplicates and auto-deduplicate if corrupted
4. Backfill from START_HEIGHT (or resume from existing output) until target date
5. Write cointime data and deduplicated prices to separate CSV files
6. Exit

### Crash Recovery

The indexer automatically handles corrupted or partially written cointime.csv files on startup:
- Detects duplicate heights in existing output
- Removes duplicates (keeps last occurrence)
- Rewrites the file before resuming indexing

This allows the indexer to recover gracefully from crashes or interruptions without manual intervention.

### How it works

**Per-height fetch** (3 HTTP calls to the Ergo node):

1. `GET /emission/at/{h}` → `totalCoinsIssued` = CBC
2. `GET /blocks/at/{h}` → header ID
3. `GET /blockchain/block/byHeaderId/{id}` → indexed block with full transaction inputs → compute CBD + extract timestamp

CBD excludes the **emission contract box**, which carries the unissued supply and is consumed/recreated every block. Including it would massively inflate CBD. The emission box is identified by its constant ergoTree; when emissions eventually run out, the filter simply matches nothing.

CBS = CBC − CBD.

## Development

Requires [pixi](https://pixi.sh) for install/test workflows and status checks. (Optionally) [Task](https://taskfile.dev) is used for task automation. Install pixi first:

```bash
curl -fsSL https://pixi.sh/install.sh | bash
# or download from https://pixi.sh
```

A `Taskfile.yml` provides shortcuts for common operations:

| Task | Description |
|---|---|
| `task up` | Start with local node (default) |
| `task up:remote-node` | Start with remote node |
| `task down` | Stop all containers |
| `task clean` | Remove all containers, images, and volumes |
| `task install` | Install Python dependencies with pixi |
| `task test` | Run the test suite |
| `task status` | Check indexer sync status (shows chain height, progress) |

## License

See [LICENSE](LICENSE).
