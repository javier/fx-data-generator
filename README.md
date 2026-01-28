# FX Synthetic Data Generator & Ingestor for QuestDB

This script generates highly realistic, multi-level FX market data and ingests it into QuestDB at high speed. It produces three interconnected datasets:

1. **`market_data`** - Full depth orderbook snapshots (L2 market data)
2. **`core_price`** - Best bid/offer (BBO) with indicators and metadata
3. **`fx_trades`** - Executed trades with realistic order matching against orderbooks

Designed for **stress testing**, **benchmarking**, and **live demo scenarios**, it supports both wall-clock-paced ("real-time") and maximum-throughput ("faster-than-life") simulation with multi-process orchestration, pip-accurate pricing, WAL backpressure detection, and robust state management.

---

## Features

### Data Generation

- **Three-Table Architecture:**
  - **`market_data`**: Multi-level orderbook snapshots (bids/asks at 3-50 levels) at 1,200-15,000 events/sec
  - **`core_price`**: Best bid/offer snapshots with ECN, reason codes, and technical indicators at 700-1,100 events/sec
  - **`fx_trades`**: Realistic trade executions (5-30 orders/sec) with partial fills, level walking, and counterparty LEIs

- **Realistic Order Execution:**
  - Orders execute against actual orderbook liquidity
  - Supports passive (limit) and aggressive (market) orders
  - Orders can walk multiple levels until filled or limit price reached
  - Each order generates 1-N trades with consistent `order_id`
  - ECN consistency: all trades from same order execute on same ECN

- **30 FX Pairs with Liquidity Ranking:**
  - Major pairs (EURUSD, USDJPY, GBPUSD) have higher trade frequency
  - Exotic pairs trade less frequently based on realistic liquidity distribution
  - Pip-accurate pricing for each pair (0.0001 for majors, 0.01 for JPY crosses)

- **Deterministic Counterparty Pool:**
  - Generates realistic 20-character LEIs (Legal Entity Identifiers)
  - Hash-based generation ensures expandability (first N LEIs always identical)
  - Configurable pool size (default: 2000)

- **Live Market Data Integration (Real-time Mode):**
  - Yahoo Finance updates price brackets every 5 minutes
  - Keeps simulation aligned with current market conditions
  - Prevents unrealistic price drift

- **Temporal Consistency:**
  - Intra-second price interpolation for smooth OHLC bars
  - `close(second N)` always equals `open(second N+1)`
  - Trades timestamped after their source orderbook snapshot (ASOF join compatible)
  - All three tables flush synchronously for dashboard consistency

### Performance & Reliability

- **Parallel High-Throughput Ingestion:**
  - Multiprocessing splits event generation across workers
  - Each process handles unique time partitions (no overlap)
  - Typical: 8-11K events/sec sustained on 6-8 cores

- **WAL Lag Detection and Flow Control:**
  - Dedicated monitor process queries QuestDB's WAL progress
  - Workers pause when lag exceeds threshold, resume when cleared
  - Prevents QuestDB memory pressure during fast ingestion

- **Resumable, Incremental Ingestion:**
  - Automatically detects latest timestamp in database
  - Advances start time to avoid overlap
  - Supports continuous/incremental data generation

- **Materialized Views Auto-Setup:**
  - Creates OHLC views at multiple time granularities (1s, 1m, 15m, 1d)
  - Separate views for `core_price` and `market_data` aggregations
  - Optional TTLs for demo environments

---

## Tables & Schema

### `market_data`
Full orderbook depth with array storage for efficient ingestion:
```sql
timestamp TIMESTAMP
symbol SYMBOL
bids DOUBLE[][]  -- bids[0] = prices, bids[1] = volumes
asks DOUBLE[][]  -- asks[0] = prices, asks[1] = volumes
```
**Volume:** 1,200-15,000 events/second

### `core_price`
Best bid/offer snapshots with metadata:
```sql
timestamp TIMESTAMP
symbol SYMBOL
ecn SYMBOL  -- LMAX, EBS, Hotspot, Currenex
bid_price DOUBLE
bid_volume LONG
ask_price DOUBLE
ask_volume LONG
reason SYMBOL  -- normal, news_event, liquidity_event
indicator1 DOUBLE
indicator2 DOUBLE
```
**Volume:** 700-1,100 events/second

### `fx_trades`
Executed trades with nanosecond precision:
```sql
timestamp TIMESTAMP_NS  -- Nanosecond precision
symbol SYMBOL
ecn SYMBOL
trade_id UUID
side SYMBOL  -- buy, sell
passive BOOLEAN
price DOUBLE
quantity DOUBLE
counterparty SYMBOL  -- 20-char LEI
order_id UUID  -- Multiple trades can share same order_id (partial fills)
```
**Volume:** 5-30 orders/second → typically 5-50 trades/second (depending on partial fills)

---

## Arguments

| Argument                     | Type      | Default       | Description                                                                                    |
|------------------------------|-----------|---------------|------------------------------------------------------------------------------------------------|
| `--host`                     | str       | `127.0.0.1`   | Host/IP of QuestDB instance                                                                    |
| `--pg_port`                  | str/int   | `8812`        | PostgreSQL port for QuestDB metadata queries                                                   |
| `--user`                     | str       | `admin`       | Database user for metadata                                                                     |
| `--password`                 | str       | `quest`       | Password for metadata                                                                          |
| `--token`                    | str       | None          | ILP authentication token (JWK) for HTTP/HTTPS                                                  |
| `--token_x`                  | str       | None          | JWK token X coordinate (for tcps)                                                              |
| `--token_y`                  | str       | None          | JWK token Y coordinate (for tcps)                                                              |
| `--ilp_user`                 | str       | `admin`       | ILP/HTTP ingestion user                                                                        |
| `--protocol`                 | str       | `http`        | `tcp` or `http` (`tcps`/`https` if token present)                                             |
| `--mode`                     | str       | **Required**  | `real-time` (wall clock) or `faster-than-life` (max speed)                                     |
| `--market_data_min_eps`      | int       | `1200`        | Min `market_data` events/sec (must be > `core_max_eps`)                                        |
| `--market_data_max_eps`      | int       | `15000`       | Max `market_data` events/sec                                                                   |
| `--core_min_eps`             | int       | `700`         | Min `core_price` events/sec (must be > `orders_max_per_sec`)                                   |
| `--core_max_eps`             | int       | `1000`        | Max `core_price` events/sec (must be < `market_data_min_eps`)                                  |
| `--orders_min_per_sec`       | int       | `5`           | Min orders/sec (each order → 1-N trades, must be < `core_min_eps`)                             |
| `--orders_max_per_sec`       | int       | `30`          | Max orders/sec                                                                                 |
| `--lei_pool_size`            | int       | `2000`        | Number of unique counterparty LEIs to generate                                                 |
| `--total_market_data_events` | int       | `1_000_000`   | Total `market_data` events to generate (faster-than-life mode only)                            |
| `--start_ts`                 | str       | now (UTC)     | Simulation start time (ISO8601). Not allowed in real-time mode                                 |
| `--end_ts`                   | str       | None          | Simulation end time (ISO8601)                                                                  |
| `--chunk_seconds`            | int       | `900`         | Max seconds to precompute per chunk in faster-than-life mode (limits memory usage)             |
| `--processes`                | int       | `1`           | Number of worker processes (real-time allows only 1)                                           |
| `--min_levels`               | int       | `40`          | Min orderbook levels                                                                           |
| `--max_levels`               | int       | `40`          | Max orderbook levels                                                                           |
| `--incremental`              | bool      | `false`       | Load last state from DB to continue appending (faster-than-life only)                          |
| `--create_views`             | bool      | `true`        | Create materialized views if not present                                                       |
| `--short_ttl`                | bool      | `false`       | Enforce TTLs on all tables (3 days for tables, 4 hours-1 month for views)                      |
| `--suffix`                   | str       | `""`          | Suffix to append to all table/view names                                                       |
| `--yahoo_refresh_secs`       | int       | `300`         | Yahoo Finance bracket refresh interval (real-time mode only)                                   |

### Event Rate Validation

The script enforces a hierarchy to maintain realism:
- `market_data_min_eps` > `core_max_eps` (orderbooks update faster than BBO snapshots)
- `core_min_eps` > `orders_max_per_sec` (more price updates than trades)

Violating these constraints will cause the script to exit with an error message.

---

## Ingestion Modes

### Real-Time Mode (`--mode real-time`)

- Generates data at wall-clock pace (1 simulated second = 1 real second)
- Timestamps are always 2 seconds ahead of wall clock for dashboard responsiveness
- Yahoo Finance refreshes price brackets every 5 minutes (configurable)
- Single process only (`--processes 1`)
- Runs indefinitely until stopped or `--total_market_data_events` reached
- Ignores `--start_ts` (always starts from now)

**Use cases:** Live demos, continuous data streams, dashboard testing

### Faster-Than-Life Mode (`--mode faster-than-life`)

- Generates data as fast as possible (no wall-clock pacing)
- Processes in chunks (default 15 min) to limit memory usage for long runs
- Maintains OHLC continuity across chunk boundaries
- Supports multiprocessing for parallel ingestion
- Requires `--total_market_data_events` to be set
- Can use `--start_ts` and `--end_ts` for specific time ranges
- Yahoo Finance queried once at startup (or bypassed if `--incremental`)

**Use cases:** Backfills, stress testing, benchmarking, generating historical datasets

---

## Usage Examples

### Real-Time Streaming (Live Demo)

```bash
python fx_data_generator.py \
  --host 172.31.42.41 \
  --market_data_min_eps 1200 \
  --market_data_max_eps 2500 \
  --core_min_eps 700 \
  --core_max_eps 1000 \
  --orders_min_per_sec 5 \
  --orders_max_per_sec 30 \
  --lei_pool_size 2000 \
  --token "your_token_here" \
  --token_x "your_token_x_here" \
  --token_y "your_token_y_here" \
  --ilp_user ilp_ingest \
  --protocol tcp \
  --mode real-time \
  --processes 1 \
  --total_market_data_events 800_000_000
```

### High-Speed Batch Backfill (14 hours of data)

```bash
python fx_data_generator.py \
  --host 172.31.42.41 \
  --market_data_min_eps 8200 \
  --market_data_max_eps 11000 \
  --core_min_eps 700 \
  --core_max_eps 1000 \
  --orders_min_per_sec 5 \
  --orders_max_per_sec 30 \
  --lei_pool_size 2000 \
  --token "your_token_here" \
  --token_x "your_token_x_here" \
  --token_y "your_token_y_here" \
  --ilp_user ilp_ingest \
  --protocol tcp \
  --mode faster-than-life \
  --processes 6 \
  --total_market_data_events 600_000_000 \
  --start_ts "2025-11-11T00:00:00.000000Z" \
  --end_ts "2025-11-11T14:00:00.000000Z" \
  --min_levels 3 \
  --max_levels 3 \
  --incremental false
```

### Local Development (No Authentication)

```bash
python fx_data_generator.py \
  --host 127.0.0.1 \
  --market_data_min_eps 110 \
  --market_data_max_eps 150 \
  --core_min_eps 70 \
  --core_max_eps 100 \
  --orders_min_per_sec 5 \
  --orders_max_per_sec 30 \
  --protocol tcp \
  --mode real-time \
  --processes 1 \
  --total_market_data_events 100_000_000 \
  --lei_pool_size 2000
```

---

## Docker Support

Build and run via Docker:

```bash
docker build -t fx-generator .

docker run -e HOST=172.31.42.41 \
           -e TOKEN="your_token" \
           -e TOKEN_X="your_token_x" \
           -e TOKEN_Y="your_token_y" \
           -e ILP_USER=ilp_ingest \
           -e MARKET_DATA_MIN_EPS=1200 \
           -e MARKET_DATA_MAX_EPS=2500 \
           -e CORE_MIN_EPS=700 \
           -e CORE_MAX_EPS=1000 \
           -e ORDERS_MIN_PER_SEC=5 \
           -e ORDERS_MAX_PER_SEC=30 \
           -e LEI_POOL_SIZE=2000 \
           fx-generator
```

All parameters can be overridden via environment variables or passed as arguments to the entrypoint script.

---

## Materialized Views

The script automatically creates OHLC aggregation views:

### Core Price Views
- `core_price_1s`: 1-second OHLC (mid-price, spread, bid/ask stats)
- `core_price_1d`: 1-day OHLC

### Market Data Views
- `market_data_ohlc_1m`: 1-minute OHLC from orderbook best bid
- `market_data_ohlc_15m`: 15-minute OHLC
- `market_data_ohlc_1d`: 1-day OHLC

### Trade Views (User-Created)
Example views for `fx_trades` aggregations:

```sql
CREATE MATERIALIZED VIEW fx_trades_ohlc_1m AS (
    SELECT timestamp, symbol,
        first(price) AS open,
        max(price) AS high,
        min(price) AS low,
        last(price) AS close,
        SUM(quantity) AS total_volume
    FROM fx_trades
    SAMPLE BY 1m
) PARTITION BY HOUR TTL 2 DAYS;

CREATE MATERIALIZED VIEW fx_trades_ohlc_1h AS (
    SELECT timestamp, symbol,
        first(price) AS open,
        max(price) AS high,
        min(price) AS low,
        last(price) AS close,
        SUM(quantity) AS total_volume
    FROM fx_trades
    SAMPLE BY 1h
) PARTITION BY DAY TTL 1 MONTH;
```

---

## Data Characteristics

### Realism Features

1. **Price Continuity:**
   - `close(second N)` = `open(second N+1)` for every symbol
   - Intra-second interpolation produces realistic candlesticks (not flat lines)
   - Controlled random walk with shock events (±20 pips occasionally)

2. **Spread Dynamics:**
   - Spreads evolve realistically (1-8 pips for majors)
   - Rare stress-widening events (0.05% probability)
   - Spread measured in pips to avoid floating-point artifacts

3. **Volume Distribution:**
   - Log-scaled volume ladder: thinner at best levels, deeper at worse levels
   - Best bid/ask: 50K-100K typical
   - Deep levels: 500M-1B liquidity

4. **Trade Execution:**
   - 40% passive orders (limit), 60% aggressive (market)
   - Passive: 0-2 pips slippage, aggressive: 3-10 pips slippage
   - Orders walk multiple levels if size exceeds liquidity
   - Log-normal trade size distribution (many small, few large)

5. **ECN Distribution:**
   - 4 ECNs: LMAX, EBS, Hotspot, Currenex
   - Random but consistent per order (all trades from order at same ECN)

6. **Liquidity Ranking:**
   - Top 9 pairs (EURUSD, USDJPY, GBPUSD, etc.) trade 10x more than exotics
   - Rank-weighted selection for realistic market share

### Timestamp Precision

- **market_data**: Microsecond (TIMESTAMP)
- **core_price**: Microsecond (TIMESTAMP)
- **fx_trades**: Nanosecond (TIMESTAMP_NS)

Trade timestamps are always ≥1 microsecond after their source `core_price` event, ensuring ASOF joins work correctly despite precision differences.

---

## WAL Backpressure Control

- **Pause Threshold:** `sequencerTxn - writerTxn > 3 * num_processes`
- **Resume:** Only when lag clears completely (`sequencerTxn == writerTxn`)
- Workers print status messages when paused/resumed
- Prevents QuestDB OOM during extreme ingestion rates

---

## Safety & Data Integrity

- **No Overlapping Data:** Script queries latest timestamp before starting and advances past it
- **No Time Travel:** If requested start is already covered, script exits cleanly
- **Atomic Flushes:** All three tables flush synchronously for dashboard consistency
- **Clean Shutdown:** All subprocesses (workers, WAL monitor, Yahoo refresher) terminate cleanly

---

## Troubleshooting

### Trades Not Appearing

If `fx_trades` table is empty or has very few rows:
- Check `--orders_min_per_sec` and `--orders_max_per_sec` are set (default: 5-30)
- Verify event rate hierarchy: `core_min_eps` > `orders_max_per_sec`
- In real-time mode with low event rates, trades may take several seconds to accumulate and flush

### Yahoo Finance Errors (Real-time Mode)

If Yahoo Finance fails to fetch prices:
- Script falls back to hardcoded price brackets (non-fatal)
- Check network connectivity to finance.yahoo.com
- Increase `--yahoo_refresh_secs` to reduce query frequency

### WAL Lag Warnings

If workers frequently pause due to WAL lag:
- Reduce `--processes` count
- Lower event rates (`--market_data_max_eps`, etc.)
- Increase QuestDB's `cairo.max.uncommitted.rows` setting
- Enable more CPU cores for QuestDB writer threads

