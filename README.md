# FX Synthetic Data Generator & Ingestor for QuestDB

This script generates highly realistic, multi-level FX order book and price tick data and ingests it into QuestDB at high speed.

Designed for **stress testing**, benchmarking, and live demo scenarios, it supports both wall-clock-paced (“real-time”) and maximum-throughput (“faster-than-life”) simulation. Multi-process orchestration, pip-accurate orderbooks, WAL backpressure detection, and robust state management ensure both realism and throughput—without data overlap or out-of-order chaos.

---

## Features

- **Realistic Multi-Level Orderbook Simulation:**
  For a configurable set of FX pairs, generates L2 snapshots (bids/asks/volumes at multiple levels) and price ticks at a tunable, randomized rate. All values are always valid multiples of the correct pip for each pair (e.g., 0.0001 for EURUSD, 0.01 for USDJPY).
- **True Tick Precision, No Floating Point Drift:**
  All prices, spreads, and ladders are always generated as valid pips—never random floats—at every step.
- **Parallel High-Throughput Ingestion:**
  Multiprocessing splits both the event plan and pre-evolved state for max CPU utilization and ingest bandwidth. Each process ingests a unique time partition, never overlapping.
- **WAL Lag Detection and Flow Control:**
  Dedicated monitor process queries QuestDB’s WAL progress; when lag exceeds a threshold, all workers pause and resume cleanly.
- **Resumable, Incremental Ingestion:**
  Supports loading state from the latest DB rows, for continuous/incremental ingestion without data overlap, or generating from scratch.
- **Start/End Timestamp Enforcement:**
  The generator will **never** overlap or backfill data into already-populated time ranges; it automatically advances the start time if needed and stops cleanly at the end.
- **Materialized Views Auto-Setup:**
  On startup, creates all necessary tables and downstream views (with suffix support) if not already present.

---

## Mechanics

### Table & View Creation

Tables (`market_data`, `core_price`, with optional suffix) and their required materialized views are created at startup.

### Initial State

- By default (incremental mode), loads last-known state from the DB (`core_price LATEST BY symbol`), advancing the simulation timestamp just after the latest row found.
- Otherwise, starts from deterministic pip-aligned midpoints for each pair.

### Event Plan

- In `"faster-than-life"` mode, the total number of `market_data` (L2) events is set by `--total_market_data_events` and distributed across simulated seconds, then evenly split among processes. Each process gets a unique time slice—no overlap.
- In `"real-time"` mode, the generator respects wall-clock time, writing only as time passes. If prior data exists in the DB, it waits for wall-clock to advance past the last ingested row, then starts.

### Backpressure Management

- The WAL monitor process checks for lag (`sequencerTxn - writerTxn` on the `market_data` table) and pauses/resumes ingestion globally via a multiprocessing event.
- The threshold is `3 * num_processes`, with resume only when lag is fully cleared (debounced).

### Exit Logic

- If the requested start timestamp is already covered by existing data (or the end is past), the script exits with a clear message and does **not** overwrite or overlap existing data.
- All subprocesses, including the WAL monitor, are cleanly terminated at the end.

---

## Arguments

| Argument                     | Type      | Description                                                                                                    |
|------------------------------|-----------|----------------------------------------------------------------------------------------------------------------|
| `--host`                     | str       | Host/IP of QuestDB instance. Default: `127.0.0.1`                                                              |
| `--pg_port`                  | str/int   | PostgreSQL port for QuestDB. Default: `8812`                                                                   |
| `--user`                     | str       | Database user for metadata. Default: `admin`                                                                   |
| `--password`                 | str       | Password for metadata. Default: `quest`                                                                        |
| `--token`                    | str       | (Optional) ILP/HTTP authentication token (JWK)                                                                 |
| `--token_x`                  | str       | (Optional) JWK token X (for tcps)                                                                              |
| `--token_y`                  | str       | (Optional) JWK token Y (for tcps)                                                                              |
| `--ilp_user`                 | str       | (Optional) ILP/HTTP ingestion user. Default: `admin`                                                           |
| `--protocol`                 | str       | `tcp` or `http` (`tcps`/`https` if token present). Default: `http`                                             |
| `--mode`                     | str       | `"real-time"` (wall clock pacing) or `"faster-than-life"` (max speed). **Required**                            |
| `--market_data_min_eps`      | int       | Min events/sec for `market_data` (per simulated second). Default: `1000`                                       |
| `--market_data_max_eps`      | int       | Max events/sec for `market_data`. Default: `15000`                                                             |
| `--core_min_eps`             | int       | Min events/sec for `core_price`. Default: `800`                                                                |
| `--core_max_eps`             | int       | Max events/sec for `core_price`. Default: `1100`                                                               |
| `--total_market_data_events` | int       | Total `market_data` (L2) events to generate (across all workers). Default: `1_000_000`                         |
| `--start_ts`                 | str       | (Optional) Simulation start time, ISO8601 format. Default: now (UTC)                                           |
| `--end_ts`                   | str       | (Optional) Simulation end time, ISO8601 format.                                                                |
| `--processes`                | int       | Number of worker processes. Default: `1`                                                                       |
| `--min_levels`               | int       | Min orderbook levels (bids/asks). Default: `5`                                                                 |
| `--max_levels`               | int       | Max orderbook levels. Default: `5`                                                                             |
| `--incremental`              | bool/flag | If true (default), load last state from DB to continue appending, never overlap existing data.                 |
| `--create_views`             | bool/flag | If true (default), create required materialized views if not present.                                          |
| `--suffix`                   | str       | (Optional) Suffix to append to all table/view names.                                                           |

---

## WAL Backpressure Control

- **Pause:** When `sequencerTxn - writerTxn > 3 * num_processes`, all workers pause and print a message.
- **Resume:** Only resumes after at least one interval with *zero* lag (`sequencerTxn == writerTxn`).

---

## Usage Examples

### High-speed batch ingest (“faster-than-life” mode)

Ingests ~900M rows in one day, using TCP/TCPS with authentication:

```bash
python fx_data_generator.py \
  --host 192.21.12.42 \
  --market_data_min_eps 8200 \
  --market_data_max_eps 11000 \
  --core_min_eps 700 \
  --core_max_eps 1000 \
  --token "secret_jwk_token" \
  --token_x "jwk_token_x_public_key" \
  --token_y "jwk_token_y_public_key" \
  --ilp_user ilp_ingest \
  --protocol tcp \
  --mode faster-than-life \
  --processes 8 \
  --total_market_data_events 900_000_000 \
  --start_ts "2025-07-05T00:00:00Z" \
  --end_ts "2025-07-06T00:00:00Z"
```

### Wall-clock pacing (“real-time” mode)

Ingests in real time, with up to 100M events. Starts now (ignores `--start_ts`):

```bash
python fx_data_generator.py \
  --host 192.21.12.42 \
  --market_data_min_eps 1000 \
  --market_data_max_eps 2000 \
  --core_min_eps 700 \
  --core_max_eps 1000 \
  --protocol http \
  --mode real-time \
  --processes 1 \
  --total_market_data_events 100_000_000
```

---

## Notes

- The script will **never** insert data that overlaps existing rows—if there is already data for the requested range, it advances the start timestamp or exits.
- All state, event counts, and random walks are pip-quantized, so *no invalid ticks or drift* are possible.
- All subprocesses are shut down cleanly, and WAL monitoring never lingers after completion.

---

**Questions?** See comments in the script or [open an issue](https://github.com/questdb/questdb).
