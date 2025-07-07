# FX Synthetic Data Generator & Ingestor for QuestDB

This script generates highly realistic, multi-level FX order book and price tick data and ingests it into QuestDB at high speed.
It is specifically designed for *stress testing*, benchmarking, and demo scenarios, and contains multi-process orchestration
and ingestion to get high throughput. Since multi process can lead to out-of-order ingestion in QuestDB, and that might
slow down the instance, there are safety controls to avoid transaction lag between sequencer and writer.

## Features

- **Realistic Multi-Level Orderbook Simulation:**
  For a configurable set of FX pairs, generates synthetic L2 snapshots and price ticks (best bid/ask + ladders + indicators) at a tunable rate.
- **Multi-Process High-Throughput Ingestion:**
  Uses Python multiprocessing, splitting the per-second plan *and* the event counts among worker processes, to maximize throughput and exploit multi-core CPUs.
- **WAL Lag Monitoring & Flow Control:**
  Monitors QuestDB's WAL transaction lag in a separate process, pausing ingestion across all workers when lag exceeds a configurable threshold, and only resuming once the WAL has fully caught up.
- **Resilient, Debounced Ingestion Resume:**
  Avoids "flapping" between pause/resume by requiring the WAL to be fully caught up for at least one full interval before resuming ingestion, reducing the risk of thrashing under heavy write load.

## Mechanics & Architecture

### 1. **Table/View Setup**

On startup, the script creates the required tables (`market_data`, `core_price`) and all downstream materialized views, if not already present.

### 2. **Initial State Bootstrapping**

- Can load initial state from the latest DB values (`core_price LATEST BY symbol`) or start from deterministic defaults.
- This state is then evolved forward (tick-by-tick, second-by-second) for as many simulated seconds as needed to cover the event plan.

### 3. **Event Generation Plan**

- The total number of `market_data` (L2) events is specified by `--total_market_data_events`.
- For each simulated second, a random (but bounded) number of market and core price events are generated, using `--market_data_min_eps`, `--market_data_max_eps`, etc.
- The script **precomputes** this per-second event plan, then splits it evenly among all worker processes, so each process gets a unique slice of the event plan and their total event counts add up to the requested global total.

### 4. **Multiprocessing & Event Coordination**

- Each worker process receives its per-second slice, along with the precomputed state for each second.
- Each worker repeatedly:
  1. Checks if a global pause event is set (see below); if so, waits.
  2. Generates and ingests its assigned events for the current second.
  3. Flushes and proceeds to the next simulated second.

### 5. **WAL Transaction Lag Monitoring & Pausing**

- A dedicated `wal_monitor` process queries QuestDB every few seconds:
  - `SELECT sequencerTxn, writerTxn FROM wal_tables() WHERE name = 'market_data'`
- If `sequencerTxn - writerTxn > (3 x num_processes)` (the threshold), the `pause_event` is set.
- **All worker processes respect this event**: when set, they pause ingestion (with a message) and loop waiting for resume.
- **Resume is debounced**: workers only resume when `sequencerTxn == writerTxn` for at least one monitor interval.
- This prevents the database from being overwhelmed by excessive WAL lag or IO stalls.

### 6. **Other Safeguards**

- Cleanly terminates the WAL monitor process when all workers complete.
- Optionally supports incremental mode, view (re-)creation, and full parameterization.

## Arguments

| Argument                     | Type      | Description                                                                                                              |
|------------------------------|-----------|--------------------------------------------------------------------------------------------------------------------------|
| `--host`                     | str       | Hostname or IP address of the QuestDB instance. Default: `127.0.0.1`                                                     |
| `--pg_port`                  | str/int   | PostgreSQL port for QuestDB. Default: `8812`                                                                             |
| `--user`                     | str       | Database user for metadata connection. Default: `admin`                                                                  |
| `--password`                 | str       | Database password for metadata connection. Default: `quest`                                                              |
| `--token`                    | str       | (Optional) ILP/HTTP authentication token (JWK)                                                                           |
| `--token_x`                  | str       | (Optional) JWK token X (public key) for tcps ingestion                                                                  |
| `--token_y`                  | str       | (Optional) JWK token Y (public key) for tcps ingestion                                                                  |
| `--ilp_user`                 | str       | (Optional) ILP/HTTP ingestion user. Default: `admin`                                                                     |
| `--protocol`                 | str       | `tcp` or `http` (or `tcps`/`https` if token present). Controls ingestion method. Default: `http`                         |
| `--mode`                     | str       | Either `"real-time"` (1 second pacing) or `"faster-than-life"` (max speed). **Required**                                 |
| `--market_data_min_eps`      | int       | Minimum events/sec for `market_data` (per simulated second). Default: `1000`                                             |
| `--market_data_max_eps`      | int       | Maximum events/sec for `market_data` (per simulated second). Default: `15000`                                            |
| `--core_min_eps`             | int       | Minimum events/sec for `core_price` (per simulated second). Default: `800`                                               |
| `--core_max_eps`             | int       | Maximum events/sec for `core_price` (per simulated second). Default: `1100`                                              |
| `--total_market_data_events` | int       | Total number of `market_data` (L2) events to generate (global across all workers). Default: `1_000_000`                  |
| `--start_ts`                 | str       | (Optional) Simulation start time in ISO8601 format. Default: now (UTC)                                                   |
| `--end_ts`                   | str       | (Optional) Simulation end time in ISO8601 format. Optional.                                                              |
| `--processes`                | int       | Number of worker processes to use. Default: `4`                                                                          |
| `--min_levels`               | int       | Minimum number of orderbook levels (for both bids/asks). Default: `5`                                                    |
| `--max_levels`               | int       | Maximum number of orderbook levels. Default: `5`                                                                         |
| `--incremental`              | flag      | If set, load last known prices from QuestDB as starting state (for incremental/continuous ingestion).                    |
| `--create_views`             | bool      | Create required materialized views if not present. Default: `True`                                                       |


## WAL Backpressure Logic

- **Threshold:**
  Pausing triggers when WAL sequencerTxn is > `3 x num_processes` ahead of writerTxn.
- **Pause:**
  Workers enter a paused state and print messages until lag is fully gone.
- **Resume:**
  Only resumes after at least one interval with *no lag* (`sequencerTxn == writerTxn`).

## How To Run

Scenario 1: This would generate date in `faster-than-life` mode, for 24 hours of data, as per the `start_ts` and `end_ts`
parameters. It will ingest about 850 million rows in the `market_data` table, as per the eps rate passed as parameters.

Example in with tcp ingestion and authentication (will use tcps)

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
  --start_ts "2025-07-05T00:00:00" \
  --end_ts "2025-07-06T00:00:00"
```


Example with http ingestion and authentication (will use https)

```bash
python fx_data_generator.py \
  --host 192.21.12.42 \
  --market_data_min_eps 8200 \
  --market_data_max_eps 11000 \
  --core_min_eps 700 \
  --core_max_eps 1000 \
  --token "secret_jwk_token" \
  --protocol http \
  --mode faster-than-life \
  --processes 8 \
  --total_market_data_events 900_000_000 \
  --start_ts "2025-07-05T00:00:00" \
  --end_ts "2025-07-06T00:00:00"
```

Scenario 2: This would generate date in `real-time` mode, up to 100_000_000 events in the `market_data` table, starting
from the `start_ts` date. If no `start_ts` is provided, the current timestamp in UTC will be used as starting point. Data
is sent via http and with no authentication.


```bash
python fx_data_generator.py \
  --host 192.21.12.42 \
  --market_data_min_eps 8200 \
  --market_data_max_eps 11000 \
  --core_min_eps 700 \
  --core_max_eps 1000 \
  --protocol http \
  --mode real-time \
  --processes 8 \
  --total_market_data_events 100_000_000 \
  --start_ts "2025-07-05T00:00:00"
```

