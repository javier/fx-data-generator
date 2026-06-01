# qwp-fx

A synthetic **FX market-data generator** that ingests into QuestDB over **QWP**
(the WebSocket binary protocol of the QuestDB Java client), HA-aware across a fleet
of hosts. It can populate two tables, each with its own pool of worker threads:

- **`qwp_trades`** — trade prints (schema identical to the Python `fx_trades`).
- **`qwp_market_data`** — full order-book snapshots (identical to the Python
  `market_data`: `bids`/`asks` as `DOUBLE[][]`, plus `best_bid`/`best_ask`).

It is a simplified sibling of the Python FX data generator
(`../fx_data_generator.py`). Differences by design:

- **Two tables, no others** — no `core_price`, no materialized views.
- **Independent per-table pools:** `--trades_processes` workers feed `qwp_trades`,
  `--market_data_processes` workers feed `qwp_market_data`. Each pool snake-drafts
  the symbols across its own threads (both-ends popularity draft), and each worker
  has its own QWP sender. You can give each table its own degree of parallelism
  (e.g. 1 for trades, 2 for market_data). Separate tables ⇒ separate WAL writers.
- **Transactional ingestion** (always on): frames stream deferred and each worker
  commits one WAL transaction on a fixed cadence (`--commit_interval_ms`, default
  1s), so commit size is decoupled from the WebSocket frame size — keeping the WAL
  sequencer/writer gap small.
- **Cross-table price consistency:** each symbol's mid/spread walk is
  **deterministic** (seeded by the symbol), so a trades worker and a market_data
  worker that both own a symbol compute the identical top-of-book for the same
  (symbol, second) — with no shared state. Trades then **execute against the
  reconstructed book** (walk levels), so every trade prints at a real book-level
  price consistent with the published snapshot. Order size is log-normal; the
  order-book volume ladder is log-scaled (~100k…1B), like the Python generator.
- **Order → fills:** `--orders_*_per_sec` sets *orders*/sec; each order executes
  against the book and yields one or more trade rows (Python semantics), so the
  trade row count is `orders × fills`.

## Table schemas

```sql
CREATE TABLE IF NOT EXISTS qwp_trades (
    timestamp    TIMESTAMP_NS, symbol SYMBOL, ecn SYMBOL, trade_id UUID,
    side SYMBOL, passive BOOLEAN, price DOUBLE, quantity DOUBLE,
    counterparty SYMBOL, order_id UUID
) TIMESTAMP(timestamp) PARTITION BY HOUR DEDUP UPSERT KEYS(timestamp, trade_id);

CREATE TABLE IF NOT EXISTS qwp_market_data (
    timestamp TIMESTAMP, symbol SYMBOL,
    bids DOUBLE[][], asks DOUBLE[][],   -- shape [2][levels]: row1 = prices, row2 = volumes
    best_bid DOUBLE, best_ask DOUBLE
) TIMESTAMP(timestamp) PARTITION BY HOUR;
```

(Real DDL also carries the Python PARQUET column encodings.) The generator creates
whichever tables it needs over QWP. Retention is attached only with `--short_ttl`
(`TTL 1 MONTH`/`3 DAYS`, or `STORAGE POLICY(...)` with `--enterprise`). `--suffix`
applies to both names (`qwp_trades<s>`, `qwp_market_data<s>`).

## Prerequisites

- **Java 17+** and **Maven 3+**.
- A running **QuestDB** that speaks **QWP over WebSocket**, protocol-compatible
  with client `1.3.2`. QWP shares the HTTP port (default `9000`).
- The **`org.questdb:questdb-client:1.3.2`** dependency — resolved automatically
  from **Maven Central**, no local build needed.

## Build

From the repo root:

```bash
cd java-qwp
mvn -q clean package
```

Invoke directly with `mvn compile exec:java` (the `compile` goal is required —
`exec:java` alone does not build, so a fresh checkout would hit
`ClassNotFoundException`). Show all options:

```bash
mvn -q -f ./pom.xml compile exec:java -Dexec.args="--help"
```

## Examples

All examples target an HA fleet (internal VPC IPs) and read the token from a file
via `--token_file $HOME/qwp_token.txt` — put your token there first
(`echo '<token>' > ~/qwp_token.txt && chmod 600 ~/qwp_token.txt`) so it stays off
the command line / shell history. Use `$HOME` (not `~`) inside the quoted args.
`--tls_insecure` is `wss` + skipped cert validation (self-signed clusters); use
`--tls` where certs are valid. Keep each command on one logical line (trailing `\`
for line continuations).

### Uncapped throughput, both tables, 1 minute

Run flat-out for 60 wall-clock seconds — `--run_secs` is a wall-clock stopwatch, so
this measures max throughput (it does **not** bound the data-time range). Low
`--max_levels` keeps `market_data` rows small enough to sustain. Prints rows/sec
each second (trades + market_data) and a summary:

```bash
mvn -q -f ./pom.xml compile exec:java -Dexec.args="--mode faster-than-life \
    --hosts 172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000 \
    --tls_insecure --token_file $HOME/qwp_token.txt \
    --trades_processes 1 --market_data_processes 2 \
    --orders_min_per_sec 1000 --orders_max_per_sec 1000 \
    --market_data_min_eps 5000 --market_data_max_eps 5000 \
    --min_levels 1 --max_levels 2 \
    --total_market_data_events 0 --run_secs 60 \
    --short_ttl true --enterprise true \
    --suffix _xxx"
```

### A day of data (the data-generation pattern)

Bound by the **data-time window** (`--end_ts`) for the volume you actually want,
with `--total_market_data_events` as a safety cap slightly above the expected total
— whichever limit trips first ends the run:

```bash
mvn -q -f ./pom.xml compile exec:java -Dexec.args="--mode faster-than-life \
    --hosts 172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000 \
    --tls_insecure --token_file $HOME/qwp_token.txt \
    --trades_processes 1 --market_data_processes 2 \
    --orders_min_per_sec 30 --orders_max_per_sec 30 \
    --market_data_min_eps 1200 --market_data_max_eps 1200 \
    --min_levels 40 --max_levels 40 \
    --start_ts 2026-05-22T00:00:00.000000Z --end_ts 2026-05-23T00:00:00.000000Z \
    --total_market_data_events 120000000 \
    --short_ttl true --enterprise true"
```

### Real-time (continuous stream until Ctrl+C)

```bash
mvn -q -f ./pom.xml compile exec:java -Dexec.args="--mode real-time \
    --hosts 172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000 \
    --tls_insecure --token_file $HOME/qwp_token.txt \
    --trades_processes 1 --market_data_processes 2 \
    --orders_min_per_sec 30 --orders_max_per_sec 30 \
    --market_data_min_eps 1200 --market_data_max_eps 1200 \
    --total_market_data_events 0"
```

Verify (HTTP query endpoint, any node — add the `--suffix` for the throughput run):

```bash
curl -s "http://172.31.42.41:9000/exec?query=SELECT%20count()%20FROM%20qwp_trades"
curl -s "http://172.31.42.41:9000/exec?query=SELECT%20count()%20FROM%20qwp_market_data"
```

## HA / connection model

All hosts share **one** credential set and **one** transport scheme:

- `--hosts h1:9000,h2:9000,h3:9000` — the failover fleet (`--host` for one host).
- `--tls` (`wss`) / `--tls_insecure` (also skips cert validation).
- `--token <t>` (or `--token_file <path>` to keep it off the CLI) **or**
  `--user <u> --password <p>` — applied to all hosts.
- Store-and-forward is **on** (`--sf_dir`, default `/tmp/qwp_trades_sf`, with one
  subdir per worker, e.g. `t0`, `md0`, `md1`) so a mid-batch failover does not drop
  unacknowledged rows; the sender reconnects with backoff and replays the spill.
- **WAL backpressure:** a monitor polls `wal_tables()` every 5s for each enabled
  table and pauses all workers when its `sequencerTxn - writerTxn` lag exceeds the
  threshold — `3 × processes` above 2 workers, `5 × processes` at or below — then
  resumes when caught up. Small thresholds stay sane because transactional commits
  keep only a few large transactions in flight.

## Parameters

Option names accept Python underscore form (`--start_ts`) or kebab form
(`--start-ts`).

### Pools (one thread set per table)

| Flag | Default | Purpose |
| --- | --- | --- |
| `--trades_processes <n>` | 1 | worker threads for `qwp_trades`, 0–30 (0 = off) |
| `--market_data_processes <n>` | 0 | worker threads for `qwp_market_data`, 0–30 (0 = off) |

### Volume / time (each rate is the **table-wide total** across that pool)

| Flag | Default | Purpose |
| --- | --- | --- |
| `--orders_min/max_per_sec` | 50 / 200 | `qwp_trades` orders/sec; each order → 1+ fills |
| `--market_data_min/max_eps` | 1200 / 15000 | `qwp_market_data` snapshots/sec |
| `--min_levels` / `--max_levels` | 40 / 40 | order-book depth per snapshot |
| `--total_market_data_events <n>` | 1000000 | max total rows across tables; `0` = unlimited |
| `--start_ts` / `--end_ts <iso>` | after last row / none | data-time window (bound the volume) |
| `--run_secs <n>` | 0 | **wall-clock** stop (throughput tests); not a data window |
| `--commit_interval_ms <n>` | 1000 | transaction rate (commit cadence) |

Whichever stop condition (`--end_ts`, `--total_market_data_events`, `--run_secs`)
is reached **first** ends the run. Total elapsed time is always reported; per-second
rows/sec (with a summary) is reported only when `--run_secs` is set.

### Reference data / schema

| Flag | Default | Purpose |
| --- | --- | --- |
| `--yahoo_refresh_secs <n>` | 300 | real-time Yahoo refresh interval |
| `--no_yahoo` | off | skip Yahoo, use template brackets (offline) |
| `--incremental [true\|false]` | false | seed mids from last stored trade, skip Yahoo |
| `--short_ttl` / `--enterprise [true\|false]` | off | retention (TTL, or STORAGE POLICY with enterprise) |
| `--suffix <s>` | none | tables become `qwp_trades<s>` / `qwp_market_data<s>` |
| `--lei_pool_size <n>` | 2000 | distinct counterparties |

### Accepted but unused / unsupported

`--processes` is **removed** — use `--trades_processes` / `--market_data_processes`.
`--chunk_seconds` is accepted for parity but has no effect. ILP/PG-transport flags
(`--protocol`, `--pg_port`, `--ilp_user`, `--token_x`, `--token_y`) and
`core_price` / materialized-view flags (`--core_*_eps`, `--create_views`) are not
supported and error if passed.

## Notes

- **Faster-than-life respects the per-second rate.** Volume = data-time span ×
  rate. Bound by `--end_ts` (e.g. one day at 1200 eps → exactly that many rows,
  ~1200/sec) for a known dataset; `--run_secs` instead runs flat-out for N *wall*
  seconds and covers as much simulated time as it can (use it for throughput, not
  to size a dataset). Best practice: set both `--end_ts` and a `--total_market_data_events`
  safety cap.
- **Throughput ceiling.** With apply headroom (e.g. a cluster) the limiter is
  client generation, so per-table workers scale; the eventual wall is the storage
  out-of-order (O3) apply, which grows with multi-worker data-clock divergence —
  keep an eye on the WAL lag. Separate tables/pools and modest per-table worker
  counts keep O3 low.
- **Protocol compatibility:** QWP is a development wire protocol; the server must
  be protocol-compatible with client `1.3.2`.
- Java uses its own truststore — no macOS certifi workaround needed.
```
