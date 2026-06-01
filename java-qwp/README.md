# qwp-fx-trades

A synthetic **FX trade** generator that ingests into QuestDB over **QWP** (the
WebSocket binary protocol of the QuestDB Java client), with **HA failover** across
a fleet of hosts. It runs a single worker by default and can fan out across
**N worker threads** (`--processes`, 1–30) using a by-symbol split.

It is the simplified, trades-only sibling of the Python FX data generator
(`../fx_data_generator.py`). Differences by design:

- **One table only:** `qwp_trades`, with a schema **identical** to the Python
  `fx_trades` table (same columns, types, PARQUET encodings, `PARTITION BY HOUR`,
  and `DEDUP UPSERT KEYS(timestamp, trade_id)`).
- **No** order book, `core_price`, or `market_data` tables.
- **No** materialized views.
- **Single worker by default**; `--processes N` adds worker threads (one QWP
  `Sender` each), splitting the symbols by a both-ends popularity draft.
- **Transactional ingestion** (always on): frames stream to the server deferred,
  and each worker commits one WAL transaction on a fixed cadence
  (`--commit_interval_ms`, default 1s), so commit size is decoupled from WebSocket
  frame size — many byte-bounded frames commit as one transaction, keeping the
  WAL sequencer/writer gap small.

Price realism is kept the same way as the Python generator: each pair's mid
follows a controlled pip random walk inside a Yahoo-anchored bracket, and trades
within a second are interpolated between the open and evolved close states so the
series is continuous across second boundaries (`close(t) == open(t+1)`).

## Table schema

```sql
CREATE TABLE IF NOT EXISTS qwp_trades (
    timestamp    TIMESTAMP_NS PARQUET(delta_binary_packed, zstd(4)),
    symbol       SYMBOL CAPACITY 15000 PARQUET(rle_dictionary, zstd(4), bloom_filter),
    ecn          SYMBOL PARQUET(rle_dictionary, zstd(4), bloom_filter),
    trade_id     UUID PARQUET(default, zstd(4)),
    side         SYMBOL PARQUET(rle_dictionary, zstd(4), bloom_filter),
    passive      BOOLEAN PARQUET(default, zstd(4)),
    price        DOUBLE PARQUET(default, zstd(4)),
    quantity     DOUBLE PARQUET(default, zstd(4)),
    counterparty SYMBOL PARQUET(rle_dictionary, zstd(4)),
    order_id     UUID PARQUET(default, zstd(4))
) TIMESTAMP(timestamp) PARTITION BY HOUR DEDUP UPSERT KEYS(timestamp, trade_id);
```

The generator creates this table itself (over QWP) if it does not exist.
Retention is attached only with `--short_ttl` (`TTL 1 MONTH`, or
`STORAGE POLICY(...)` when combined with `--enterprise`), matching the Python
generator's `fx_trades` retention behaviour.

## Prerequisites

- **Java 17+** and **Maven 3+**.
- A running **QuestDB** that speaks **QWP over WebSocket** (a build
  protocol-compatible with client `1.3.2`). QWP listens on the same port as the
  HTTP endpoint (default `9000`).
- The **`org.questdb:questdb-client:1.3.2`** artifact in your local Maven cache.
  Build and install it from the client repo:

  ```bash
  cd /Users/j/prj/questdb/java-questdb-client
  mvn clean install -DskipTests
  ```

## Build

```bash
cd /Users/j/prj/python/fx/java-qwp
mvn -q clean package
```

## Run

Helper scripts (set `HOSTS` to your fleet; defaults to `127.0.0.1:9000`):

```bash
# Real-time: wall-clock aligned, periodic Yahoo refresh, runs until Ctrl+C
HOSTS=127.0.0.1:9000 ./run_realtime.sh

# Faster-than-life backfill / stress test: no waiting, bounded by TOTAL
HOSTS=127.0.0.1:9000 TOTAL=1000000 ./run_backfill.sh
```

Or invoke directly with `mvn exec:java` (see the full parameter list below):

```bash
mvn -q exec:java -Dexec.args="--mode real-time \
    --hosts 172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000 \
    --tls_insecure --token <TOKEN> --total_market_data_events 0"
```

Show all options:

```bash
mvn -q exec:java -Dexec.args="--help"
```

## Examples

All examples target an HA fleet (internal VPC IPs). `<TOKEN>` is a placeholder —
substitute your QWP ingestion token; don't commit a real one. `--tls_insecure`
uses `wss` and **skips TLS certificate validation** (self-signed test clusters);
on a cluster with valid certs use `--tls` instead.

### 1-minute throughput test

Faster-than-life, single worker, bounded to 60 wall-clock seconds. Prints rows/sec
every second, then a min/median/avg/max summary and total elapsed (generate/send
time only, excluding Yahoo/DDL). Writes to a throwaway `qwp_trades_xxx`:

```bash
mvn -q exec:java -Dexec.args="--mode faster-than-life \
    --hosts 172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000 \
    --tls_insecure --token <TOKEN> \
    --processes 1 \
    --total_market_data_events 0 --run_secs 60 \
    --orders_min_per_sec 1000 --orders_max_per_sec 1000 \
    --short_ttl true --enterprise true \
    --suffix _xxx"
```

### Backfill (faster-than-life, bounded by row count)

Fill history forward from a start timestamp as fast as possible, stopping at a
fixed row count:

```bash
mvn -q exec:java -Dexec.args="--mode faster-than-life \
    --hosts 172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000 \
    --tls_insecure --token <TOKEN> \
    --processes 1 \
    --total_market_data_events 20000000 \
    --start_ts 2026-05-22T00:00:00.000000Z \
    --orders_min_per_sec 1000 --orders_max_per_sec 1000 \
    --short_ttl true --enterprise true"
```

### Real-time (continuous stream)

Wall-clock paced (one data-second per real second), periodic Yahoo refresh, runs
until Ctrl+C:

```bash
mvn -q exec:java -Dexec.args="--mode real-time \
    --hosts 172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000 \
    --tls_insecure --token <TOKEN> \
    --processes 1 \
    --total_market_data_events 0 \
    --orders_min_per_sec 2000 --orders_max_per_sec 5000 \
    --short_ttl true --enterprise true"
```

Verify what landed (HTTP query endpoint, against any node — use `qwp_trades_xxx`
for the throughput example):

```bash
curl -s "http://172.31.42.41:9000/exec?query=SELECT%20count()%20rows,%20count_distinct(trade_id)%20ids,%20min(timestamp)%20first_ts,%20max(timestamp)%20last_ts%20FROM%20qwp_trades"
```

## HA / connection model

All hosts share **one** credential set and **one** transport scheme:

- `--hosts h1:9000,h2:9000,h3:9000` — the failover fleet (`--host` for a single host).
- `--tls` — use `wss` for every host. `--tls_insecure` also disables certificate
  validation (self-signed clusters).
- `--token <t>` **or** `--user <u> --password <p>` — applied to all hosts.
- Store-and-forward is **on** (`--sf_dir`, default `/tmp/qwp_trades_sf`, with one
  `wN` subdir per worker) so a failover mid-batch does not drop unacknowledged
  trades; the sender reconnects with backoff and replays from the spill directory.
- **WAL backpressure:** a monitor polls `wal_tables()` every 5s and pauses all
  workers when the table's `sequencerTxn - writerTxn` lag exceeds the threshold —
  `3 × processes` for more than 2 workers, `5 × processes` for 2 or fewer —
  resuming once the writer has caught up. These stay small because transactional
  commits keep only a few large transactions in flight at a time.

Connection and batch-error events are logged (connection events throttled to
once/second per worker), so you can see which node took over during a failover.

## Parameters

Option names accept **either** the Python underscore form (`--start_ts`,
`--total_market_data_events`) **or** kebab form (`--start-ts`, `--total-trades`).

### General / connection

| Flag | Default | Purpose |
| --- | --- | --- |
| `--mode` | _(required)_ | `real-time` or `faster-than-life` |
| `--hosts` (`--host`) | `127.0.0.1:9000` | comma-separated HA fleet |
| `--tls` / `--tls_insecure` | off | `wss` for all hosts (insecure = skip cert check) |
| `--token` | none | QWP/bearer token (enterprise), shared across hosts |
| `--user` + `--password` | none | OR HTTP basic auth, shared across hosts |
| `--sf_dir <dir>` | `/tmp/qwp_trades_sf` | store-and-forward spill directory |
| `--sender_id <id>` | `qwp-fx-trades` | store-and-forward sender id |
| `--auto_flush_bytes <n>` | 524288 | QWP auto-flush size in bytes (keep under the ~1MB WS frame) |

### Trades / volume / time

| Flag | Default | Purpose |
| --- | --- | --- |
| `--orders_min_per_sec` / `--orders_max_per_sec` | 50 / 200 | throughput, events/sec |
| `--total_market_data_events` | 1000000 | max events; `0` = unlimited (real-time) |
| `--start_ts <iso>` | after last row / now | start of data clock (faster-than-life) |
| `--end_ts <iso>` | none | max timestamp / upper bound |
| `--run_secs <n>` | 0 (no cap) | stop after n wall-clock seconds (fixed-duration throughput test) |
| `--commit_interval_ms <n>` | 1000 | transaction rate: how often each worker commits (flush) |

**Total elapsed** generate/send time is **always** reported at the end
(`[DONE] emitted N in X.XXs = Y trades/sec`; it excludes Yahoo and DDL startup).
**Per-second** throughput (`[rate] t=Ns  … rows/sec`, plus a min/median/avg/max
summary) is reported **only when `--run_secs` is set** — that's the fixed-duration
throughput-test path.

### Reference data / schema

| Flag | Default | Purpose |
| --- | --- | --- |
| `--yahoo_refresh_secs <n>` | 300 | real-time Yahoo refresh interval |
| `--no_yahoo` | off | skip Yahoo, use template brackets (offline) |
| `--incremental [true\|false]` | false | seed prices from last stored row, skip Yahoo |
| `--short_ttl [true\|false]` | off | attach retention to the table |
| `--enterprise [true\|false]` | off | with `--short_ttl`, use STORAGE POLICY instead of TTL |
| `--suffix <s>` | none | table becomes `qwp_trades<s>` |
| `--lei_pool_size <n>` | 2000 | distinct counterparties |

### Parallelism

| Flag | Default | Purpose |
| --- | --- | --- |
| `--processes <n>` | 1 | worker threads, 1–30; symbols split by a both-ends popularity draft |

Each worker owns a disjoint, weight-balanced set of symbols end-to-end (own price
state, own QWP sender), preserving per-symbol continuity; the global
`--total_market_data_events` cap is shared via an atomic counter.

**Note:** for a single table on a single QuestDB node, **one worker is fastest** —
the bottleneck is the server's single-writer WAL apply, so extra workers add
contention (and trigger backpressure pauses) without raising throughput.
Multi-worker pays off only across multiple tables or nodes.

### Accepted but currently unused

`--chunk_seconds` is accepted for Python-CLI parity but has no effect (state is
streamed per-second, so there is no upfront precompute to chunk); it prints a
`[note]`.

ILP/PG-transport flags (`--protocol`, `--pg_port`, `--ilp_user`, `--token_x`,
`--token_y`) and the order-book / `market_data` / `core_price` / materialized-view
flags (`--market_data_*_eps`, `--core_*_eps`, `--min_levels`, `--max_levels`,
`--create_views`) are **not supported** — this is a trades-only QWP generator, so
passing them is an error.

## Notes

- **Timestamp safety:** the generator reads `max(timestamp)` from `qwp_trades` and
  continues strictly after it, so reruns extend the series rather than overlapping
  it. `--start_ts` overrides this in faster-than-life mode. On an empty table it
  starts at "now".
- **Modes:** real-time advances the data clock one second per wall-clock second
  (and refreshes Yahoo periodically); faster-than-life runs as fast as possible
  with no wall-clock wait, bounded by `--total_market_data_events` and/or `--end_ts`.
- **Protocol compatibility:** QWP is a development wire protocol. The running
  QuestDB server must be protocol-compatible with client `1.3.2`; a mismatch
  surfaces as a version/role exception at connect time.
- Java uses its own truststore, so there is no macOS certifi workaround to apply
  (unlike the Python pipeline).
