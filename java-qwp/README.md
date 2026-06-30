# qwp-fx

A synthetic **FX market-data generator** that ingests into QuestDB over **QWP**
(the WebSocket binary protocol of the QuestDB Java client), HA-aware across a fleet
of hosts. It can populate three tables, each with its own pool of worker threads
(names shown with the default `--prefix qwp_`):

- **`qwp_fx_trades`** — trade prints (schema identical to the Python `fx_trades`).
- **`qwp_market_data`** — full order-book snapshots (identical to the Python
  `market_data`: `bids`/`asks` as `DOUBLE[][]`, plus `best_bid`/`best_ask`).
- **`qwp_core_price`** — top-of-book snapshots with metadata (identical to the
  Python `core_price`: best `bid_price`/`ask_price` + volumes, `ecn`, `reason`,
  `indicator1`/`indicator2`).

It is a sibling of the Python FX data generator (`../fx_data_generator.py`) and is
**dataset-compatible** with it: set `--prefix ""` and the table/view names, schemas,
and the full materialized-view set match the Python generator exactly, so you can
backfill with one and continue with the other on the same tables (see
[Interchangeability with the Python generator](#interchangeability-with-the-python-generator)).
Design points:

- **Three tables, full Python-parity materialized views** — `<prefix>fx_trades`,
  `<prefix>market_data`, `<prefix>core_price`; with `--create_views` (default
  **true**) it also builds the same 11 matviews the Python generator creates (BBO
  ladder, market-data OHLC, core-price OHLC-mid, and trade OHLC — see below).
- **Independent per-table pools:** `--trades_processes` workers feed `qwp_trades`,
  `--market_data_processes` workers feed `qwp_market_data`, `--core_processes`
  workers feed `qwp_core_price`. Each pool snake-drafts the symbols across its own
  threads (both-ends popularity draft), and each worker has its own QWP sender. You
  can give each table its own degree of parallelism (e.g. 1 for trades, 3 for
  market_data, 1 for core_price). Separate tables ⇒ separate WAL writers.
- **Transactional ingestion** (always on): frames stream deferred and each worker
  commits one WAL transaction on a fixed cadence (`--commit_interval_ms`, default
  1s), so commit size is decoupled from the WebSocket frame size — keeping the WAL
  sequencer/writer gap small.
- **Cross-table price consistency:** each symbol's mid/spread walk is
  **deterministic** (seeded by the symbol), so trades, market_data and core_price
  workers that own a symbol compute the identical top-of-book for the same
  (symbol, second) — with no shared state. `qwp_core_price`'s `bid_price`/`ask_price`
  therefore match `qwp_market_data`'s `best_bid`/`best_ask` exactly, and trades
  **execute against the reconstructed book** (walk levels), so every trade prints at
  a real book-level price consistent with the published snapshot. Order size is
  log-normal; the volume ladder is log-scaled (~100k…1B), like the Python generator.
  (core_price's `indicator1`/`indicator2` use a separate per-symbol RNG so they never
  perturb the shared price walk.)
- **Order → fills:** `--orders_*_per_sec` sets *orders*/sec; each order executes
  against the book and yields one or more trade rows (Python semantics), so the
  trade row count is `orders × fills`.

## Table schemas

```sql
CREATE TABLE IF NOT EXISTS qwp_fx_trades (
    timestamp    TIMESTAMP_NS, symbol SYMBOL, ecn SYMBOL, trade_id UUID,
    side SYMBOL, passive BOOLEAN, price DOUBLE, quantity DOUBLE,
    counterparty SYMBOL, order_id UUID
) TIMESTAMP(timestamp) PARTITION BY HOUR DEDUP UPSERT KEYS(timestamp, trade_id);

CREATE TABLE IF NOT EXISTS qwp_market_data (
    timestamp TIMESTAMP, symbol SYMBOL,
    bids DOUBLE[][], asks DOUBLE[][],   -- shape [2][levels]: row1 = prices, row2 = volumes
    best_bid DOUBLE, best_ask DOUBLE
) TIMESTAMP(timestamp) PARTITION BY HOUR;

CREATE TABLE IF NOT EXISTS qwp_core_price (
    timestamp TIMESTAMP, symbol SYMBOL, ecn SYMBOL,
    bid_price DOUBLE, bid_volume LONG, ask_price DOUBLE, ask_volume LONG,
    reason SYMBOL,                      -- normal | news_event | liquidity_event
    indicator1 DOUBLE, indicator2 DOUBLE
) TIMESTAMP(timestamp) PARTITION BY HOUR;
```

(Real DDL also carries the Python PARQUET column encodings.) The generator creates
whichever tables its enabled pools need over QWP. Retention is attached only with
`--short_ttl` (`TTL 1 MONTH`/`3 DAYS`, or `STORAGE POLICY(...)` with `--enterprise`
on the base tables — matviews always take TTL). Table/view names are
`<prefix><stem><suffix>`: `--prefix` (default `qwp_`, set `""` to match the Python
names) and `--suffix` (default empty) both concatenate **verbatim**, so the trades
table is `<prefix>fx_trades<suffix>`, e.g. `qwp_fx_trades` by default or `fx_trades`
with `--prefix ""`.

### Materialized views (`--create_views`, default on)

With `--create_views true` (the default, matching Python) the generator creates the
**same 11 matviews as the Python generator** — prefix/suffix threaded through every
name, and each view gated on its base pool being enabled (a disabled pool's views are
skipped, not errored). The cascades and per-view TTL/partitions are byte-faithful to
the Python DDL:

| view (`<prefix>…<suffix>`) | base | refresh | content |
| --- | --- | --- | --- |
| `core_price_1s` | `core_price` | immediate | 1s OHLC-mid, spread, bid/ask stats |
| `core_price_1d` | `core_price` | EVERY 1h (deferred) | 1d OHLC-mid, spread, bid/ask stats |
| `bbo_1s` | `market_data` | immediate | 1s last bid / last ask |
| `bbo_1m` | `bbo_1s` | EVERY 1m (deferred) | 1m max bid / min ask |
| `bbo_1h` | `bbo_1m` | EVERY 10m (deferred) | 1h max bid / min ask |
| `bbo_1d` | `bbo_1h` | EVERY 1h (deferred) | 1d max bid / min ask |
| `market_data_ohlc_1m` | `market_data` | immediate | 1m OHLC of `best_bid` + Σ best-bid volume |
| `market_data_ohlc_15m` | the 1m view | immediate | 15m OHLC rolled up from the 1m view |
| `market_data_ohlc_1d` | `market_data` | EVERY 1h (deferred) | 1d OHLC of `best_bid` + Σ volume |
| `fx_trades_ohlc_1m` | `fx_trades` | immediate | 1m OHLC of traded `price` + Σ quantity |
| `fx_trades_ohlc_1d` | the 1m view | EVERY 1h (deferred) | 1d OHLC rolled up from the 1m view |

All use `CREATE ... IF NOT EXISTS` (idempotent — to redefine one, drop it first; the
**first** generator to run fixes the definition, so keep both engines on the same
build for byte-identical DDL). Retention follows Python: matviews always take **TTL**
(not STORAGE POLICY, which QuestDB doesn't yet support on views) when `--short_ttl`
is set, with per-view TTLs matching the Python generator. The views are owned by the
connecting (ingest) user — no `OWNED BY` clause — so creation never requires
admin/superuser privileges.

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
each second (`trades` / `md` / `core`) and a summary:

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

#### Pinning a target rate (real-time)

In real-time, one data-second is one wall-second, so the per-second rates *are*
the rows/sec: `--market_data_*_eps` is market_data rows/sec and `--orders_*_per_sec`
is the order rate. Set the min and max equal for a flat rate. This example pins
**~1,000,000 market_data rows/sec** with trades at the Python default proportion
(`orders : market_data` ≈ `30 : 15000` = 1:500, so ~2,000 orders/sec):

```bash
mvn -q -f ./pom.xml compile exec:java -Dexec.args="--mode real-time \
    --hosts 172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000 \
    --tls_insecure --token_file $HOME/qwp_token.txt \
    --trades_processes 1 --market_data_processes 2 \
    --orders_min_per_sec 2000 --orders_max_per_sec 2000 \
    --market_data_min_eps 1000000 --market_data_max_eps 1000000 \
    --min_levels 1 --max_levels 2 \
    --total_market_data_events 0 \
    --short_ttl true --enterprise true \
    --suffix _xxx"
```

#### All three tables, just over 1M rows/sec total (real-time)

Splits ~1.015M rows/sec across the tables — **750K market_data + 240K core_price
+ ~25K trades**. market_data and core_price (top-of-book) are rate-controlled
directly by their eps; trades is `orders × fills`, and each order fills ~3.5 book
levels, so ~7,150 orders/sec ≈ 25K trade rows/sec. Bump `--orders_*_per_sec` live to
raise the trade share:

```bash
mvn -q -f ./pom.xml compile exec:java -Dexec.args="--mode real-time \
    --hosts 172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000 \
    --tls_insecure --token_file $HOME/qwp_token.txt \
    --trades_processes 1 --market_data_processes 3 --core_processes 1 \
    --orders_min_per_sec 7150 --orders_max_per_sec 7150 \
    --market_data_min_eps 750000 --market_data_max_eps 750000 \
    --core_min_eps 240000 --core_max_eps 240000 \
    --min_levels 10 --max_levels 10 \
    --total_market_data_events 0 \
    --short_ttl true --enterprise true \
    --suffix _xxx"
```

Verify (HTTP query endpoint, any node — add the `--suffix` for the throughput run):

```bash
curl -s "http://172.31.42.41:9000/exec?query=SELECT%20count()%20FROM%20qwp_fx_trades"
curl -s "http://172.31.42.41:9000/exec?query=SELECT%20count()%20FROM%20qwp_market_data"
curl -s "http://172.31.42.41:9000/exec?query=SELECT%20count()%20FROM%20qwp_core_price"
```

## HA / connection model

All hosts share **one** credential set and **one** transport scheme:

- `--hosts h1:9000,h2:9000,h3:9000` — the failover fleet (`--host` for one host).
- `--tls` (`wss`) / `--tls_insecure` (also skips cert validation).
- `--token <t>` (or `--token_file <path>` to keep it off the CLI) **or**
  `--user <u> --password <p>` — applied to all hosts.
- Store-and-forward is **on** (`--sf_dir`, default `/tmp/qwp_trades_sf`, with one
  subdir per worker, e.g. `t0`, `md0`, `md1`, `cp0`) so an outage does not drop
  unacknowledged rows: rows spill to local disk and the sender reconnects with
  backoff (up to ~5 min) and replays the spill on recovery. Because real-time stamps
  each row at generation time, replayed rows keep their original per-second
  timestamps, so a brief primary outage leaves **no gaps** when you query by second.
  Spill files are purged automatically once the data is acknowledged (only tiny
  `.lock` stubs remain). Size the S&F volume for your worst outage: at ~1M rows/sec a
  30s stop buffers a few GB.
- **WAL backpressure:** a monitor polls `wal_tables()` for each enabled table and
  pauses **only that table's pool** when its `sequencerTxn - writerTxn` lag exceeds
  the high-water threshold — `3 × processes` above 2 workers, `5 × processes` at or
  below — then resumes once it drains back to **half** the threshold (hysteresis, so
  the pool rides the apply ceiling instead of stalling to 0/s). Polling idles at 5s
  and tightens to 250ms only while a pool is draining.

## Timestamp safety & backfill

Before generating, the tool reads `max(timestamp)` across the enabled tables (suffix
aware) and refuses to ingest behind existing data — out-of-order inserts behind newer
rows are painful (O3 partition rewrites). Behaviour matches the Python generator:

- **Faster-than-life:** the start is **advanced** past the latest existing row
  (`[INFO] Advancing start … to avoid overlap`). If `--end_ts` is at or before the
  latest row, the run **aborts** with a clear error (the whole window is behind
  existing data). If everything requested is already present, it exits cleanly.
- **Real-time:** if the newest row is in the future (e.g. from a prior run's
  look-ahead), it **waits** (`[INFO] … Waiting Ns to avoid overlap`) until wall-clock
  passes it, then starts at "now".

So to **backfill** into a table that already has newer data, the older window is
skipped — backfill into a fresh `--suffix`, or extend forward from where the data
ends. Real-time rows are stamped `--realtime_lookahead_secs` (default 2s) ahead of
wall-clock so the live dashboard stays ahead of WAL apply lag.

Intra-second events interpolate each symbol's best bid/ask between its open and close
state, with the **first and last event pinned exactly to open/close**, so
`close(second t) == open(second t+1)` holds in the emitted rows — OHLC candles join
cleanly with no false gaps.

## Interchangeability with the Python generator

This generator and the Python `fx_data_generator.py` are **dataset-compatible**: you
can backfill with one and continue (or go live) with the other on the **same tables**,
and downstream views stay consistent. To share tables, run this generator with
`--prefix ""` so the names match the Python generator exactly (`fx_trades`,
`market_data`, `core_price`, and the 11 views). The Python generator is authoritative;
schemas, view DDL, the symbol universe, ECN/reason/counterparty pools, the volume
ladder, and the price/spread/indicator walks all mirror it.

For a genuinely seamless dataset, also align:

- **Event density (most important).** Per-second row counts come from the EPS / orders
  flags, **not** from the existing data. Run both engines with the **same**
  `--core_min_eps/--core_max_eps`, `--market_data_min_eps/--market_data_max_eps` and
  `--orders_min_per_sec/--orders_max_per_sec` (and `--min_levels/--max_levels`), or the
  row density will step at the hand-off even though prices stay continuous.
- **Continuation seed.** Use `--incremental true` (faster-than-life) on the continuing
  run: it seeds mid/spread/indicators from the last stored `core_price` row per symbol
  — exactly like the Python `--incremental` — so `open(seam) == close(prev)` holds and
  OHLC candles join cleanly. Real-time never uses incremental on either engine; it
  reseeds from live Yahoo quotes, so a faster-than-life → live transition deliberately
  snaps to the real market (identical behaviour on both).
- **Same build / flags.** Views are `IF NOT EXISTS` (the first creator fixes the
  definition), and `--enterprise/--short_ttl/--prefix/--suffix` must match across the
  engines so both address the same tables with the same schema.

Because both engines reconstruct `market_data` and `core_price` from a **single**
`core_price` seed, the order-book `best_bid` can step a few pips at the seam to realign
with `core_price`. This is the same on either engine (it is how the Python generator's
own incremental restart behaves) and is bounded by the spread.

## Parameters

Option names accept Python underscore form (`--start_ts`) or kebab form
(`--start-ts`).

### Pools (one thread set per table)

| Flag | Default | Purpose |
| --- | --- | --- |
| `--trades_processes <n>` | 1 | worker threads for `qwp_trades`, 0–30 (0 = off) |
| `--market_data_processes <n>` | 0 | worker threads for `qwp_market_data`, 0–30 (0 = off) |
| `--core_processes <n>` | 0 | worker threads for `qwp_core_price`, 0–30 (0 = off) |

### Volume / time (each rate is the **table-wide total** across that pool)

| Flag | Default | Purpose |
| --- | --- | --- |
| `--orders_min/max_per_sec` | 50 / 200 | `qwp_trades` orders/sec; each order → 1+ fills |
| `--market_data_min/max_eps` | 1200 / 15000 | `qwp_market_data` snapshots/sec |
| `--core_min/max_eps` | 700 / 1000 | `qwp_core_price` top-of-book snapshots/sec |
| `--min_levels` / `--max_levels` | 40 / 40 | order-book depth per snapshot |
| `--total_market_data_events <n>` | 1000000 | max **market_data** rows (the dominant table; caps trades if market_data is off); stops the whole run; `0` = unlimited |
| `--start_ts` / `--end_ts <iso>` | after last row / none | data-time window (bound the volume) |
| `--run_secs <n>` | 0 | **wall-clock** stop (throughput tests); not a data window |
| `--commit_interval_ms <n>` | 1000 | global commit (WAL transaction) cadence in ms |
| `--trades_commit_interval_ms <n>` | inherit | commit cadence for `qwp_trades` only |
| `--market_data_commit_interval_ms <n>` | inherit | commit cadence for `qwp_market_data` only |
| `--core_commit_interval_ms <n>` | inherit | commit cadence for `qwp_core_price` only |

Commit cadence is **per pool**; each per-table flag overrides `--commit_interval_ms`
for its table (unset = inherit the global). In **real-time** the cadence can be
**sub-second** — each data-second is sliced into `commit_interval` chunks that are
emitted, flushed and paced individually, so e.g. `--market_data_commit_interval_ms 400`
commits the order book ~2.5×/sec (fresher live dashboard) while trades/prices stay at
1s. In **faster-than-life** the cadence is wall-clock based (larger interval = more
data-seconds squashed per transaction = higher throughput); sub-second values there
just mean "commit as often as possible".

Whichever stop condition (`--end_ts`, `--total_market_data_events`, `--run_secs`)
is reached **first** ends the run. Total elapsed time is always reported. Reporting
depends on the mode:

- **Benchmark mode (`--run_secs > 0`):** a per-second `[rate]` line (`trades` / `md`
  / `core`) plus a min/median/avg/max summary at the end.
- **Continuous mode (no `--run_secs`):** a lightweight `[hb]` heartbeat every 10s
  showing, per enabled table, the average rows/sec over the interval and the
  accumulated row count, plus a `total` column aggregating all three, e.g.
  `[hb] t=20s  trades 25,030/s (500,600)  md 749,900/s (15M)  core 240,100/s (4.8M)  total 1,015,030/s (20.3M)`.

### Reference data / schema

| Flag | Default | Purpose |
| --- | --- | --- |
| `--yahoo_refresh_secs <n>` | 300 | real-time Yahoo refresh interval |
| `--realtime_lookahead_secs <n>` | 2 | real-time: stamp rows n s ahead of wall-clock so the live dashboard stays ahead of WAL apply lag |
| `--no_yahoo` | off | skip Yahoo, use template brackets (offline) |
| `--incremental [true\|false]` | false | seed full state (mid, spread, indicators) from the last stored `core_price` row, skip Yahoo; **faster-than-life only** (rejected in real-time, which always syncs live quotes) |
| `--short_ttl` / `--enterprise [true\|false]` | off | retention (TTL, or STORAGE POLICY with enterprise on the base tables) |
| `--create_views [true\|false]` | **true** | build the full Python-parity matview set (11 views) |
| `--prefix <s>` | `qwp_` | table-name prefix (verbatim); set `""` to match the Python names exactly |
| `--suffix <s>` | none | suffix (verbatim); tables become `<prefix>fx_trades<s>` / `<prefix>market_data<s>` / `<prefix>core_price<s>` |
| `--lei_pool_size <n>` | 2000 | distinct counterparties |

### Accepted but unused / unsupported

`--processes` is **removed** — use `--trades_processes` / `--market_data_processes`
/ `--core_processes`. `--chunk_seconds` is accepted for parity but has no effect.
ILP/PG-transport flags (`--protocol`, `--pg_port`, `--ilp_user`, `--token_x`,
`--token_y`) are not supported and error if passed.

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
