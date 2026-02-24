# FX Query Benchmarks

Concurrent query benchmarks for QuestDB using [wrk](https://github.com/wg/wrk) with Lua scripting.
Queries are sent over the QuestDB REST API (`/exec` endpoint) with Bearer token authentication.

## Setup

Copy the sample env file and fill in your token:

```bash
cp .env.sample .env
# edit .env with your actual TOKEN and HOST
```

## Benchmarks

### 1. OHLC Materialized View (`query_bench_fx.lua`)

Each request queries the `market_data_ohlc_1m` materialized view for a single randomly chosen
FX pair (out of 30) over a random time window between 1 and 20 minutes within the recent past.
Every request picks a different symbol and a different time range, simulating concurrent dashboard
or API consumers each looking at different instruments and timeframes.

**SQL:**

```sql
SELECT *
FROM market_data_ohlc_1m
WHERE symbol = '{random_symbol}'
  AND timestamp IN '$now-{start}m..$now-{end}m'
```

### 2. WINDOW JOIN - Trade Execution Analysis (`query_bench_fx_wj.lua`)

Each request runs a WINDOW JOIN between `fx_trades` and `core_price` for a single randomly chosen
FX pair (out of 30) over a random time window between 10 minutes and 2 hours in the recent past.
The query finds each trade's surrounding market context - the minimum ask and maximum bid within
10 seconds of each trade - simulating execution quality or slippage analysis workloads.

**SQL:**

```sql
SELECT
    t.symbol,
    t.timestamp,
    t.side,
    t.price,
    min(p.ask_price) AS min_ask,
    max(p.bid_price) AS max_bid
FROM fx_trades t
WINDOW JOIN core_price p
    ON (symbol)
    RANGE BETWEEN 10 seconds PRECEDING AND 10 seconds FOLLOWING
    EXCLUDE PREVAILING
WHERE t.symbol = '{random_symbol}'
  AND t.timestamp IN '$now-{start}m..$now-{end}m'
```

## Running

### Using the shell wrappers

The shell scripts source `.env` automatically. Arguments: `THREADS CONNECTIONS DURATION`.

```bash
# OHLC benchmark - defaults: 8 threads, 20 connections, 60s
. ./run_ohlc.sh
. ./run_ohlc.sh 32 32
. ./run_ohlc.sh 32 32 120s

# WINDOW JOIN benchmark
. ./run_wj.sh
. ./run_wj.sh 32 32
```

### Using wrk directly

```bash
TOKEN="your_token" wrk -t32 -c32 -d60s -s query_bench_fx.lua https://172.31.42.41:9000
TOKEN="your_token" wrk -t32 -c32 -d60s -s query_bench_fx_wj.lua https://172.31.42.41:9000
```

### Parameters

| Param | Description |
|-------|-------------|
| `-t`  | Number of threads (ideally matches CPU cores) |
| `-c`  | Number of concurrent connections (must be >= threads) |
| `-d`  | Duration (e.g. `30s`, `1m`, `5m`) |

### Environment variables

| Variable    | Default | Script | Description |
|-------------|---------|--------|-------------|
| `TOKEN`     | -       | both   | Bearer token for QuestDB REST API |
| `HOST`      | -       | .sh    | Target URL (used by shell wrappers) |
| `MAX_RANGE` | 20      | ohlc   | Max lookback in minutes |
| `MAX_RANGE` | 120     | wj     | Max lookback in minutes (2 hours) |
| `MIN_WIDTH` | 10      | wj     | Minimum window width in minutes |

## Reference results

Tested from an EC2 instance (48 CPUs) in the same region as the QuestDB server,
32 threads / 32 connections, 60 seconds.

### OHLC Materialized View

```
Running 1m test @ https://172.31.42.41:9000
  32 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   442.48us  373.41us   9.89ms   98.78%
    Req/Sec     2.31k    61.10     2.51k    75.69%
  4426186 requests in 1.00m, 4.70GB read
Requests/sec:  73646.93
Transfer/sec:     80.14MB
```

### WINDOW JOIN

```
Running 1m test @ https://172.31.42.41:9000
  32 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   396.22ms  222.01ms   1.71s    72.87%
    Req/Sec     3.37      2.56    10.00     88.50%
  4963 requests in 1.00m, 8.75GB read
  Socket errors: connect 0, read 0, write 0, timeout 1
Requests/sec:     82.60
Transfer/sec:    149.13MB
```
