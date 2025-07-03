import argparse
import time
import random
import multiprocessing as mp
import numpy as np
import datetime
from questdb.ingress import Sender, TimestampNanos
import psycopg as pg

FX_PAIRS = [
    ("EURUSD", 1.05, 1.10, 5, 0.0001),
    ("GBPUSD", 1.25, 1.30, 5, 0.0001),
    ("USDJPY", 150.0, 155.0, 3, 0.01),
    ("USDCHF", 0.90, 0.95, 4, 0.0001),
    ("AUDUSD", 0.65, 0.70, 5, 0.0001),
    ("NZDUSD", 0.60, 0.65, 5, 0.0001),
    ("USDCAD", 1.35, 1.40, 5, 0.0001),
    ("EURGBP", 0.85, 0.88, 5, 0.0001),
    ("EURJPY", 160.0, 165.0, 3, 0.01),
    ("GBPJPY", 180.0, 185.0, 3, 0.01),
    ("EURCHF", 0.95, 1.00, 4, 0.0001),
    ("EURAUD", 1.55, 1.60, 5, 0.0001),
    ("GBPCHF", 1.10, 1.15, 4, 0.0001),
    ("AUDJPY", 100.0, 105.0, 3, 0.01),
    ("NZDJPY", 95.0, 100.0, 3, 0.01),
    ("USDSEK", 10.0, 11.0, 4, 0.0001),
    ("USDNOK", 10.0, 11.0, 4, 0.0001),
    ("USDMXN", 17.0, 18.0, 4, 0.0001),
    ("USDSGD", 1.35, 1.40, 5, 0.0001),
    ("USDHKD", 7.75, 7.85, 4, 0.0001),
    ("USDZAR", 18.0, 19.0, 4, 0.0001),
    ("USDTRY", 27.0, 28.0, 4, 0.0001),
    ("EURCAD", 1.45, 1.50, 5, 0.0001),
    ("EURNZD", 1.70, 1.75, 5, 0.0001),
    ("GBPAUD", 1.85, 1.90, 5, 0.0001),
    ("GBPNZD", 2.00, 2.05, 5, 0.0001),
    ("AUDCAD", 0.85, 0.90, 5, 0.0001),
    ("AUDNZD", 1.05, 1.10, 5, 0.0001),
    ("NZDCAD", 0.80, 0.85, 5, 0.0001),
    ("CADJPY", 110.0, 115.0, 3, 0.01)
]

VOLUME_LADDER = [1_000_000, 10_000_000, 30_000_000, 50_000_000, 100_000_000, 150_000_000, 200_000_000, 300_000_000, 500_000_000, 1_000_000_000]

def ensure_tables_exist(args):
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"
    with pg.connect(conn_str, autocommit=True) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            timestamp TIMESTAMP,
            symbol SYMBOL CAPACITY 15000,
            bids DOUBLE[][],
            asks DOUBLE[][]
        ) timestamp(timestamp) PARTITION BY HOUR;
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS core_price (
            timestamp TIMESTAMP,
            symbol SYMBOL CAPACITY 15000,
            ecn SYMBOL,
            bid_price DOUBLE,
            bid_volume LONG,
            ask_price DOUBLE,
            ask_volume LONG,
            reason SYMBOL,
            indicator1 DOUBLE,
            indicator2 DOUBLE
        ) timestamp(timestamp) PARTITION BY HOUR;
        """)

def ensure_materialized_views_exist(args):
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"
    with pg.connect(conn_str, autocommit=True) as conn:
        conn.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS core_price_1s AS (
            SELECT timestamp, symbol,
                first((bid_price + ask_price)/2) AS open_mid,
                max((bid_price + ask_price)/2) AS high_mid,
                min((bid_price + ask_price)/2) AS low_mid,
                last((bid_price + ask_price)/2) AS close_mid,
                last(ask_price) - last(bid_price) AS last_spread,
                max(bid_price) AS max_bid,
                min(bid_price) AS min_bid,
                avg(bid_price) AS avg_bid,
                max(ask_price) AS max_ask,
                min(ask_price) AS min_ask,
                avg(ask_price) AS avg_ask
            FROM core_price
            SAMPLE BY 1s
        ) PARTITION BY HOUR TTL 4 HOURS;
        """)
        conn.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS core_price_1d REFRESH EVERY 1h DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                first((bid_price + ask_price)/2) AS open_mid,
                max((bid_price + ask_price)/2) AS high_mid,
                min((bid_price + ask_price)/2) AS low_mid,
                last((bid_price + ask_price)/2) AS close_mid,
                last(ask_price) - last(bid_price) AS last_spread,
                max(bid_price) AS max_bid,
                min(bid_price) AS min_bid,
                avg(bid_price) AS avg_bid,
                max(ask_price) AS max_ask,
                min(ask_price) AS min_ask,
                avg(ask_price) AS avg_ask
            FROM core_price
            SAMPLE BY 1d
        );
        """)
        conn.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS bbo_1s AS (
            SELECT timestamp, symbol,
                last(bids[1][1]) AS bid,
                last(asks[1][1]) AS ask
            FROM market_data
            SAMPLE BY 1s
        );
        """)

        conn.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS bbo_1m REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM bbo_1s
            SAMPLE BY 1m
        );
        """)

        conn.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS bbo_1h REFRESH EVERY 10m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM bbo_1m
            SAMPLE BY 1h
        );
        """)

        conn.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS bbo_1d REFRESH EVERY 1h DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM bbo_1h
            SAMPLE BY 1d
        );
        """)

        conn.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS market_data_ohlc_15m AS (
            SELECT timestamp, symbol,
                first((bids[1][1] + asks[1][1])/2) AS open,
                max((bids[1][1] + asks[1][1])/2) AS high,
                min((bids[1][1] + asks[1][1])/2) AS low,
                last((bids[1][1] + asks[1][1])/2) AS close
            FROM market_data
            SAMPLE BY 15m
        ) PARTITION BY HOUR TTL 2 DAYS;
        """)
        conn.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS market_data_ohlc_1d REFRESH EVERY 1h DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                first((bids[1][1] + asks[1][1])/2) AS open,
                max((bids[1][1] + asks[1][1])/2) AS high,
                min((bids[1][1] + asks[1][1])/2) AS low,
                last((bids[1][1] + asks[1][1])/2) AS close
            FROM market_data
            SAMPLE BY 1h
        );
        """)

def load_initial_state(args):
    state = {}
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"
    with pg.connect(conn_str) as conn:
        cur = conn.execute("SELECT symbol, bid_price, ask_price, indicator1, indicator2 FROM core_price LATEST BY symbol")
        for row in cur.fetchall():
            state[row[0]] = {
                "bid_price": row[1], "ask_price": row[2], "indicator1": row[3], "indicator2": row[4]
            }
    return state

def evolve_indicator(value, drift=0.01, shock_prob=0.01):
    value += random.uniform(-drift, drift)
    if random.random() < shock_prob:
        value += random.uniform(-0.2, 0.2)
    return max(0.0, min(1.0, value))

def get_volume(level):
    return VOLUME_LADDER[min(level, len(VOLUME_LADDER) - 1)]

def generate_bids_asks(prebuilt_bids, prebuilt_asks, levels, best_bid, best_ask, precision, pip):
    for i in range(levels):
        price_bid = round(best_bid - i * pip, precision)
        price_ask = round(best_ask + i * pip, precision)
        volume = get_volume(i)
        prebuilt_bids[0][i] = price_bid
        prebuilt_bids[1][i] = volume
        prebuilt_asks[0][i] = price_ask
        prebuilt_asks[1][i] = volume
    return prebuilt_bids, prebuilt_asks

def generate_events_for_second(start_ns, market_event_count, core_count, state, sender, min_levels, max_levels, prebuilt_bid_arrays, prebuilt_ask_arrays):
    offsets_market = sorted([random.randint(0, 999_999_999) for _ in range(market_event_count)])
    offsets_core = sorted([random.randint(0, 999_999_999) for _ in range(core_count)])

    for offset in offsets_market:
        symbol, low, high, precision, pip = random.choice(FX_PAIRS)
        levels = random.randint(min_levels, max_levels)
        bids, asks = prebuilt_bid_arrays[levels - 1], prebuilt_ask_arrays[levels - 1]

        mid_price = (state[symbol]["bid_price"] + state[symbol]["ask_price"]) / 2 if symbol in state else random.uniform(low, high)
        spread = round(random.uniform(0.0001, 0.0005), precision)
        best_bid = round(mid_price - spread / 2, precision)
        best_ask = round(mid_price + spread / 2, precision)

        generate_bids_asks(bids, asks, levels, best_bid, best_ask, precision, pip)

        ts = start_ns + offset
        sender.row("market_data", symbols={"symbol": symbol}, columns={"bids": bids, "asks": asks}, at=TimestampNanos(ts))

    for offset in offsets_core:
        symbol, low, high, precision, pip = random.choice(FX_PAIRS)
        levels = random.randint(min_levels, max_levels)
        bids, asks = prebuilt_bid_arrays[levels - 1], prebuilt_ask_arrays[levels - 1]

        mid_price = (state[symbol]["bid_price"] + state[symbol]["ask_price"]) / 2 if symbol in state else random.uniform(low, high)
        indicators = state.get(symbol, {"indicator1": 0.2, "indicator2": 0.5})
        spread = round(random.uniform(0.0001, 0.0005), precision)
        best_bid = round(mid_price - spread / 2, precision)
        best_ask = round(mid_price + spread / 2, precision)

        generate_bids_asks(bids, asks, levels, best_bid, best_ask, precision, pip)

        indicators["indicator1"] = evolve_indicator(indicators["indicator1"])
        indicators["indicator2"] = evolve_indicator(indicators["indicator2"])
        reason = random.choice(["normal", "news_event", "liquidity_event"])
        ecn = random.choice(["LMAX", "EBS", "Hotspot", "Currenex"])
        ts = start_ns + offset

        sender.row("core_price", symbols={"symbol": symbol, "ecn": ecn, "reason": reason}, columns={
            "bid_price": float(best_bid), "bid_volume": int(bids[1][0]),
            "ask_price": float(best_ask), "ask_volume": int(asks[1][0]),
            "indicator1": float(round(indicators["indicator1"], 3)), "indicator2": float(round(indicators["indicator2"], 3))
        }, at=TimestampNanos(ts))

def ingest_worker(args, mode, per_second_plan, total_market_data_events, start_timestamp_ns, end_timestamp_ns, state):
    if args.protocol == "http":
        conf = f"http::addr={args.host}:9000;" if not args.token else f"https::addr={args.host}:9000;username={args.ilp_user};token={args.token};tls_verify=unsafe_off;"
    else:
        conf = f"tcp::addr={args.host}:9009;protocol_version=2;" if not args.token else f"tcps::addr={args.host}:9009;username={args.ilp_user};token={args.token};token_x={args.token_x};token_y={args.token_y};tls_verify=unsafe_off;protocol_version=2;"

    prebuilt_bid_arrays = [np.zeros((2, levels), dtype=np.float64) for levels in range(1, args.max_levels + 1)]
    prebuilt_ask_arrays = [np.zeros((2, levels), dtype=np.float64) for levels in range(1, args.max_levels + 1)]

    with Sender.from_conf(conf) as sender:
        simulated_time_ns = start_timestamp_ns
        market_data_sent = 0

        for market_total_rows, core_total in per_second_plan:
            if end_timestamp_ns and simulated_time_ns >= end_timestamp_ns or market_data_sent >= total_market_data_events:
                break

            generate_events_for_second(simulated_time_ns, market_total_rows // args.processes, core_total // args.processes, state, sender, args.min_levels, args.max_levels, prebuilt_bid_arrays, prebuilt_ask_arrays)
            market_data_sent += market_total_rows // args.processes
            simulated_time_ns += int(1e9)

            sender.flush()

            if mode == "real-time":
                time.sleep(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--pg_port", default="8812")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default="quest")
    parser.add_argument("--ilp_user", default="admin")
    parser.add_argument("--token", default=None)
    parser.add_argument("--token_x", default=None)
    parser.add_argument("--token_y", default=None)
    parser.add_argument("--protocol", choices=["http", "tcp"], default="http")
    parser.add_argument("--mode", choices=["real-time", "faster-than-life"], required=True)
    parser.add_argument("--market_data_min_eps", type=int, default=1000)
    parser.add_argument("--market_data_max_eps", type=int, default=15000)
    parser.add_argument("--core_min_eps", type=int, default=800)
    parser.add_argument("--core_max_eps", type=int, default=1100)
    parser.add_argument("--total_market_data_events", type=int, default=1000000)
    parser.add_argument("--start_ts", type=str, default=None)
    parser.add_argument("--end_ts", type=str, default=None)
    parser.add_argument("--processes", type=int, default=4)
    parser.add_argument("--min_levels", type=int, default=5)
    parser.add_argument("--max_levels", type=int, default=5)
    parser.add_argument("--incremental", action="store_true")
    args = parser.parse_args()

    ensure_tables_exist(args)
    ensure_materialized_views_exist(args)
    state = load_initial_state(args) if args.incremental else {}

    if args.start_ts:
        dt = datetime.datetime.fromisoformat(args.start_ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        start_ts_ns = int(dt.timestamp() * 1e9)
    else:
        start_ts_ns = int(time.time() * 1e9)

    end_ts_ns = None
    if args.end_ts:
        dt = datetime.datetime.fromisoformat(args.end_ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        end_ts_ns = int(dt.timestamp() * 1e9)

    per_second_plan = []
    estimated_seconds = args.total_market_data_events // (args.market_data_min_eps)
    for _ in range(estimated_seconds * 2):
        market_total_rows = random.randint(args.market_data_min_eps, args.market_data_max_eps)
        core_total = random.randint(args.core_min_eps, args.core_max_eps)
        per_second_plan.append((market_total_rows, core_total))

    pool = []
    for _ in range(args.processes):
        p = mp.Process(target=ingest_worker, args=(args, args.mode, per_second_plan, args.total_market_data_events // args.processes, start_ts_ns, end_ts_ns, state))
        p.start()
        pool.append(p)

    for p in pool:
        p.join()

if __name__ == "__main__":
    main()



