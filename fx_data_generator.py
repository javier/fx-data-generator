import argparse
import time
import random
import multiprocessing as mp
import numpy as np
import datetime
from questdb.ingress import Sender, TimestampNanos
import psycopg as pg

# ---------------------------
# Table Creation
# ---------------------------

def ensure_table_exists(args):
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"

    market_data_ddl = f"""
    CREATE TABLE IF NOT EXISTS market_data (
        timestamp TIMESTAMP,
        symbol SYMBOL CAPACITY 15000,
        side SYMBOL,
        price DOUBLE,
        size DOUBLE
    ) timestamp(timestamp) PARTITION BY HOUR;
    """

    core_price_ddl = f"""
    CREATE TABLE IF NOT EXISTS core_price (
        timestamp TIMESTAMP,
        symbol SYMBOL CAPACITY 15000,
        mid_price DOUBLE,
        spread DOUBLE,
        bid_price DOUBLE,
        ask_price DOUBLE,
        reason SYMBOL,
        indicator1 DOUBLE,
        indicator2 DOUBLE
    ) timestamp(timestamp) PARTITION BY HOUR;
    """

    with pg.connect(conn_str, autocommit=True) as conn:
        conn.execute(market_data_ddl)
        conn.execute(core_price_ddl)

# ---------------------------
# FX Data Generator Functions
# ---------------------------

FX_PAIRS = [
    ("EUR/USD", 1.05, 1.10),
    ("GBP/USD", 1.25, 1.30),
    ("USD/JPY", 150.0, 155.0),
    ("USD/CHF", 0.90, 0.95),
    ("AUD/USD", 0.65, 0.70),
    ("NZD/USD", 0.60, 0.65),
    ("USD/CAD", 1.35, 1.40),
    ("EUR/GBP", 0.85, 0.88),
    ("EUR/JPY", 160.0, 165.0),
    ("GBP/JPY", 180.0, 185.0),
    ("EUR/CHF", 0.95, 1.00),
    ("EUR/AUD", 1.55, 1.60),
    ("GBP/CHF", 1.10, 1.15),
    ("AUD/JPY", 100.0, 105.0),
    ("NZD/JPY", 95.0, 100.0),
    ("USD/SEK", 10.0, 11.0),
    ("USD/NOK", 10.0, 11.0),
    ("USD/MXN", 17.0, 18.0),
    ("USD/SGD", 1.35, 1.40),
    ("USD/HKD", 7.75, 7.85),
    ("USD/ZAR", 18.0, 19.0),
    ("USD/TRY", 27.0, 28.0),
    ("EUR/CAD", 1.45, 1.50),
    ("EUR/NZD", 1.70, 1.75),
    ("GBP/AUD", 1.85, 1.90),
    ("GBP/NZD", 2.00, 2.05),
    ("AUD/CAD", 0.85, 0.90),
    ("AUD/NZD", 1.05, 1.10),
    ("NZD/CAD", 0.80, 0.85),
    ("CAD/JPY", 110.0, 115.0)
]

def generate_price(symbol, low, high):
    return round(random.uniform(low, high), 5)

def evolve_indicator(value, drift=0.01, shock_prob=0.01):
    value += random.uniform(-drift, drift)
    if random.random() < shock_prob:
        value += random.uniform(-0.2, 0.2)
    return max(0.0, min(1.0, value))

def generate_events_for_second(start_ns, market_event_count, core_count, state, sender):
    offsets_market = sorted([random.randint(0, 999_999_999) for _ in range(market_event_count)])
    offsets_core = sorted([random.randint(0, 999_999_999) for _ in range(core_count)])

    for offset in offsets_market:
        symbol, low, high = random.choice(FX_PAIRS)
        mid_price = generate_price(symbol, low, high)
        spread = round(random.uniform(0.0001, 0.0005), 5)
        bid_price = mid_price - spread / 2
        ask_price = mid_price + spread / 2
        size = round(random.uniform(0.1, 5.0), 2)
        ts = start_ns + offset

        sender.row("market_data", symbols={"symbol": symbol, "side": "bid"}, columns={"price": bid_price, "size": size}, at=TimestampNanos(ts))
        sender.row("market_data", symbols={"symbol": symbol, "side": "ask"}, columns={"price": ask_price, "size": size}, at=TimestampNanos(ts))

    for offset in offsets_core:
        symbol, low, high = random.choice(FX_PAIRS)
        mid_price = generate_price(symbol, low, high)
        spread = round(random.uniform(0.0001, 0.0005), 5)
        bid_price = mid_price - spread / 2
        ask_price = mid_price + spread / 2

        indicators = state[symbol]
        indicators["indicator1"] = evolve_indicator(indicators["indicator1"])
        indicators["indicator2"] = evolve_indicator(indicators["indicator2"])

        reason = random.choice(["normal", "news_event", "liquidity_event"])
        ts = start_ns + offset

        sender.row(
            "core_price",
            symbols={"symbol": symbol, "reason": reason},
            columns={
                "mid_price": mid_price,
                "spread": spread,
                "bid_price": bid_price,
                "ask_price": ask_price,
                "indicator1": round(indicators["indicator1"], 3),
                "indicator2": round(indicators["indicator2"], 3)
            },
            at=TimestampNanos(ts)
        )

# ---------------------------
# Ingestion Worker
# ---------------------------

def ingest_worker(args, mode, per_second_plan, total_market_data_events, start_timestamp_ns, end_timestamp_ns):
    if args.token:
        conf = f"tcps::addr={args.host}:9009;username={args.ilp_user};token={args.token};token_x={args.token_x};token_y={args.token_y};tls_verify=unsafe_off;"
    else:
        conf = f"tcp::addr={args.host}:9009;"

    with Sender.from_conf(conf) as sender:

        state = {symbol: {"indicator1": 0.2, "indicator2": 0.5} for symbol, _, _ in FX_PAIRS}
        simulated_time_ns = start_timestamp_ns
        market_data_sent = 0

        for market_total_rows, core_total in per_second_plan:
            if end_timestamp_ns and simulated_time_ns >= end_timestamp_ns:
                break

            market_event_count = market_total_rows // 2
            per_worker_market = market_event_count // args.processes
            per_worker_core = core_total // args.processes

            generate_events_for_second(simulated_time_ns, per_worker_market, per_worker_core, state, sender)

            market_data_sent += per_worker_market * 2
            simulated_time_ns += int(1e9)

            sender.flush()

            if market_data_sent >= total_market_data_events:
                break

            if mode == "real-time":
                time.sleep(1)

# ---------------------------
# Main Entry Point
# ---------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--pg_port", default="8812")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default="quest")
    parser.add_argument("--ilp_user", default="admin", help="ILP username for tcps connection")
    parser.add_argument("--token", default=None, help="ILP token to enable tcps connection")
    parser.add_argument("--token_x", default=None, help="Optional token_x for tcps connection")
    parser.add_argument("--token_y", default=None, help="Optional token_y for tcps connection")
    parser.add_argument("--mode", choices=["real-time", "faster-than-life"], required=True)
    parser.add_argument("--market_data_min_eps", type=int, default=1000)
    parser.add_argument("--market_data_max_eps", type=int, default=15000)
    parser.add_argument("--core_min_eps", type=int, default=800)
    parser.add_argument("--core_max_eps", type=int, default=1100)
    parser.add_argument("--total_market_data_events", type=int, default=1000000)
    parser.add_argument("--start_ts", type=str, default=None, help="Start timestamp in ISO UTC format, e.g. 2025-06-27T00:00:00")
    parser.add_argument("--end_ts", type=str, default=None, help="Exclusive end timestamp for faster-than-life mode in ISO UTC format")
    parser.add_argument("--processes", type=int, default=4)

    args = parser.parse_args()
    ensure_table_exists(args)

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
        if market_total_rows % 2 != 0:
            market_total_rows -= 1  # ensure even number for bid/ask pairs
        core_total = random.randint(args.core_min_eps, args.core_max_eps)
        per_second_plan.append((market_total_rows, core_total))

    pool = []
    for _ in range(args.processes):
        p = mp.Process(
            target=ingest_worker,
            args=(args, args.mode, per_second_plan, args.total_market_data_events // args.processes, start_ts_ns, end_ts_ns)
        )
        p.start()
        pool.append(p)

    for p in pool:
        p.join()

if __name__ == "__main__":
    main()
