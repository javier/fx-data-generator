import argparse
import time
import random
import multiprocessing as mp
from multiprocessing import Event
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
        CREATE MATERIALIZED VIEW IF NOT EXISTS market_data_ohlc_1m AS (
            SELECT timestamp, symbol,
                first((bids[1][1] + asks[1][1])/2) AS open,
                max((bids[1][1] + asks[1][1])/2) AS high,
                min((bids[1][1] + asks[1][1])/2) AS low,
                last((bids[1][1] + asks[1][1])/2) AS close,
                SUM(bids[2][1] + asks[2][1]) AS total_volume
            FROM market_data
            SAMPLE BY 1m
        ) PARTITION BY HOUR TTL 12 HOURS;
        """)

        conn.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS market_data_ohlc_15m AS (
            SELECT timestamp, symbol,
                first(open) AS open,
                max(high) AS high,
                min(low) AS low,
                last(close) AS close,
                SUM(total_volume) AS total_volume

            FROM market_data_ohlc_1m
            SAMPLE BY 15m
        ) PARTITION BY HOUR TTL 2 DAYS;
        """)

        conn.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS market_data_ohlc_1d REFRESH EVERY 1h DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                first((bids[1][1] + asks[1][1])/2) AS open,
                max((bids[1][1] + asks[1][1])/2) AS high,
                min((bids[1][1] + asks[1][1])/2) AS low,
                last((bids[1][1] + asks[1][1])/2) AS close,
                SUM(bids[2][1] + asks[2][1]) AS total_volume
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
                "bid_price": row[1], "ask_price": row[2],
                "indicator1": row[3], "indicator2": row[4],
                "spread": 0.0004
            }
    return state

def evolve_mid_price(prev_mid, low, high, precision, pip, drift=0.2, shock_prob=0.001):
    change = random.uniform(-drift * pip, drift * pip)
    if random.random() < shock_prob:
        change += random.uniform(-20 * pip, 20 * pip)
    new_mid = prev_mid + change
    return round(max(low, min(high, new_mid)), precision)

def evolve_indicator(value, drift=0.01, shock_prob=0.01):
    value += random.uniform(-drift, drift)
    if random.random() < shock_prob:
        value += random.uniform(-0.2, 0.2)
    return max(0.0, min(1.0, value))

def get_random_volume_for_level(level_idx):
    if level_idx == 0:
        return random.randint(VOLUME_LADDER[0] // 2, VOLUME_LADDER[0])
    lower = VOLUME_LADDER[level_idx - 1]
    upper = VOLUME_LADDER[level_idx]
    return random.randint(lower, upper)

def generate_bids_asks(prebuilt_bids, prebuilt_asks, levels, best_bid, best_ask, precision, pip):
    for i in range(levels):
        price_bid = round(best_bid - i * pip, precision)
        price_ask = round(best_ask + i * pip, precision)
        prebuilt_bids[0][i] = price_bid
        prebuilt_bids[1][i] = get_random_volume_for_level(i)
        prebuilt_asks[0][i] = price_ask
        prebuilt_asks[1][i] = get_random_volume_for_level(i)
    return prebuilt_bids, prebuilt_asks

def precompute_state(args, start_ns, total_seconds, state_template):
    # state_template is a dict {symbol: {bid_price, ask_price, spread, ...}}
    states = []
    current = {k: v.copy() for k, v in state_template.items()}
    for second in range(total_seconds):
        # evolve state for each symbol
        next_state = {}
        for symbol, low, high, precision, pip in FX_PAIRS:
            prev_mid = (current[symbol]["bid_price"] + current[symbol]["ask_price"]) / 2
            prev_spread = current[symbol]["spread"]
            new_mid = evolve_mid_price(prev_mid, low, high, precision, pip)
            new_spread = round(max(pip, prev_spread + random.uniform(-0.2 * pip, 0.2 * pip)), precision)
            next_state[symbol] = {
                "bid_price": round(new_mid - new_spread / 2, precision),
                "ask_price": round(new_mid + new_spread / 2, precision),
                "spread": new_spread,
                "indicator1": evolve_indicator(current[symbol]["indicator1"]),
                "indicator2": evolve_indicator(current[symbol]["indicator2"])
            }
        states.append(next_state)
        current = next_state
    return states

def generate_events_for_second(ts, market_event_count, core_count, state_for_second, sender, min_levels, max_levels, prebuilt_bid_arrays, prebuilt_ask_arrays):
    offsets_market = sorted(random.randint(0, 999_999_999) for _ in range(market_event_count))
    offsets_core = sorted(random.randint(0, 999_999_999) for _ in range(core_count))
    for offset in offsets_market:
        symbol, low, high, precision, pip = random.choice(FX_PAIRS)
        levels = random.randint(min_levels, max_levels)
        bids, asks = prebuilt_bid_arrays[levels - 1], prebuilt_ask_arrays[levels - 1]
        mid_price = (state_for_second[symbol]["bid_price"] + state_for_second[symbol]["ask_price"]) / 2
        spread = state_for_second[symbol]["spread"]
        mid_jitter = random.uniform(-0.5*pip, 0.5*pip)
        best_bid = round(mid_price + mid_jitter - spread / 2, precision)
        best_ask = round(mid_price + mid_jitter + spread / 2, precision)
        generate_bids_asks(bids, asks, levels, best_bid, best_ask, precision, pip)
        row_ts = ts + offset
        sender.row("market_data", symbols={"symbol": symbol}, columns={"bids": bids, "asks": asks}, at=TimestampNanos(row_ts))
    for offset in offsets_core:
        symbol, low, high, precision, pip = random.choice(FX_PAIRS)
        levels = random.randint(min_levels, max_levels)
        bids, asks = prebuilt_bid_arrays[levels - 1], prebuilt_ask_arrays[levels - 1]
        indicators = state_for_second[symbol]
        mid_price = (state_for_second[symbol]["bid_price"] + state_for_second[symbol]["ask_price"]) / 2
        spread = state_for_second[symbol]["spread"]
        mid_jitter = random.uniform(-0.5*pip, 0.5*pip)
        best_bid = round(mid_price + mid_jitter - spread / 2, precision)
        best_ask = round(mid_price + mid_jitter + spread / 2, precision)
        generate_bids_asks(bids, asks, levels, best_bid, best_ask, precision, pip)
        reason = random.choice(["normal", "news_event", "liquidity_event"])
        ecn = random.choice(["LMAX", "EBS", "Hotspot", "Currenex"])
        row_ts = ts + offset
        lvl = random.randint(0, levels - 1)
        sender.row("core_price", symbols={"symbol": symbol, "ecn": ecn, "reason": reason}, columns={
            "bid_price": float(bids[0][lvl]), "bid_volume": int(bids[1][lvl]),
            "ask_price": float(asks[0][lvl]), "ask_volume": int(asks[1][lvl]),
            "indicator1": float(round(indicators["indicator1"], 3)), "indicator2": float(round(indicators["indicator2"], 3))
        }, at=TimestampNanos(row_ts))

def ingest_worker(args, per_second_plan, total_events, start_ns, end_ns, global_states, process_idx, processes, pause_event):
    if args.protocol == "http":
        conf = f"http::addr={args.host}:9000;" if not args.token else f"https::addr={args.host}:9000;username={args.ilp_user};token={args.token};tls_verify=unsafe_off;"
    else:
        conf = f"tcp::addr={args.host}:9009;protocol_version=2;" if not args.token else f"tcps::addr={args.host}:9009;username={args.ilp_user};token={args.token};token_x={args.token_x};token_y={args.token_y};tls_verify=unsafe_off;protocol_version=2;"

    prebuilt_bids = [np.zeros((2, lvl), dtype=np.float64) for lvl in range(1, args.max_levels + 1)]
    prebuilt_asks = [np.zeros((2, lvl), dtype=np.float64) for lvl in range(1, args.max_levels + 1)]
    ts = start_ns
    sent = 0

    with Sender.from_conf(conf) as sender:
        for sec_idx, (market_total, core_total) in enumerate(per_second_plan):
            # ---- WAL PAUSE CHECK ----
            while pause_event.is_set():
                print(f"[WORKER {process_idx}] Paused due to WAL lag (waiting for sequencerTxn==writerTxn)...")
                time.sleep(2)
            # ---- /WAL PAUSE CHECK ----
            if (end_ns and ts >= end_ns) or (sent >= total_events):
                break

            state_for_this_second = global_states[sec_idx]
            generate_events_for_second(
                ts, market_total, core_total,
                state_for_this_second, sender,
                args.min_levels, args.max_levels, prebuilt_bids, prebuilt_asks
            )
            sent += market_total
            ts += int(1e9)
            sender.flush()
            if args.mode == "real-time":
                time.sleep(1)

def wal_monitor(args, pause_event, processes, interval=5):
    import time
    import psycopg as pg
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"
    threshold = 3 * processes
    last_logged_paused = False

    with pg.connect(conn_str, autocommit=True) as conn:
        while True:
            cur = conn.execute("SELECT sequencerTxn, writerTxn FROM wal_tables() WHERE name = 'market_data'")
            row = cur.fetchone()
            if row:
                seq, wrt = row
                lag = seq - wrt
                if lag > threshold:
                    pause_event.set()
                    if not last_logged_paused:
                        print(f"[WAL MONITOR] Pausing ingestion: sequencerTxn={seq}, writerTxn={wrt}, lag={lag}")
                        last_logged_paused = True
                elif last_logged_paused:
                    # Only resume if sequencerTxn == writerTxn (i.e., no lag at all)
                    if seq == wrt:
                        print(f"[WAL MONITOR] Resuming ingestion: sequencerTxn={seq}, writerTxn={wrt}, lag={lag}")
                        pause_event.clear()
                        last_logged_paused = False
            time.sleep(interval)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--pg_port", default="8812")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default="quest")
    parser.add_argument("--token", default=None)
    parser.add_argument("--token_x", default=None)
    parser.add_argument("--token_y", default=None)
    parser.add_argument("--ilp_user", default="admin")
    parser.add_argument("--protocol", choices=["http", "tcp"], default="http")
    parser.add_argument("--mode", choices=["real-time", "faster-than-life"], required=True)
    parser.add_argument("--market_data_min_eps", type=int, default=1000)
    parser.add_argument("--market_data_max_eps", type=int, default=15000)
    parser.add_argument("--core_min_eps", type=int, default=800)
    parser.add_argument("--core_max_eps", type=int, default=1100)
    parser.add_argument("--total_market_data_events", type=int, default=1_000_000)
    parser.add_argument("--start_ts", type=str)
    parser.add_argument("--end_ts", type=str)
    parser.add_argument("--processes", type=int, default=4)
    parser.add_argument("--min_levels", type=int, default=5)
    parser.add_argument("--max_levels", type=int, default=5)
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--create_views", type=lambda x: str(x).lower() != 'false', default=True)
    args = parser.parse_args()

    if args.incremental:
        state = load_initial_state(args)
    else:
        state = {}
    for symbol, low, high, precision, pip in FX_PAIRS:
        if symbol not in state:
            state[symbol] = {
                "bid_price": round((low + high) / 2, precision),
                "ask_price": round((low + high) / 2, precision),
                "spread": 0.0004, "indicator1": 0.2, "indicator2": 0.5
            }

    ensure_tables_exist(args)
    if args.create_views:
        ensure_materialized_views_exist(args)

    if args.start_ts:
        dt = datetime.datetime.fromisoformat(args.start_ts).replace(tzinfo=datetime.timezone.utc)
        start_ns = int(dt.timestamp() * 1e9)
    else:
        start_ns = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1e9)

    end_ns = int(datetime.datetime.fromisoformat(args.end_ts).replace(tzinfo=datetime.timezone.utc).timestamp() * 1e9) if args.end_ts else None
    # Precompute full per-second plan so we can split across workers and always get the right event count
    per_second_plan = []
    events_so_far = 0
    while events_so_far < args.total_market_data_events:
        market_total = random.randint(args.market_data_min_eps, args.market_data_max_eps)
        core_total = random.randint(args.core_min_eps, args.core_max_eps)
        per_second_plan.append((market_total, core_total))
        events_so_far += market_total

    # Trim last second to hit target exactly
    overage = events_so_far - args.total_market_data_events
    if overage > 0:
        market_total, core_total = per_second_plan[-1]
        per_second_plan[-1] = (market_total - overage, core_total)

    total_seconds = len(per_second_plan)
    def split_event_counts(total, num_workers):
        # Returns a list of event counts for each worker that sum to total, as even as possible
        base = total // num_workers
        remainder = total % num_workers
        return [base + (1 if i < remainder else 0) for i in range(num_workers)]

    #To check if writerTxn is lagging
    pause_event = Event()
    # Start WAL monitor process
    wal_proc = mp.Process(target=wal_monitor, args=(args, pause_event, args.processes))
    wal_proc.start()

    # Build worker plans: for every second, split event counts among workers
    worker_plans = [[] for _ in range(args.processes)]
    for sec_idx, (market_total, core_total) in enumerate(per_second_plan):
        market_splits = split_event_counts(market_total, args.processes)
        core_splits = split_event_counts(core_total, args.processes)
        for i in range(args.processes):
            worker_plans[i].append((market_splits[i], core_splits[i]))

    # Precompute state for every simulated second!
    global_states = precompute_state(args, start_ns, total_seconds, state)

    # Start workers, each picks a slice of seconds (no overlap)
    pool = []
    for process_idx in range(args.processes):
        p = mp.Process(
            target=ingest_worker,
            args=(args, worker_plans[process_idx], sum(market for market, _ in worker_plans[process_idx]), start_ns, end_ns, global_states, process_idx, args.processes, pause_event)
        )
        p.start()
        pool.append(p)

    for p in pool:
        p.join()

    wal_proc.terminate()

if __name__ == "__main__":
    main()
