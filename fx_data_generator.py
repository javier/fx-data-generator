import argparse
import time
import sys
import math
import random
import multiprocessing as mp
from multiprocessing import Event
import numpy as np
import datetime
from questdb.ingress import Sender, TimestampNanos
import psycopg as pg
import yfinance as yf

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


def make_ladder(levels=50, v_min=100_000, v_max=1_000_000_000):
    step = (math.log10(v_max) - math.log10(v_min)) / (levels - 1)
    return [int(round(10 ** (math.log10(v_min) + i * step))) for i in range(levels)]


def table_name(name, suffix):
    return name + suffix if suffix else name


def get_latest_timestamp_ns(conn, table):
    cur = conn.execute(f"SELECT timestamp FROM {table} ORDER BY timestamp DESC LIMIT 1")
    row = cur.fetchone()
    if row and row[0]:
        dt = row[0]
        if dt.tzinfo is None:
            # Assume UTC if naive
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return int(dt.timestamp() * 1e9)
    else:
        return None

def ensure_tables_exist(args, suffix):
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"
    short_ttl = args.short_ttl
    with pg.connect(conn_str, autocommit=True) as conn:
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name('market_data', suffix)} (
            timestamp TIMESTAMP,
            symbol SYMBOL CAPACITY 15000,
            bids DOUBLE[][],
            asks DOUBLE[][]
        ) timestamp(timestamp) PARTITION BY HOUR {'TTL 3 DAYS' if short_ttl else ''};
        """)
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name('core_price', suffix)} (
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
        ) timestamp(timestamp) PARTITION BY HOUR {'TTL 3 DAYS' if short_ttl else ''};
        """)

def ensure_materialized_views_exist(args, suffix):
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"
    short_ttl = args.short_ttl
    with pg.connect(conn_str, autocommit=True) as conn:
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('core_price_1s', suffix)}  AS (
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
            FROM {table_name('core_price', suffix)}
            SAMPLE BY 1s
        ) PARTITION BY HOUR TTL 4 HOURS;
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('core_price_1d', suffix)} REFRESH EVERY 1h DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
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
            FROM {table_name('core_price', suffix)}
            SAMPLE BY 1d
        ) PARTITION BY MONTH  {'TTL 1 MONTH' if short_ttl else ''};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('bbo_1s', suffix)} AS (
            SELECT timestamp, symbol,
                last(bids[1][1]) AS bid,
                last(asks[1][1]) AS ask
            FROM {table_name('market_data', suffix)}
            SAMPLE BY 1s
        ) PARTITION BY HOUR  {'TTL 3 DAYS' if short_ttl else ''};
        """)

        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('bbo_1m', suffix)} REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM {table_name('bbo_1s', suffix)}
            SAMPLE BY 1m
        ) PARTITION BY DAY {'TTL 3 DAYS' if short_ttl else ''};
        """)

        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('bbo_1h', suffix)} REFRESH EVERY 10m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM  {table_name('bbo_1m', suffix)}
            SAMPLE BY 1h
        ) PARTITION BY MONTH {'TTL 1 MONTH' if short_ttl else ''};
        """)

        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('bbo_1d', suffix)} REFRESH EVERY 1h DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM {table_name('bbo_1h', suffix)}
            SAMPLE BY 1d
        ) PARTITION BY MONTH  {'TTL 1 MONTH' if short_ttl else ''};
        """)

        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('market_data_ohlc_1m', suffix)}  AS (
            SELECT timestamp, symbol,
                first(bids[1][1]) AS open,
                max(bids[1][1]) AS high,
                min(bids[1][1]) AS low,
                last(bids[1][1]) AS close,
                SUM(bids[2][1]) AS total_volume
            FROM {table_name('market_data', suffix)}
            SAMPLE BY 1m
        ) PARTITION BY HOUR TTL 1 DAY;
        """)

        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('market_data_ohlc_15m', suffix)} AS (
            SELECT timestamp, symbol,
                first(open) AS open,
                max(high) AS high,
                min(low) AS low,
                last(close) AS close,
                SUM(total_volume) AS total_volume

            FROM {table_name('market_data_ohlc_1m', suffix)}
            SAMPLE BY 15m
        ) PARTITION BY HOUR TTL 2 DAYS;
        """)

        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('market_data_ohlc_1d', suffix)} REFRESH EVERY 1h DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                first(bids[1][1]) AS open,
                max(bids[1][1]) AS high,
                min(bids[1][1]) AS low,
                last(bids[1][1]) AS close,
                SUM(bids[2][1]) AS total_volume
            FROM {table_name('market_data', suffix)}
            SAMPLE BY 1d
        ) PARTITION BY MONTH  {'TTL 1 MONTH' if short_ttl else ''};
        """)

# used only with incremental mode
def load_initial_state(args, suffix):
    state = {}
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"
    with pg.connect(conn_str) as conn:
        cur = conn.execute(f"SELECT symbol, bid_price, ask_price, indicator1, indicator2 FROM {table_name('core_price', suffix)} LATEST BY symbol")
        for row in cur.fetchall():
            state[row[0]] = {
                "bid_price": row[1], "ask_price": row[2],
                "indicator1": row[3], "indicator2": row[4],
                "spread": 0.0004
            }
    return state

def load_initial_state_from_brackets(fx_pairs):
    state = {}
    for symbol, low, high, precision, pip in fx_pairs:
        mid = (low + high) / 2
        spread = quantize_to_pip(0.0004, pip)
        state[symbol] = {
            "bid_price": quantize_to_pip(mid - spread / 2, pip),
            "ask_price": quantize_to_pip(mid + spread / 2, pip),
            "spread": spread,
            "indicator1": 0.2,
            "indicator2": 0.5
        }
    return state

def fetch_fx_pairs_from_yahoo(fx_pairs_template, bracket_pct=1.0):
    out = []
    pct = bracket_pct / 100.0
    print("[INFO] Refreshing reference data from Yahoo Finance.", flush=True)
    for symbol, _low, _high, precision, pip in fx_pairs_template:
        ysym = symbol + "=X"
        try:
            bars = yf.Ticker(ysym).history(period="1d", interval="1m")
            if not bars.empty:
                mid = float(bars["Close"].iloc[-1])
                if math.isnan(mid) or mid == 0.0:
                    print(f"[YF] {symbol}: DataFrame non-empty, but got NaN or zero as price! Fallback used.", flush=True)
                    raise ValueError("Yahoo price NaN/zero")
                else:
                    pass
                    #print(f"[YF] {symbol}: Got {mid}")
                low = mid * (1 - pct)
                high = mid * (1 + pct)
            else:
                print(f"[YF] {symbol}: DataFrame EMPTY. Fallback used.", flush=True)
                raise ValueError("No data from Yahoo")
        except Exception:
            low = _low
            high = _high
            print(f"[YF] {symbol}: Fallback to template low/high: {low}, {high}", flush=True)
        out.append((symbol, low, high, precision, pip))
    return out

def fx_pairs_refresher(shared_fx_pairs, interval=300, bracket_pct=1.0, first_ready_event=None):
    while True:
        new_fx_pairs = fetch_fx_pairs_from_yahoo(list(shared_fx_pairs), bracket_pct)
        for i in range(len(shared_fx_pairs)):
            shared_fx_pairs[i] = new_fx_pairs[i]
        if first_ready_event is not None and not first_ready_event.is_set():
            first_ready_event.set()
        time.sleep(interval)

def quantize_to_pip(price, pip):
    decimals = abs(int(round(np.log10(pip))))
    return round(round(price / pip) * pip, decimals)

def evolve_mid_price(prev_mid, low, high, precision, pip, drift=0.9, shock_prob=0.010):
    change = random.uniform(-drift * pip, drift * pip)
    if random.random() < shock_prob:
        change += random.uniform(-20 * pip, 20 * pip)
    new_mid = prev_mid + change
    return quantize_to_pip(max(low, min(high, new_mid)), pip)

def evolve_indicator(value, drift=0.01, shock_prob=0.01):
    value += random.uniform(-drift, drift)
    if random.random() < shock_prob:
        value += random.uniform(-0.2, 0.2)
    return max(0.0, min(1.0, value))

def get_random_volume_for_level(level_idx, ladder):
    if level_idx == 0:
        return random.randint(ladder[0] // 2, ladder[0])
    lower = ladder[level_idx - 1]
    upper = ladder[level_idx]
    return random.randint(lower, upper)

def generate_bids_asks(prebuilt_bids, prebuilt_asks, levels, best_bid, best_ask, precision, pip, ladder):
    for i in range(levels):
        price_bid = quantize_to_pip(best_bid - i * pip, pip)
        price_ask = quantize_to_pip(best_ask + i * pip, pip)
        prebuilt_bids[0][i] = price_bid
        prebuilt_bids[1][i] = get_random_volume_for_level(i, ladder)
        prebuilt_asks[0][i] = price_ask
        prebuilt_asks[1][i] = get_random_volume_for_level(i, ladder)
    return prebuilt_bids, prebuilt_asks

def evolve_state_one_tick(prev_state, fx_pairs, drift=5.0):
    next_state = {}
    for symbol, low, high, precision, pip in fx_pairs:
        prev_mid = (prev_state[symbol]["bid_price"] + prev_state[symbol]["ask_price"]) / 2
        prev_spread = prev_state[symbol]["spread"]
        new_mid = evolve_mid_price(prev_mid, low, high, precision, pip, drift=drift)
        new_spread = quantize_to_pip(max(pip, prev_spread + random.uniform(-0.2 * pip, 0.2 * pip)), pip)
        next_state[symbol] = {
            "bid_price": quantize_to_pip(new_mid - new_spread / 2, pip),
            "ask_price": quantize_to_pip(new_mid + new_spread / 2, pip),
            "spread": new_spread,
            "indicator1": evolve_indicator(prev_state[symbol]["indicator1"]),
            "indicator2": evolve_indicator(prev_state[symbol]["indicator2"])
        }
    return next_state

def precompute_open_close_state(total_seconds, fx_pairs, state_template):
    open_per_second = []
    close_per_second = []

    current = {k: v.copy() for k, v in state_template.items()}
    for _ in range(total_seconds):
        # open: state at start of second
        open_per_second.append({k: v.copy() for k, v in current.items()})
        # evolve to close
        current = evolve_state_one_tick(current, fx_pairs)
        close_per_second.append({k: v.copy() for k, v in current.items()})
    return open_per_second, close_per_second


def generate_events_for_second(
    ts,
    market_event_count,
    core_count,
    fx_pairs,
    open_state_for_second,
    close_state_for_second,
    sender,
    ladder,
    min_levels,
    max_levels,
    prebuilt_bid_arrays,
    prebuilt_ask_arrays,
    end_ns=None,
    suffix="",
    real_time=True
):
    # Prepare offsets and symbol picks
    offsets_market = sorted(random.randint(0, 999_999_999) for _ in range(market_event_count))
    symbol_choices = random.choices(fx_pairs, k=market_event_count)

    # Track how many events we generate per symbol for this second
    symbol_event_indices = {symbol: [] for symbol, *_ in fx_pairs}
    for i, (symbol, *_ ) in enumerate(symbol_choices):
        symbol_event_indices[symbol].append(i)

    for symbol, indices in symbol_event_indices.items():
        n_events = len(indices)
        if n_events == 0:
            continue
        for j, i in enumerate(indices):
            offset = offsets_market[i]
            _, low, high, precision, pip = [v for v in fx_pairs if v[0] == symbol][0]
            levels = random.randint(min_levels, max_levels)
            bids, asks = prebuilt_bid_arrays[levels - 1], prebuilt_ask_arrays[levels - 1]

            if j == 0:
                # First event = open
                state = open_state_for_second[symbol]
            elif j == n_events - 1:
                # Last event = close
                state = close_state_for_second[symbol]
            else:
                # Linear interpolate between open and close
                frac = j / (n_events - 1)
                state = {
                    "bid_price": open_state_for_second[symbol]["bid_price"] + frac * (close_state_for_second[symbol]["bid_price"] - open_state_for_second[symbol]["bid_price"]),
                    "ask_price": open_state_for_second[symbol]["ask_price"] + frac * (close_state_for_second[symbol]["ask_price"] - open_state_for_second[symbol]["ask_price"]),
                    "spread": open_state_for_second[symbol]["spread"],  # could interpolate if desired
                    "indicator1": open_state_for_second[symbol]["indicator1"],  # or interpolate
                    "indicator2": open_state_for_second[symbol]["indicator2"],
                }

            generate_bids_asks(bids, asks, levels, state["bid_price"], state["ask_price"], precision, pip, ladder)
            row_ts = ts + offset
            if end_ns is not None and row_ts >= end_ns:
                continue
            if real_time:
                at = TimestampNanos.now()
            else:
                TimestampNanos(row_ts)
            sender.row(
                table_name('market_data', suffix),
                symbols={"symbol": symbol},
                columns={"bids": bids, "asks": asks},
                at=at
            )

    # --- Core price events: use open state (or close, your call) ---
    offsets_core = sorted(random.randint(0, 999_999_999) for _ in range(core_count))
    symbol_choices_core = random.choices(fx_pairs, k=core_count)
    for i, offset in enumerate(offsets_core):
        symbol, low, high, precision, pip = symbol_choices_core[i]
        levels = random.randint(min_levels, max_levels)
        bids, asks = prebuilt_bid_arrays[levels - 1], prebuilt_ask_arrays[levels - 1]
        indicators = open_state_for_second[symbol]
        bid_price = open_state_for_second[symbol]["bid_price"]
        ask_price = open_state_for_second[symbol]["ask_price"]
        reason = random.choice(["normal", "news_event", "liquidity_event"])
        ecn = random.choice(["LMAX", "EBS", "Hotspot", "Currenex"])
        row_ts = ts + offset
        if end_ns is not None and row_ts >= end_ns:
            continue
        lvl = random.randint(0, levels - 1)
        generate_bids_asks(bids, asks, levels, bid_price, ask_price, precision, pip, ladder)
        if real_time:
                at = TimestampNanos.now()
        else:
            TimestampNanos(row_ts)
        sender.row(
            table_name('core_price', suffix),
            symbols={"symbol": symbol, "ecn": ecn, "reason": reason},
            columns={
                "bid_price": float(bids[0][lvl]),
                "bid_volume": int(bids[1][lvl]),
                "ask_price": float(asks[0][lvl]),
                "ask_volume": int(asks[1][lvl]),
                "indicator1": float(round(indicators["indicator1"], 3)),
                "indicator2": float(round(indicators["indicator2"], 3))
            },
            at=at
        )

def wait_if_paused(pause_event, process_idx):
    while pause_event.is_set():
        print(f"[WORKER {process_idx}] Paused due to WAL lag (waiting for sequencerTxn==writerTxn)...")
        time.sleep(5)

def ingest_worker(
    args,
    per_second_plan,
    total_events,
    start_ns,
    end_ns,
    global_states,
    fx_pairs,
    process_idx,
    processes,
    pause_event,
    global_sec_idx_offset # only meaningful in faster-than-life
):
    if args.mode=="real-time":
        auto_flush_interval=1000
    else:
        auto_flush_interval=10000

    if args.protocol == "http":
        conf = f"http::addr={args.host}:9000;auto_flush_interval={auto_flush_interval};" if not args.token else f"https::addr={args.host}:9000;username={args.ilp_user};token={args.token};tls_verify=unsafe_off;auto_flush_interval={auto_flush_interval};"
    else:
        conf = f"tcp::addr={args.host}:9009;protocol_version=2;auto_flush_interval={auto_flush_interval};" if not args.token else f"tcps::addr={args.host}:9009;username={args.ilp_user};token={args.token};token_x={args.token_x};token_y={args.token_y};tls_verify=unsafe_off;protocol_version=2;auto_flush_interval={auto_flush_interval};"

    prebuilt_bids = [np.zeros((2, lvl), dtype=np.float64) for lvl in range(1, args.max_levels + 1)]
    prebuilt_asks = [np.zeros((2, lvl), dtype=np.float64) for lvl in range(1, args.max_levels + 1)]
    ts = start_ns
    sent = 0
    ladder = make_ladder(args.max_levels)

    # <-- NEW: Track last close per symbol for continuity
    last_close_per_symbol = {}

    with Sender.from_conf(conf) as sender:
        wall_start = None  # for real-time alignment

        if args.mode == "faster-than-life":
            open_per_second, close_per_second = global_states
            for sec_idx, (market_total, core_total) in enumerate(per_second_plan):
                wait_if_paused(pause_event, process_idx)
                if (end_ns and ts >= end_ns) or (sent >= total_events):
                    break
                open_state = open_per_second[sec_idx]
                close_state = close_per_second[sec_idx]
                generate_events_for_second(
                    ts, market_total, core_total, fx_pairs,
                    open_state, close_state, sender, ladder,
                    args.min_levels, args.max_levels,
                    prebuilt_bids, prebuilt_asks,
                    end_ns, args.suffix, False
                )
                ts += int(1e9)
                sent += market_total

        elif args.mode=="real-time":
            # Real-time mode: infinite, build state as we go
            # Start with global_states as initial state (can be state or initial state dict)
            if isinstance(global_states, list):
                current_state = {k: v.copy() for k, v in global_states[0].items()}
            else:
                current_state = {k: v.copy() for k, v in global_states.items()}
            sec_idx = 0
            wall_start = time.time()
            last_refresh = time.time()

            while not (end_ns and ts >= end_ns) and (total_events == 0 or sent < total_events):
                fx_pairs_snapshot = list(fx_pairs)
                wait_if_paused(pause_event, process_idx)
                market_total = random.randint(args.market_data_min_eps, args.market_data_max_eps)
                core_total = random.randint(args.core_min_eps, args.core_max_eps)


                # At the beginning of the second: capture OPEN state
                open_state = {k: v.copy() for k, v in current_state.items()}
                # Evolve for close (to be used as interpolation target)
                close_state = evolve_state_one_tick(current_state, fx_pairs_snapshot, 7.0)

                generate_events_for_second(
                    ts, market_total, core_total, fx_pairs_snapshot,
                    open_state, close_state, sender, ladder,
                    args.min_levels, args.max_levels,
                    prebuilt_bids, prebuilt_asks,
                    end_ns, args.suffix, True
                )
                current_state = close_state
                sent += market_total
                ts += int(1e9)
                sec_idx += 1

                # Wall clock alignment
                next_tick = wall_start + sec_idx
                now = time.time()
                sleep_for = next_tick - now
                if sleep_for > 0:
                    time.sleep(sleep_for)

    # ... main event loop ends here ...
    # Print reason for exit
    if end_ns is not None and ts >= end_ns:
        print(f"[WORKER {process_idx}] Finished. Exiting because end_ts ({ns_to_iso(end_ns)}) was reached (last ts: {ns_to_iso(ts)}). Events sent from worker {sent} ")
    elif sent >= total_events:
        print(f"[WORKER {process_idx}] Finished. Exiting because total_market_data_events ({total_events}) was reached (last ts: {ns_to_iso(ts)}).")
    else:
        print(f"[WORKER {process_idx}] Finished. Exiting for unknown reason (last ts: {ns_to_iso(ts)}).")

    sys.exit(0)

def wal_monitor(args, pause_event, processes, interval=5,suffix=''):
    import time
    import psycopg as pg
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"
    threshold = 3 * processes
    last_logged_paused = False

    with pg.connect(conn_str, autocommit=True) as conn:
        while True:
            cur = conn.execute(f"SELECT sequencerTxn, writerTxn FROM wal_tables() WHERE name = '{table_name('market_data', suffix)}'")
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
                        time.sleep(interval)
                        print(f"[WAL MONITOR] Resuming ingestion: sequencerTxn={seq}, writerTxn={wrt}, lag={lag}")
                        pause_event.clear()
                        last_logged_paused = False
            time.sleep(interval)

def parse_ts_arg(ts):
    # Accepts 2025-07-11T14:00:00, 2025-07-11T14:00:00Z, or 2025-07-11T14:00:00+00:00 as UTC
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    else:
        dt = dt.astimezone(datetime.timezone.utc)
    return int(dt.timestamp() * 1e9)

def ns_to_iso(ns):
    dt = datetime.datetime.utcfromtimestamp(ns / 1e9)
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

def split_event_counts(total, num_workers):
    # Returns a list of event counts for each worker that sum to total, as even as possible
    base = total // num_workers
    remainder = total % num_workers
    return [base + (1 if i < remainder else 0) for i in range(num_workers)]


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
    parser.add_argument("--processes", type=int, default=1)
    parser.add_argument("--min_levels", type=int, default=40)
    parser.add_argument("--max_levels", type=int, default=40)
    parser.add_argument("--incremental", type=lambda x: str(x).lower() != 'false', default=False)
    parser.add_argument("--create_views", type=lambda x: str(x).lower() != 'false', default=True)
    parser.add_argument("--short_ttl", type=lambda x: str(x).lower() == 'true', default=False)
    parser.add_argument("--suffix", type=str, default="")
    parser.add_argument("--yahoo_refresh_secs", type=int, default=300)

    args = parser.parse_args()
    suffix = args.suffix

    # Parse ts ONCE
    if args.start_ts:
        start_ns = parse_ts_arg(args.start_ts)
    else:
        start_ns = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1e9)
    end_ns = parse_ts_arg(args.end_ts) if args.end_ts else None

    if args.min_levels > args.max_levels:
        print("ERROR: min_levels cannot be greater than max_levels.")
        exit(1)
    if args.mode == "real-time" and args.processes != 1:
        print("ERROR: real-time mode only supports processes=1 (no parallelism in wallclock mode).")
        exit(1)
    if args.mode == "real-time" and args.start_ts:
        print("ERROR: --start_ts is not allowed in real-time mode.")
        exit(1)
    if args.mode == "faster-than-life":
        if not args.total_market_data_events or args.total_market_data_events <= 0:
            print("ERROR: --total_market_data_events must be set to a positive integer in faster-than-life mode.")
            exit(1)

    ensure_tables_exist(args, suffix)
    if args.create_views:
        ensure_materialized_views_exist(args, suffix)

    # Connect and get latest timestamps
    conn_str = f"user={args.user} password={args.password} host={args.host} port={args.pg_port} dbname=qdb"
    with pg.connect(conn_str) as conn:
        latest_market_ns = get_latest_timestamp_ns(conn, table_name('market_data', suffix))
        latest_core_ns = get_latest_timestamp_ns(conn, table_name('core_price', suffix))

    max_latest_ns = max(x for x in [latest_market_ns, latest_core_ns] if x is not None) if (latest_market_ns is not None or latest_core_ns is not None) else None

    if args.mode == "faster-than-life":
        if max_latest_ns is not None:
            next_ns = max_latest_ns + 1_000  # 1 microsecond
            if next_ns > start_ns:
                print(f"[INFO] Advancing start_ns from {ns_to_iso(start_ns)} to {ns_to_iso(next_ns)} to avoid overlap with existing data.")
                start_ns = next_ns



            if end_ns is not None and start_ns >= end_ns:
                print(f"[INFO] All data in requested range is already present. Exiting.")
                exit(0)

            if end_ns is not None:
                available_seconds = (end_ns - start_ns) // 1_000_000_000
                if available_seconds <= 0:
                    print(f"[INFO] No available interval after advancing start_ns.")
                    print("Exiting.")
                    exit(0)
                elif available_seconds < 5:
                    print(f"[WARN] Only {available_seconds} seconds available for event generation. Very little data will be generated.")

        if end_ns is not None and start_ns >= end_ns:
            print(f"[INFO] No work to do: start_ts ({ns_to_iso(start_ns)}) >= end_ts ({ns_to_iso(end_ns)})")
            print("Exiting.")
            exit(0)

        if end_ns and max_latest_ns is not None and end_ns <= max_latest_ns:
            print(f"[ERROR] Provided --end_ts ({args.end_ts}) is earlier than the most recent data in tables ({ns_to_iso(max_latest_ns)}). Aborting.")
            exit(1)

    elif args.mode == "real-time":
        now_ns = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1e9)
        if max_latest_ns is not None:
            next_ns = max_latest_ns + 1_000  # 1 microsecond after last row
            if next_ns > now_ns:
                wait_seconds = (next_ns - now_ns) / 1e9
                print(f"[INFO] Last row in DB is {ns_to_iso(max_latest_ns)}. Waiting {wait_seconds:.1f}s to avoid overlap...")
                time.sleep(wait_seconds)
        # In real-time mode, always use wallclock, so start_ns is always "now"
        start_ns = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1e9)
        if end_ns is not None and start_ns >= end_ns:
            print(f"[INFO] No work to do: wallclock now ({ns_to_iso(start_ns)}) >= end_ts ({ns_to_iso(end_ns)})")
            print("Exiting.")
            exit(0)

    # Build state
    manager = mp.Manager()
    fx_pairs = manager.list(FX_PAIRS)
    refresher_proc = None

    if args.incremental:
        if args.mode == "real-time":
            print(f"[ERROR] Incremental load not allowed in real-time, as real-time syncs from yahoo finance.")
            exit(1)
        else:
            fx_pairs = FX_PAIRS
            state = load_initial_state(args, suffix)
    else:
        if args.mode == "real-time":
            first_ready_event = mp.Event()
            # Start the refresher
            refresher_proc = mp.Process(target=fx_pairs_refresher, args=(fx_pairs, args.yahoo_refresh_secs),
                                        kwargs={'first_ready_event': first_ready_event})
            refresher_proc.daemon = True
            refresher_proc.start()
            print("[INFO] Waiting for Yahoo FX brackets initial load...", flush=True)
            first_ready_event.wait()
            print("[INFO] FX brackets loaded from Yahoo. Proceeding with ingestion.", flush=True)
            # Initial state is built from the initial brackets
            state = load_initial_state_from_brackets(list(fx_pairs))
        else:
            fx_pairs = fetch_fx_pairs_from_yahoo(FX_PAIRS, bracket_pct=1.0)
            state = load_initial_state_from_brackets(fx_pairs)

    pause_event = Event()
    wal_proc = mp.Process(
        target=wal_monitor,
        args=(args, pause_event, args.processes),
        kwargs={'suffix': suffix}
    )
    wal_proc.start()

    if args.mode == "faster-than-life":
        per_second_plan = []
        events_so_far = 0
        while events_so_far < args.total_market_data_events:
            market_total = random.randint(args.market_data_min_eps, args.market_data_max_eps)
            core_total = random.randint(args.core_min_eps, args.core_max_eps)
            per_second_plan.append((market_total, core_total))
            events_so_far += market_total
        overage = events_so_far - args.total_market_data_events
        if overage > 0:
            market_total, core_total = per_second_plan[-1]
            per_second_plan[-1] = (market_total - overage, core_total)
        total_seconds = len(per_second_plan)
        worker_plans = [[] for _ in range(args.processes)]
        for sec_idx, (market_total, core_total) in enumerate(per_second_plan):
            market_splits = split_event_counts(market_total, args.processes)
            core_splits = split_event_counts(core_total, args.processes)
            for i in range(args.processes):
                worker_plans[i].append((market_splits[i], core_splits[i]))
        open_per_second, close_per_second = precompute_open_close_state(total_seconds, fx_pairs, state)
        global_states = (open_per_second, close_per_second)
        global_sec_offsets = []
        cum = 0
        for plan in worker_plans:
            global_sec_offsets.append(cum)
            cum += len(plan)

        if end_ns is not None and start_ns >= end_ns:
            print(f"[INFO] No work to do: start_ts ({ns_to_iso(start_ns)}) >= end_ts ({ns_to_iso(end_ns)})")
            print("Exiting.")
            exit(0)

        pool = []
        for process_idx in range(args.processes):
            p = mp.Process(
                target=ingest_worker,
                args=(
                    args,
                    worker_plans[process_idx],
                    sum(market for market, _ in worker_plans[process_idx]),
                    start_ns,
                    end_ns,
                    global_states,
                    fx_pairs,
                    process_idx,
                    args.processes,
                    pause_event,
                    global_sec_offsets[process_idx]
                )
            )
            p.start()
            pool.append(p)
        for p in pool:
            p.join()
    elif args.mode == "real-time":
        p = mp.Process(
            target=ingest_worker,
            args=(
                args,
                None,  # No per-second plan
                args.total_market_data_events,
                start_ns,
                end_ns,
                state,  # Only initial state
                fx_pairs,
                0,  # process_idx
                1,  # processes
                pause_event,
                0 # not needed for real-time
            )
        )
        p.start()
        p.join()

    wal_proc.terminate()
    wal_proc.join()

    if refresher_proc is not None:
        refresher_proc.terminate()
        refresher_proc.join()

if __name__ == "__main__":
    main()
