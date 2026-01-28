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
import hashlib
import uuid

FX_PAIRS = [
    # (symbol, low, high, precision, pip, rank)
    # Rank 1-9: Most liquid pairs, Rank 10: Others
    ("EURUSD", 1.05, 1.10, 5, 0.0001, 1),   # Most liquid
    ("USDJPY", 150.0, 155.0, 3, 0.01, 2),
    ("GBPUSD", 1.25, 1.30, 5, 0.0001, 3),   # "Cable"
    ("USDCHF", 0.90, 0.95, 4, 0.0001, 4),   # "Swissy"
    ("AUDUSD", 0.65, 0.70, 5, 0.0001, 5),   # Commodity currency
    ("USDCAD", 1.35, 1.40, 5, 0.0001, 6),   # Oil-linked
    ("EURGBP", 0.85, 0.88, 5, 0.0001, 7),   # Major cross
    ("EURJPY", 160.0, 165.0, 3, 0.01, 8),   # Popular cross
    ("NZDUSD", 0.60, 0.65, 5, 0.0001, 9),   # Commodity-linked
    # Less liquid pairs (rank 10)
    ("GBPJPY", 180.0, 185.0, 3, 0.01, 10),
    ("EURCHF", 0.95, 1.00, 4, 0.0001, 10),
    ("EURAUD", 1.55, 1.60, 5, 0.0001, 10),
    ("GBPCHF", 1.10, 1.15, 4, 0.0001, 10),
    ("AUDJPY", 100.0, 105.0, 3, 0.01, 10),
    ("NZDJPY", 95.0, 100.0, 3, 0.01, 10),
    ("USDSEK", 10.0, 11.0, 4, 0.0001, 10),
    ("USDNOK", 10.0, 11.0, 4, 0.0001, 10),
    ("USDMXN", 17.0, 18.0, 4, 0.0001, 10),
    ("USDSGD", 1.35, 1.40, 5, 0.0001, 10),
    ("USDHKD", 7.75, 7.85, 4, 0.0001, 10),
    ("USDZAR", 18.0, 19.0, 4, 0.0001, 10),
    ("USDTRY", 27.0, 28.0, 4, 0.0001, 10),
    ("EURCAD", 1.45, 1.50, 5, 0.0001, 10),
    ("EURNZD", 1.70, 1.75, 5, 0.0001, 10),
    ("GBPAUD", 1.85, 1.90, 5, 0.0001, 10),
    ("GBPNZD", 2.00, 2.05, 5, 0.0001, 10),
    ("AUDCAD", 0.85, 0.90, 5, 0.0001, 10),
    ("AUDNZD", 1.05, 1.10, 5, 0.0001, 10),
    ("NZDCAD", 0.80, 0.85, 5, 0.0001, 10),
    ("CADJPY", 110.0, 115.0, 3, 0.01, 10)
]


def make_ladder(levels=50, v_min=100_000, v_max=1_000_000_000):
    step = (math.log10(v_max) - math.log10(v_min)) / (levels - 1)
    return [int(round(10 ** (math.log10(v_min) + i * step))) for i in range(levels)]


def table_name(name, suffix):
    return name + suffix if suffix else name


def generate_lei_pool(count):
    """
    Generate deterministic fake LEIs with realistic format.
    LEI format: 20 alphanumeric characters.
    Using hash-based generation for realism while maintaining determinism.
    Same count always produces same LEIs; expanding count preserves existing LEIs.
    """
    leis = []
    for i in range(count):
        # Create deterministic hash from index
        hash_input = f"LEI_SEED_{i:010d}".encode('utf-8')
        hash_hex = hashlib.sha256(hash_input).hexdigest()
        # Take first 18 chars of hex, add "00" prefix for structure
        lei = "00" + hash_hex[:18].upper()
        leis.append(lei)
    return leis


def generate_trade_size_lognormal(min_size=100_000, median_size=500_000, max_size=100_000_000):
    """
    Generate realistic trade sizes using log-normal distribution.
    Many small trades, few large trades.
    """
    # Log-normal parameters to hit our targets
    mu = math.log(median_size)
    sigma = 1.2  # Controls spread

    size = random.lognormvariate(mu, sigma)
    # Clamp to realistic bounds
    return int(max(min_size, min(max_size, size)))


def generate_order(symbol, best_bid, best_ask, pip, rank, ecn):
    """
    Generate a realistic order with rank-weighted probability.

    Args:
        symbol: FX pair symbol
        best_bid: Current best bid price
        best_ask: Current best ask price
        pip: Pip size for the symbol
        rank: Liquidity rank (1=most liquid, 10=least liquid)
        ecn: ECN/venue where order is placed (LMAX, EBS, Hotspot, Currenex)

    Returns:
        dict with order_id, symbol, side, quantity, limit_price, passive, ecn
    """
    order_id = str(uuid.uuid4())

    # Side: 50/50 buy vs sell
    side = random.choice(["buy", "sell"])

    # Passive: ~40% of orders are passive (resting limit orders)
    passive = random.random() < 0.4

    # Quantity: log-normal distribution
    quantity = generate_trade_size_lognormal()

    # Limit price depends on passive vs aggressive
    if passive:
        # Passive orders: crosses spread but with limited slippage
        if side == "buy":
            # Passive buy: willing to pay slightly above best ask
            limit_price = best_ask + random.randint(0, 2) * pip
        else:
            # Passive sell: willing to accept slightly below best bid
            limit_price = best_bid - random.randint(0, 2) * pip
    else:
        # Aggressive orders: willing to walk deeper into the book
        if side == "buy":
            # Aggressive buy: willing to pay much more above ask
            max_slippage = random.randint(3, 10)
            limit_price = best_ask + max_slippage * pip
        else:
            # Aggressive sell: willing to accept much less below bid
            max_slippage = random.randint(3, 10)
            limit_price = best_bid - max_slippage * pip

    return {
        "order_id": order_id,
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "limit_price": limit_price,
        "passive": passive,
        "ecn": ecn
    }


def execute_order_against_orderbook(order, bids, asks, levels, pip):
    """
    Execute an order against the orderbook, generating trades.

    Args:
        order: dict with keys: order_id, symbol, side, quantity, limit_price, passive
        bids: numpy array [prices, volumes] for bid side
        asks: numpy array [prices, volumes] for ask side
        levels: number of levels in the book
        pip: pip size for the symbol

    Returns:
        list of trade dicts, each with: order_id, side, price, quantity, passive
    """
    trades = []
    remaining = order["quantity"]
    side = order["side"]
    limit_price = order["limit_price"]
    passive = order["passive"]

    if side == "buy":
        # Buy order: execute against asks (walk up the ask side)
        for lvl in range(levels):
            if remaining <= 0:
                break
            ask_price = asks[0][lvl]
            ask_volume = asks[1][lvl]

            # Check limit price constraint
            if ask_price > limit_price:
                break

            # Execute what we can at this level
            trade_qty = min(remaining, ask_volume)
            trades.append({
                "order_id": order["order_id"],
                "side": side,
                "price": float(ask_price),
                "quantity": float(trade_qty),
                "passive": passive
            })
            remaining -= trade_qty

    else:  # side == "sell"
        # Sell order: execute against bids (walk down the bid side)
        for lvl in range(levels):
            if remaining <= 0:
                break
            bid_price = bids[0][lvl]
            bid_volume = bids[1][lvl]

            # Check limit price constraint
            if bid_price < limit_price:
                break

            # Execute what we can at this level
            trade_qty = min(remaining, bid_volume)
            trades.append({
                "order_id": order["order_id"],
                "side": side,
                "price": float(bid_price),
                "quantity": float(trade_qty),
                "passive": passive
            })
            remaining -= trade_qty

    return trades

class SortedEmitter:
    """
    Buffers rows for market_data, core_price, and fx_trades, sorts them by timestamp,
    and writes them out in order.
    """

    def __init__(self, sender, buffer_limit, suffix):
        self.sender = sender
        self.buffer_limit = buffer_limit
        self.suffix = suffix
        self._md = []   # market_data buffer
        self._cp = []   # core_price buffer
        self._tr = []   # fx_trades buffer

    # --- Market data rows --- #
    def emit_market(self, ts_ns, symbol, bids, asks):
        self._md.append({
            "ts": ts_ns,
            "symbol": symbol,
            "bids": bids.copy(),   # numpy copy
            "asks": asks.copy()
        })
        if len(self._md) >= self.buffer_limit:
            self._flush_md()

    # --- Core price rows --- #
    def emit_core(self, ts_ns, symbol, ecn, reason, columns):
        self._cp.append({
            "ts": ts_ns,
            "symbol": symbol,
            "ecn": ecn,
            "reason": reason,
            "columns": columns,
        })
        if len(self._cp) >= self.buffer_limit:
            self._flush_cp()

    # --- Trade rows --- #
    def emit_trade(self, ts_ns, symbol, ecn, trade_id, side, passive, price, quantity, counterparty, order_id):
        self._tr.append({
            "ts": ts_ns,
            "symbol": symbol,
            "ecn": ecn,
            "trade_id": trade_id,
            "side": side,
            "passive": passive,
            "price": price,
            "quantity": quantity,
            "counterparty": counterparty,
            "order_id": order_id,
        })
        if len(self._tr) >= self.buffer_limit:
            self._flush_tr()

    # --- Flush methods --- #
    def _flush_md(self):
        if not self._md:
            return
        self._md.sort(key=lambda r: r["ts"])
        tbl = table_name("market_data", self.suffix)
        for r in self._md:
            self.sender.row(
                tbl,
                symbols={"symbol": r["symbol"]},
                columns={"bids": r["bids"], "asks": r["asks"]},
                at=TimestampNanos(r["ts"])
            )
        self.sender.flush()
        self._md.clear()

        # Also flush core_price and fx_trades to keep all tables synchronized
        self._flush_cp()
        self._flush_tr()

    def _flush_cp(self):
        if not self._cp:
            return
        self._cp.sort(key=lambda r: r["ts"])
        tbl = table_name("core_price", self.suffix)
        for r in self._cp:
            self.sender.row(
                tbl,
                symbols={"symbol": r["symbol"], "ecn": r["ecn"], "reason": r["reason"]},
                columns=r["columns"],
                at=TimestampNanos(r["ts"])
            )
        self.sender.flush()
        self._cp.clear()

    def _flush_tr(self):
        if not self._tr:
            return
        self._tr.sort(key=lambda r: r["ts"])
        tbl = table_name("fx_trades", self.suffix)
        for r in self._tr:
            self.sender.row(
                tbl,
                symbols={"symbol": r["symbol"], "ecn": r["ecn"], "side": r["side"], "counterparty": r["counterparty"]},
                columns={
                    "trade_id": r["trade_id"],
                    "passive": r["passive"],
                    "price": r["price"],
                    "quantity": r["quantity"],
                    "order_id": r["order_id"],
                },
                at=TimestampNanos(r["ts"])
            )
        self.sender.flush()
        self._tr.clear()

    # --- flush all before exit --- #
    def flush_all(self):
        self._flush_md()
        self._flush_cp()
        self._flush_tr()


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
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name('fx_trades', suffix)} (
            timestamp TIMESTAMP_NS,
            symbol SYMBOL CAPACITY 15000,
            ecn SYMBOL,
            trade_id UUID,
            side SYMBOL,
            passive BOOLEAN,
            price DOUBLE,
            quantity DOUBLE,
            counterparty SYMBOL,
            order_id UUID
        ) timestamp(timestamp) PARTITION BY HOUR {'TTL 1 MONTH' if short_ttl else ''} DEDUP UPSERT KEYS(timestamp, trade_id);
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
        ) PARTITION BY HOUR {'TTL 1 DAY' if short_ttl else ''};
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
        ) PARTITION BY HOUR {'TTL 2 DAYS' if short_ttl else ''};
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

        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('fx_trades_ohlc_1m', suffix)} AS (
            SELECT timestamp, symbol,
                first(price) AS open,
                max(price) AS high,
                min(price) AS low,
                last(price) AS close,
                SUM(quantity) AS total_volume
            FROM {table_name('fx_trades', suffix)}
            SAMPLE BY 1m
        ) PARTITION BY HOUR {'TTL 2 DAYS' if short_ttl else ''};
        """)

        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('fx_trades_ohlc_1d', suffix)} REFRESH EVERY 1h DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                first(open) AS open,
                max(high) AS high,
                min(low) AS low,
                last(close) AS close,
                SUM(total_volume) AS total_volume
            FROM {table_name('fx_trades_ohlc_1m', suffix)}
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
    for symbol, low, high, precision, pip, rank in fx_pairs:
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
    for symbol, _low, _high, precision, pip, rank in fx_pairs_template:
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
        out.append((symbol, low, high, precision, pip, rank))
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
    for symbol, low, high, precision, pip, rank in fx_pairs:
        prev_mid = (prev_state[symbol]["bid_price"] + prev_state[symbol]["ask_price"]) / 2
        prev_spread = prev_state[symbol]["spread"]
        new_mid = evolve_mid_price(prev_mid, low, high, precision, pip, drift=drift)

        # Work in pips to avoid the tiny deltas disappearing on quantize
        spread_pips = int(round(prev_spread / pip))

        # Small random walk in pips, mostly flat, sometimes tighter or wider
        spread_pips += random.choice([-1, 0, 0, 0, 1])

        # Rare stress widening
        if random.random() < 0.0005:  # tweak this probability for more or less frequency
            spread_pips += random.randint(3, 10)

        # Keep spreads in a sane range per pair
        # Majors: 1 to 8 pips
        min_pips = 1
        max_pips = 8
        spread_pips = max(min_pips, min(max_pips, spread_pips))

        new_spread = spread_pips * pip

        # new_spread = quantize_to_pip(max(pip, prev_spread + random.uniform(-0.2 * pip, 0.2 * pip)), pip)
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
    emitter,
    ladder,
    min_levels,
    max_levels,
    prebuilt_bid_arrays,
    prebuilt_ask_arrays,
    end_ns=None,
    suffix="",
    real_time=True,
    orders_count=0,
    lei_pool=None
):
    """
    Generate events for a single second:
    - market_data: Many events (full depth orderbook updates)
    - core_price: Fewer events (BBO snapshots with indicators)
    - fx_trades: Execute against orderbooks, timestamped after corresponding core_price
    """
    # in real_time, we always ingest a couple of seconds ahead, to account for wal apply time, so dashboards look more
    # dynamic in real-time
    if real_time:
        ts = int((datetime.datetime.now(datetime.timezone.utc).timestamp() * 1e9) + 2*1e9)

    # ECN pool
    ecn_pool = ["LMAX", "EBS", "Hotspot", "Currenex"]

    # --- Generate market_data events (LOTS of them - orderbook updates) ---
    offsets_market = sorted(random.randint(0, 999_999_999) for _ in range(market_event_count))
    symbol_choices_market = random.choices(fx_pairs, k=market_event_count)

    # Track how many events per symbol for interpolation
    symbol_event_indices = {symbol: [] for symbol, *_ in fx_pairs}
    for i, (symbol, *_) in enumerate(symbol_choices_market):
        symbol_event_indices[symbol].append(i)

    for symbol, indices in symbol_event_indices.items():
        n_events = len(indices)
        if n_events == 0:
            continue
        for j, i in enumerate(indices):
            offset = offsets_market[i]
            _, low, high, precision, pip, rank = [v for v in fx_pairs if v[0] == symbol][0]
            levels = random.randint(min_levels, max_levels)
            bids, asks = prebuilt_bid_arrays[levels - 1], prebuilt_ask_arrays[levels - 1]

            if j == 0:
                state = open_state_for_second[symbol]
            elif j == n_events - 1:
                state = close_state_for_second[symbol]
            else:
                frac = j / (n_events - 1)
                state = {
                    "bid_price": open_state_for_second[symbol]["bid_price"] + frac * (close_state_for_second[symbol]["bid_price"] - open_state_for_second[symbol]["bid_price"]),
                    "ask_price": open_state_for_second[symbol]["ask_price"] + frac * (close_state_for_second[symbol]["ask_price"] - open_state_for_second[symbol]["ask_price"]),
                    "spread": open_state_for_second[symbol]["spread"],
                    "indicator1": open_state_for_second[symbol]["indicator1"],
                    "indicator2": open_state_for_second[symbol]["indicator2"],
                }

            generate_bids_asks(bids, asks, levels, state["bid_price"], state["ask_price"], precision, pip, ladder)
            row_ts = ts + offset
            if end_ns is not None and row_ts >= end_ns:
                continue

            emitter.emit_market(row_ts, symbol, bids, asks)

    # --- Generate core_price events (FEWER - BBO snapshots with metadata) ---
    offsets_core = sorted(random.randint(0, 999_999_999) for _ in range(core_count))
    symbol_choices_core = random.choices(fx_pairs, k=core_count)

    # Track generated core_price events for trade generation
    core_price_events = []

    # Group core_price events by symbol for interpolation (same as market_data)
    symbol_core_indices = {symbol: [] for symbol, *_ in fx_pairs}
    for i, (symbol, *_) in enumerate(symbol_choices_core):
        symbol_core_indices[symbol].append(i)

    for symbol, indices in symbol_core_indices.items():
        n_events = len(indices)
        if n_events == 0:
            continue

        _, low, high, precision, pip, rank = [v for v in fx_pairs if v[0] == symbol][0]

        for j, i in enumerate(indices):
            offset = offsets_core[i]
            levels = random.randint(min_levels, max_levels)
            bids, asks = prebuilt_bid_arrays[levels - 1], prebuilt_ask_arrays[levels - 1]

            # Interpolate bid/ask prices between open and close states
            if j == 0:
                state = open_state_for_second[symbol]
            elif j == n_events - 1:
                state = close_state_for_second[symbol]
            else:
                frac = j / (n_events - 1)
                state = {
                    "bid_price": open_state_for_second[symbol]["bid_price"] + frac * (close_state_for_second[symbol]["bid_price"] - open_state_for_second[symbol]["bid_price"]),
                    "ask_price": open_state_for_second[symbol]["ask_price"] + frac * (close_state_for_second[symbol]["ask_price"] - open_state_for_second[symbol]["ask_price"]),
                    "spread": open_state_for_second[symbol]["spread"],
                    "indicator1": open_state_for_second[symbol]["indicator1"],
                    "indicator2": open_state_for_second[symbol]["indicator2"],
                }

            reason = random.choice(["normal", "news_event", "liquidity_event"])
            ecn = random.choice(ecn_pool)
            row_ts = ts + offset
            if end_ns is not None and row_ts >= end_ns:
                continue

            generate_bids_asks(bids, asks, levels, state["bid_price"], state["ask_price"], precision, pip, ladder)

            # Core price always uses level 0 (best bid/offer)
            emitter.emit_core(
                row_ts,
                symbol,
                ecn,
                reason,
                {
                    "bid_price": float(bids[0][0]),  # Level 0 = Best bid
                    "bid_volume": int(bids[1][0]),
                    "ask_price": float(asks[0][0]),  # Level 0 = Best ask
                    "ask_volume": int(asks[1][0]),
                    "indicator1": float(round(state["indicator1"], 3)),
                    "indicator2": float(round(state["indicator2"], 3))
                }
            )

            # Store this core_price event for potential trade generation
            core_price_events.append({
                "timestamp": row_ts,
                "symbol": symbol,
                "ecn": ecn,
                "bids": bids.copy(),
                "asks": asks.copy(),
                "levels": levels,
                "pip": pip,
                "rank": rank
            })

    # --- Generate trades from core_price events ---
    if orders_count > 0 and lei_pool and core_price_events:
        # Cap orders to available core_price events to avoid excessive duplication
        actual_orders_count = min(orders_count, len(core_price_events))

        # Select exactly actual_orders_count events, weighted by liquidity rank
        # Lower rank = more liquid = higher weight
        weights = [(11 - cp_event["rank"]) for cp_event in core_price_events]
        selected_events = random.choices(core_price_events, weights=weights, k=actual_orders_count)

        for cp_event in selected_events:
            # Check if we have room for trades within this second
            offset_within_second = cp_event["timestamp"] % 1_000_000_000
            max_offset = 999_999_999 - offset_within_second

            # Skip this entire order if too close to second boundary (less than 1 microsecond room)
            if max_offset < 1_000:
                continue

            # Generate order for this ECN/symbol
            order = generate_order(
                cp_event["symbol"],
                float(cp_event["bids"][0][0]),
                float(cp_event["asks"][0][0]),
                cp_event["pip"],
                cp_event["rank"],
                cp_event["ecn"]
            )

            # Execute order
            trades = execute_order_against_orderbook(
                order,
                cp_event["bids"],
                cp_event["asks"],
                cp_event["levels"],
                cp_event["pip"]
            )

            # Emit trades with timestamps AFTER the core_price event
            # but within the same second to maintain temporal ordering
            for trade in trades:
                # Generate trade timestamp within same second
                trade_ts = cp_event["timestamp"] + random.randint(1_000, min(1_000_000, max_offset))
                if end_ns is not None and trade_ts >= end_ns:
                    continue

                counterparty = random.choice(lei_pool)
                emitter.emit_trade(
                    trade_ts,
                    cp_event["symbol"],
                    cp_event["ecn"],
                    str(uuid.uuid4()),
                    trade["side"],
                    trade["passive"],
                    trade["price"],
                    trade["quantity"],
                    counterparty,
                    trade["order_id"]
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
    global_sec_idx_offset, # only meaningful in faster-than-life
    lei_pool
):
    if args.mode=="real-time":
        buffer_limit=1000
    else:
        buffer_limit=10000

    auto_flush_interval = buffer_limit * 2  # safety net in the ILP client

    if args.protocol == "http":
        conf = f"http::addr={args.host}:9000;auto_flush_interval={auto_flush_interval};" if not args.token else f"https::addr={args.host}:9000;token={args.token};tls_verify=unsafe_off;auto_flush_interval={auto_flush_interval};"
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
        emitter = SortedEmitter(sender, buffer_limit=buffer_limit, suffix=args.suffix)
        wall_start = None  # for real-time alignment

        if args.mode == "faster-than-life":
            open_per_second, close_per_second = global_states
            for sec_idx, (market_total, core_total) in enumerate(per_second_plan):
                wait_if_paused(pause_event, process_idx)

                open_state = open_per_second[sec_idx]
                close_state = close_per_second[sec_idx]
                orders_total = random.randint(args.orders_min_per_sec, args.orders_max_per_sec)
                generate_events_for_second(
                    ts, market_total, core_total, fx_pairs,
                    open_state, close_state, emitter, ladder,
                    args.min_levels, args.max_levels,
                    prebuilt_bids, prebuilt_asks,
                    end_ns, args.suffix, False,
                    orders_total, lei_pool
                )
                sent += market_total

                if (end_ns and ts >= end_ns) or (sent >= total_events):
                    emitter.flush_all()
                    break

                ts += int(1e9)


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
                orders_total = random.randint(args.orders_min_per_sec, args.orders_max_per_sec)


                # At the beginning of the second: capture OPEN state
                open_state = {k: v.copy() for k, v in current_state.items()}
                # Evolve for close (to be used as interpolation target)
                close_state = evolve_state_one_tick(current_state, fx_pairs_snapshot, 7.0)

                generate_events_for_second(
                    ts, market_total, core_total, fx_pairs_snapshot,
                    open_state, close_state, emitter, ladder,
                    args.min_levels, args.max_levels,
                    prebuilt_bids, prebuilt_asks,
                    end_ns, args.suffix, True,
                    orders_total, lei_pool
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
            emitter.flush_all()

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
    parser.add_argument("--market_data_min_eps", type=int, default=1200)
    parser.add_argument("--market_data_max_eps", type=int, default=15000)
    parser.add_argument("--core_min_eps", type=int, default=700)
    parser.add_argument("--core_max_eps", type=int, default=1000)
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
    parser.add_argument("--orders_min_per_sec", type=int, default=5)
    parser.add_argument("--orders_max_per_sec", type=int, default=30)
    parser.add_argument("--lei_pool_size", type=int, default=2000)
    parser.add_argument("--chunk_seconds", type=int, default=900,
                        help="Max seconds to precompute at once in faster-than-life mode (default: 900 = 15 min)")

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

    # Validate event rate hierarchy: market_data > core_price > orders
    if args.market_data_min_eps <= args.core_max_eps:
        print(f"ERROR: market_data_min_eps ({args.market_data_min_eps}) must be greater than core_max_eps ({args.core_max_eps}).")
        print("Market data events should always be more frequent than core price events.")
        exit(1)
    if args.core_min_eps <= args.orders_max_per_sec:
        print(f"ERROR: core_min_eps ({args.core_min_eps}) must be greater than orders_max_per_sec ({args.orders_max_per_sec}).")
        print("Core price events should always be more frequent than orders per second.")
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
    refresher_proc = None
    if args.mode == "real-time":
        # Real-time uses a shared Manager list updated by Yahoo refresher
        fx_pairs = manager.list(FX_PAIRS)
    else:
        # Faster-than-life uses a plain list (no Manager, no refresher)
        fx_pairs = list(FX_PAIRS)

    # Generate LEI pool for counterparties (deterministic and reusable)
    lei_pool = generate_lei_pool(args.lei_pool_size)
    print(f"[INFO] Generated {len(lei_pool)} LEIs for counterparties.")


    if args.incremental:
        if args.mode == "real-time":
            print(f"[ERROR] Incremental load not allowed in real-time, as real-time syncs from yahoo finance.")
            exit(1)
        else:
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
        # Calculate max seconds based on time window to avoid unbounded precomputation
        max_seconds_from_window = None
        if end_ns is not None:
            max_seconds_from_window = (end_ns - start_ns) // 1_000_000_000
            if max_seconds_from_window <= 0:
                print(f"[INFO] No work to do: time window is zero or negative.")
                exit(0)

        # Build full per_second_plan (just tuples - low memory)
        full_per_second_plan = []
        events_so_far = 0
        while events_so_far < args.total_market_data_events:
            # Stop if we've hit the time window limit
            if max_seconds_from_window is not None and len(full_per_second_plan) >= max_seconds_from_window:
                print(f"[INFO] Reached end_ts limit at {len(full_per_second_plan)} seconds ({events_so_far} events). "
                      f"Requested {args.total_market_data_events} events but time window only allows {events_so_far}.")
                break
            market_total = random.randint(args.market_data_min_eps, args.market_data_max_eps)
            core_total = random.randint(args.core_min_eps, args.core_max_eps)
            full_per_second_plan.append((market_total, core_total))
            events_so_far += market_total
        overage = events_so_far - args.total_market_data_events
        if overage > 0 and (max_seconds_from_window is None or len(full_per_second_plan) < max_seconds_from_window):
            market_total, core_total = full_per_second_plan[-1]
            full_per_second_plan[-1] = (market_total - overage, core_total)

        total_seconds = len(full_per_second_plan)
        if total_seconds == 0:
            print(f"[INFO] No work to do: zero seconds in plan.")
            exit(0)

        # Process in chunks to limit memory usage
        chunk_size = args.chunk_seconds
        num_chunks = (total_seconds + chunk_size - 1) // chunk_size
        print(f"[INFO] Processing {total_seconds} seconds in {num_chunks} chunk(s) of up to {chunk_size} seconds each.")

        carry_forward_state = state  # Initial state for first chunk
        chunk_start_ns = start_ns

        for chunk_idx in range(num_chunks):
            chunk_start_sec = chunk_idx * chunk_size
            chunk_end_sec = min((chunk_idx + 1) * chunk_size, total_seconds)
            chunk_seconds = chunk_end_sec - chunk_start_sec

            # Slice the plan for this chunk
            chunk_plan = full_per_second_plan[chunk_start_sec:chunk_end_sec]
            chunk_events = sum(market for market, _ in chunk_plan)

            print(f"[CHUNK {chunk_idx + 1}/{num_chunks}] Seconds {chunk_start_sec}-{chunk_end_sec - 1}, "
                  f"{chunk_events} market_data events, starting at {ns_to_iso(chunk_start_ns)}")

            # Precompute state for this chunk only (using carry_forward_state for continuity)
            open_per_second, close_per_second = precompute_open_close_state(
                chunk_seconds, fx_pairs, carry_forward_state
            )
            global_states = (open_per_second, close_per_second)

            # Build worker plans for this chunk
            worker_plans = [[] for _ in range(args.processes)]
            for sec_idx, (market_total, core_total) in enumerate(chunk_plan):
                market_splits = split_event_counts(market_total, args.processes)
                core_splits = split_event_counts(core_total, args.processes)
                for i in range(args.processes):
                    worker_plans[i].append((market_splits[i], core_splits[i]))

            global_sec_offsets = []
            cum = 0
            for plan in worker_plans:
                global_sec_offsets.append(cum)
                cum += len(plan)

            # Calculate chunk end timestamp
            chunk_end_ns = chunk_start_ns + chunk_seconds * 1_000_000_000
            effective_end_ns = min(chunk_end_ns, end_ns) if end_ns else chunk_end_ns

            # Spawn workers for this chunk
            pool = []
            for process_idx in range(args.processes):
                p = mp.Process(
                    target=ingest_worker,
                    args=(
                        args,
                        worker_plans[process_idx],
                        sum(market for market, _ in worker_plans[process_idx]),
                        chunk_start_ns,
                        effective_end_ns,
                        global_states,
                        fx_pairs,
                        process_idx,
                        args.processes,
                        pause_event,
                        global_sec_offsets[process_idx],
                        lei_pool
                    )
                )
                p.start()
                pool.append(p)

            # Wait for all workers to finish this chunk
            for p in pool:
                p.join()

            # Carry forward the final close state for continuity with next chunk
            carry_forward_state = close_per_second[-1]

            # Update start timestamp for next chunk
            chunk_start_ns = chunk_end_ns

            print(f"[CHUNK {chunk_idx + 1}/{num_chunks}] Completed.")
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
                0,  # not needed for real-time
                lei_pool
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
