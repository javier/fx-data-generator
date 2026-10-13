import argparse
import time
import sys
import os
import signal
import tempfile
import math
import random
import multiprocessing as mp
from multiprocessing import Event
import numpy as np
import pandas as pd
import datetime
from questdb import Sender, TimestampNanos
import psycopg as pg
import yfinance as yf
import hashlib
import uuid

# QWP/WebSocket caps each encoded message (one flush frame) at ~2 MiB. A flush over
# that raises "encoded message size ... exceeds maximum configured allowed size" AND
# the client discards the buffer, so the overflow must be prevented up front, not
# retried. SortedEmitter._send_df flushes by the live buffer size (len(sender)), so
# SAFE_FRAME_BYTES is a target with generous headroom rather than a per-row guess.
QWP_MAX_FRAME_BYTES = 2_097_138
SAFE_FRAME_BYTES = 1_400_000
# Emitter buffer cap (rows) for memory / commit sizing; _send_df still frames each flush.
MAX_BATCH_ROWS = 50_000

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

# Precious metals, added to the universe with --metals. They trade as FX pairs: the metal is
# the base currency, quoted per troy ounce, so the generator's base-unit sizes are ounces.
# Appended after the FX pairs, so the FX pairs keep their positions. With --metals the total
# EPS is unchanged and spread over 35 pairs, so each FX pair gets 30/35 of its usual rows.
# Fallback brackets are static, set on 2026-10-13 from Yahoo front-month futures (GC=F 4180,
# SI=F 59.5, PL=F 1672, PA=F 1128) and EURUSD 1.1196; used only when Yahoo is unavailable.
METALS_PAIRS = [
    # (symbol, low, high, precision, pip, rank)
    ("XAUUSD", 4100.0, 4260.0, 2, 0.01, 2),    # Gold, the metals benchmark
    ("XAGUSD", 58.0, 61.0, 3, 0.001, 6),       # Silver
    ("XAUEUR", 3660.0, 3810.0, 2, 0.01, 8),    # Gold in EUR, bracket derived from XAUUSD / EURUSD
    ("XPTUSD", 1640.0, 1705.0, 2, 0.01, 9),    # Platinum
    ("XPDUSD", 1100.0, 1160.0, 2, 0.01, 10),   # Palladium, widest spreads and jumpiest
]

# Per-metal overrides of the FX defaults. FX pairs have no entry and keep the original
# behaviour exactly (1 to 8 pip spreads, 1 pip per book level, FX ladder and trade sizes).
#   yahoo:  Yahoo ticker (the XXXUSD=X spot tickers do not resolve, front-month futures do);
#           None means derived from two other pairs (see YAHOO_DERIVED)
#   move:   multiplier on the per-second mid random walk and shocks, in pips. Set so the
#           per-second volatility is about twice the real one, like a lively demo market
#   shock_prob: probability of a shock in a given second (FX default 0.010)
#   spread: (min, max) spread in pips
#   level:  pips between consecutive book levels, so a 40-level gold book spans $4
#   vol:    scale on the FX volume ladder (base units, here troy ounces)
#   size:   scale on the FX trade size distribution (median 500k becomes 3,000 oz of gold)
METAL_PARAMS = {
    "XAUUSD": dict(yahoo="GC=F", move=10.0, shock_prob=0.010, spread=(20, 40), level=10, vol=0.01, size=0.006),
    "XAGUSD": dict(yahoo="SI=F", move=2.6, shock_prob=0.010, spread=(20, 40), level=5, vol=0.2, size=0.1),
    "XAUEUR": dict(yahoo=None, move=9.0, shock_prob=0.010, spread=(25, 50), level=10, vol=0.01, size=0.006),
    "XPTUSD": dict(yahoo="PL=F", move=6.0, shock_prob=0.010, spread=(100, 300), level=20, vol=0.005, size=0.002),
    "XPDUSD": dict(yahoo="PA=F", move=6.0, shock_prob=0.030, spread=(300, 800), level=50, vol=0.003, size=0.002),
}

# Pairs whose Yahoo bracket is derived from two fetched pairs: mid = numerator / denominator.
YAHOO_DERIVED = {"XAUEUR": ("XAUUSD", "EURUSD")}


def pair_universe(metals):
    return FX_PAIRS + METALS_PAIRS if metals else list(FX_PAIRS)


# --- Reproducibility (--seed) and per-stream randomness --- #
# Without --seed nothing is seeded and behaviour is exactly as before. With it, every process
# seeds its own streams from (seed, role, ...) so runs are reproducible byte for byte.
# Metals price evolution draws from its own stream, so the FX pairs' price paths are the same
# with --metals on or off; forwards draw from their own stream too (see ForwardsModel).
_SEEDED = False
_METALS_RNG = random.Random()


def _derive_seed(seed, *salt):
    h = hashlib.sha256(repr((seed,) + salt).encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big")


def seed_process(seed, *salt):
    global _SEEDED
    if seed is None:
        return
    s = _derive_seed(seed, *salt)
    random.seed(s)
    np.random.seed(s % (2 ** 32))
    _METALS_RNG.seed(_derive_seed(seed, "metals", *salt))
    _SEEDED = True


def new_uuid():
    # uuid4() reads os.urandom and can never be reproduced, so a seeded run draws its UUIDs
    # from the seeded stream instead (still valid version-4 UUIDs).
    if _SEEDED:
        return str(uuid.UUID(int=random.getrandbits(128), version=4))
    return str(uuid.uuid4())


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


def generate_order(symbol, best_bid, best_ask, pip, rank, ecn, size_scale=None):
    """
    Generate a realistic order with rank-weighted probability.

    Args:
        symbol: FX pair symbol
        best_bid: Current best bid price
        best_ask: Current best ask price
        pip: Price step between book levels (the pip for FX pairs)
        rank: Liquidity rank (1=most liquid, 10=least liquid)
        ecn: ECN/venue where order is placed (LMAX, EBS, Hotspot, Currenex)
        size_scale: optional scale on the trade size distribution (metals, in ounces)

    Returns:
        dict with order_id, symbol, side, quantity, limit_price, passive, ecn
    """
    order_id = new_uuid()

    # Side: 50/50 buy vs sell
    side = random.choice(["buy", "sell"])

    # Passive: ~40% of orders are passive (resting limit orders)
    passive = random.random() < 0.4

    # Quantity: log-normal distribution
    if size_scale is None:
        quantity = generate_trade_size_lognormal()
    else:
        quantity = generate_trade_size_lognormal(100_000 * size_scale, 500_000 * size_scale,
                                                 100_000_000 * size_scale)

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
    Buffers rows for market_data, core_price, and fx_trades and writes each buffer
    out in a single batched sender.dataframe() call (serialized by the questdb
    client's native code) instead of one sender.row() call per row.

    Performance notes vs the previous implementation (same external behaviour):
    - Buffers are no longer sorted by timestamp before sending. QuestDB's O3
      ingestion reorders out-of-order rows server-side, so the client-side sort
      was pure overhead; the final table contents are identical.
    - emit_market() takes ownership of the bids/asks arrays (callers pass fresh
      per-event arrays), so the per-row numpy copies are gone.
    - Rows are accumulated column-wise so the flush can hand contiguous columns
      straight to pandas without per-row dict churn.
    """

    def __init__(self, sender, buffer_limit, suffix, no_market_data=False):
        self.sender = sender
        self.buffer_limit = buffer_limit
        self.suffix = suffix
        # When market_data is disabled it no longer drives _flush_md (which used to flush
        # all three tables together), so flush trades alongside core_price to keep low-volume
        # trades visible promptly instead of waiting for the (now large) trades buffer to fill.
        self.no_market_data = no_market_data
        self._md = {"ts": [], "symbol": [], "bids": [], "asks": [],
                    "best_bid": [], "best_ask": []}
        # core_price is accumulated as a list of column-array chunks (one chunk per
        # emit_core_bulk call, i.e. per symbol per second) and concatenated at flush.
        # This replaces a per-event dict-append path: at high core EPS the per-row
        # Python overhead dominated generation (see emit_core_bulk / generate).
        self._cp_chunks = []
        self._cp_n = 0
        self._tr = {"ts": [], "symbol": [], "ecn": [], "side": [], "counterparty": [],
                    "trade_id": [], "passive": [], "price": [], "quantity": [], "order_id": []}
        # fx_forward_points (--forwards): one chunk per ladder (all tenors of one pair).
        self._fw_chunks = []
        self._fw_n = 0

    # --- Market data rows --- #
    def emit_market(self, ts_ns, symbol, bids, asks):
        b = self._md
        b["ts"].append(ts_ns)
        b["symbol"].append(symbol)
        b["bids"].append(bids)   # caller hands over a fresh array (no copy needed)
        b["asks"].append(asks)
        b["best_bid"].append(float(bids[0][0]))
        b["best_ask"].append(float(asks[0][0]))
        if len(b["ts"]) >= self.buffer_limit:
            self._flush_md()

    # --- Core price rows (bulk) --- #
    def emit_core_bulk(self, ts, symbol, ecn, reason, bid_price, bid_volume,
                       ask_price, ask_volume, indicator1, indicator2):
        # Each argument is a numpy column covering all of one symbol's core_price
        # events for the second (or sub-slice). Columns are stashed as-is and
        # concatenated once at flush, so there is no per-row Python work here.
        self._cp_chunks.append((ts, symbol, ecn, reason, bid_price, bid_volume,
                                ask_price, ask_volume, indicator1, indicator2))
        self._cp_n += len(ts)
        if self._cp_n >= self.buffer_limit:
            self._flush_cp()

    # --- Trade rows --- #
    def emit_trade(self, ts_ns, symbol, ecn, trade_id, side, passive, price, quantity, counterparty, order_id):
        b = self._tr
        b["ts"].append(ts_ns)
        b["symbol"].append(symbol)
        b["ecn"].append(ecn)
        b["side"].append(side)
        b["counterparty"].append(counterparty)
        b["trade_id"].append(trade_id)
        b["passive"].append(passive)
        b["price"].append(price)
        b["quantity"].append(quantity)
        b["order_id"].append(order_id)
        if len(b["ts"]) >= self.buffer_limit:
            self._flush_tr()

    # --- Forward points rows (one ladder per call) --- #
    def emit_fwd_ladder(self, ts_ns, symbol, tenors, tenor_days, value_date_ns, bid_points, ask_points, spot_ref):
        self._fw_chunks.append((ts_ns, symbol, tenors, tenor_days, value_date_ns, bid_points, ask_points, spot_ref))
        self._fw_n += len(tenors)
        if self._fw_n >= self.buffer_limit:
            self._flush_fw()

    # --- Framed send: never let one flush exceed the QWP frame limit --- #
    def _send_df(self, df, table, symbols, bytes_per_row):
        """Send df to QuestDB in frames that never exceed the QWP ~2 MiB message limit.

        Rows are appended to the sender buffer in modest increments and flushed by the
        ACTUAL staged buffer size (len(self.sender)) rather than a per-row byte guess,
        so it is robust to however the columns actually encode over QWP. bytes_per_row
        is only a rough per-table estimate used to size each append and a fallback row
        cap (in case the buffer size is ever reported as 0). A flush that overran the
        limit would be discarded by the client, so we must stay under it up front."""
        n = len(df)
        if n == 0:
            return
        add_rows = max(1, 200_000 // bytes_per_row)                 # one append stays well under the budget
        row_cap = max(add_rows, SAFE_FRAME_BYTES // bytes_per_row)  # fallback if len(sender) is unavailable
        start = 0
        staged = 0
        while start < n:
            end = min(start + add_rows, n)
            self.sender.dataframe(df.iloc[start:end], table_name=table, symbols=symbols, at="ts")
            staged += end - start
            start = end
            if start >= n or len(self.sender) >= SAFE_FRAME_BYTES or staged >= row_cap:
                self.sender.flush()
                staged = 0

    # --- Flush methods --- #
    def _flush_md(self):
        b = self._md
        if not b["ts"]:
            return
        # Order-book rows carry two DOUBLE[][] arrays; per-row size grows with depth,
        # so size the frame budget from the widest book in this batch.
        maxlv = max(a.shape[1] for a in b["bids"])
        df = pd.DataFrame({
            "symbol": pd.Categorical(b["symbol"]),
            "bids": b["bids"],
            "asks": b["asks"],
            "best_bid": np.asarray(b["best_bid"], dtype=np.float64),
            "best_ask": np.asarray(b["best_ask"], dtype=np.float64),
            "ts": pd.to_datetime(np.asarray(b["ts"], dtype=np.int64), unit="ns"),
        })
        self._send_df(df, table_name("market_data", self.suffix), ["symbol"], 48 * maxlv + 192)
        for v in b.values():
            v.clear()

        # Also flush core_price and fx_trades to keep all tables synchronized
        self._flush_cp()
        self._flush_tr()

    def _flush_cp(self):
        # Forward points are low volume; ride core_price's flush cadence.
        self._flush_fw()
        if not self._cp_chunks:
            return
        cols = list(zip(*self._cp_chunks))
        cat = lambda i: cols[i][0] if len(cols[i]) == 1 else np.concatenate(cols[i])
        df = pd.DataFrame({
            "symbol": pd.Categorical(cat(1)),
            "ecn": pd.Categorical(cat(2)),
            "reason": pd.Categorical(cat(3)),
            "bid_price": cat(4).astype(np.float64, copy=False),
            "bid_volume": cat(5).astype(np.int64),
            "ask_price": cat(6).astype(np.float64, copy=False),
            "ask_volume": cat(7).astype(np.int64),
            "indicator1": cat(8).astype(np.float64, copy=False),
            "indicator2": cat(9).astype(np.float64, copy=False),
            "ts": pd.to_datetime(cat(0).astype(np.int64, copy=False), unit="ns"),
        })
        self._send_df(df, table_name("core_price", self.suffix),
                      ["symbol", "ecn", "reason"], 80)
        self._cp_chunks = []
        self._cp_n = 0

        # With market_data off, ride core_price's flush cadence so trades stay fresh.
        if self.no_market_data:
            self._flush_tr()

    def _flush_tr(self):
        b = self._tr
        if not b["ts"]:
            return
        df = pd.DataFrame({
            "symbol": pd.Categorical(b["symbol"]),
            "ecn": pd.Categorical(b["ecn"]),
            "side": pd.Categorical(b["side"]),
            "counterparty": pd.Categorical(b["counterparty"]),
            "trade_id": b["trade_id"],
            "passive": np.asarray(b["passive"], dtype=bool),
            "price": np.asarray(b["price"], dtype=np.float64),
            "quantity": np.asarray(b["quantity"], dtype=np.float64),
            "order_id": b["order_id"],
            "ts": pd.to_datetime(np.asarray(b["ts"], dtype=np.int64), unit="ns"),
        })
        self._send_df(df, table_name("fx_trades", self.suffix),
                      ["symbol", "ecn", "side", "counterparty"], 128)
        for v in b.values():
            v.clear()

    def _flush_fw(self):
        if not self._fw_chunks:
            return
        n = self._fw_n
        ts = np.empty(n, dtype=np.int64)
        symbol = []
        tenor = []
        tenor_days = np.empty(n, dtype=np.int32)
        value_date = np.empty(n, dtype=np.int64)
        bid = np.empty(n, dtype=np.float64)
        ask = np.empty(n, dtype=np.float64)
        spot = np.empty(n, dtype=np.float64)
        i = 0
        for ts_ns, sym, tenors, days, vd, bp, ap, sr in self._fw_chunks:
            k = len(tenors)
            ts[i:i + k] = ts_ns
            symbol.extend([sym] * k)
            tenor.extend(tenors)
            tenor_days[i:i + k] = days
            value_date[i:i + k] = vd
            bid[i:i + k] = bp
            ask[i:i + k] = ap
            spot[i:i + k] = sr
            i += k
        df = pd.DataFrame({
            "symbol": pd.Categorical(symbol),
            "tenor": pd.Categorical(tenor),
            "tenor_days": tenor_days,
            "value_date": pd.to_datetime(value_date, unit="ns"),
            "bid_points": bid,
            "ask_points": ask,
            "spot_ref": spot,
            "source": pd.Categorical(["FWD_FEED"] * n),
            "ts": pd.to_datetime(ts, unit="ns"),
        })
        self._send_df(df, table_name("fx_forward_points", self.suffix),
                      ["symbol", "tenor", "source"], 96)
        self._fw_chunks = []
        self._fw_n = 0

    # --- flush all before exit --- #
    def flush_all(self):
        self._flush_md()
        self._flush_cp()
        self._flush_tr()
        self._flush_fw()


def qwp_addr_list(host, default_port=9000):
    """Build a QWP addr= value from --host, supporting multi-host HA failover.

    Accepts a single bare host ("h" -> "h:9000"), a single host:port, or a
    comma-separated list ("h1:9000,h2:9000,h3") where any entry missing a port
    gets default_port. The QWP client rotates across the listed nodes and replays
    store-and-forward frames on the one that is (or becomes) the writable primary.
    """
    out = []
    for h in host.split(","):
        h = h.strip()
        if not h:
            continue
        out.append(h if ":" in h else f"{h}:{default_port}")
    return ",".join(out)

def pg_host(host):
    """First bare hostname from --host (strip :port and any extra hosts).

    PG-wire metadata/DDL/WAL connections target a single node, and DDL must land on the
    writable primary, so put the primary first in a multi-host --host list. QWP ingest
    itself still fails over across every host via qwp_addr_list().
    """
    first = host.split(",")[0].strip()
    return first.rsplit(":", 1)[0] if ":" in first else first

def compute_buffer_limit(args):
    """Rows buffered per table before a flush (= one WAL commit).

    Base is 1000 (real-time) / 10000 (faster-than-life). When market_data is disabled,
    core_price becomes the lead table and can run at very high EPS; keep the per-flush
    batch large enough to hold ~10 flushes/sec so we don't spam tiny commits (which is
    what makes QuestDB's WAL apply fall behind and trips the backpressure monitor).
    """
    base = 1000 if args.mode == "real-time" else 10000
    if getattr(args, "no_market_data", False):
        base = max(base, args.core_max_eps // 10)
        # Bound the per-flush batch for memory / commit sizing. Frame safety (staying
        # under the QWP ~2 MiB message limit) is enforced separately by _send_df.
        base = min(base, MAX_BATCH_ROWS)
    return base

def compute_wal_threshold(args, buffer_limit):
    """WAL apply-lag threshold (in un-applied transactions) before the monitor pauses.

    Market-on keeps the proven 3*processes (market_data commits slowly). When market_data
    is disabled, core_price leads and commits far more often, so a threshold of 3 is a
    hair-trigger: default to ~10s of the lead table's commit rate instead. Override with
    --wal_lag_threshold.
    """
    if args.wal_lag_threshold is not None:
        return args.wal_lag_threshold
    if getattr(args, "no_market_data", False):
        commit_rate = max(1, args.core_max_eps // buffer_limit)   # commits/sec
        return min(1000, max(3 * args.processes, 10 * commit_rate))
    return 3 * args.processes

def lerp_state(open_state, close_state, f):
    """Linearly interpolate every per-symbol state field between open and close at fraction f
    in [0,1]. Used to slice a real-time second into sub-windows: because the second's events
    already interpolate open->close linearly, slicing on this same line reproduces the exact
    price/indicator trajectory and keeps slice boundaries continuous (sub_close[k] == sub_open[k+1]).
    """
    out = {}
    for sym, o in open_state.items():
        c = close_state[sym]
        out[sym] = {k: o[k] + f * (c[k] - o[k]) for k in o}
    return out

def slice_count(total, n, s):
    """Split `total` events across `n` slices, spreading the remainder over the first slices so
    counts are as even as possible (slice index s in [0, n))."""
    return total // n + (1 if s < (total % n) else 0)

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

def retention_clause(short_ttl, enterprise, oss_ttl, enterprise_policy):
    if not short_ttl:
        return ''
    if enterprise:
        return f'STORAGE POLICY({enterprise_policy})'
    return f'TTL {oss_ttl}'


# Materialized views currently cannot accept STORAGE POLICY in QuestDB, so they
# always fall back to TTL semantics (even in enterprise mode). The enterprise_policy
# argument is accepted but ignored, so callers can keep their existing signatures
# and we can revert by pointing the matview `rc` lambda back at retention_clause.
def mv_retention_clause(short_ttl, enterprise, oss_ttl, enterprise_policy):
    if not short_ttl:
        return ''
    return f'TTL {oss_ttl}'


TABLE_ENTERPRISE_POLICY = 'TO REMOTE 1 hour, TO PARQUET 2 days, DROP LOCAL 3 months'


def ensure_tables_exist(args, suffix):
    conn_str = f"user={args.user} password={args.password} host={pg_host(args.host)} port={args.pg_port} dbname=qdb"
    short_ttl = args.short_ttl
    enterprise = args.enterprise
    with pg.connect(conn_str, autocommit=True) as conn:
        if not getattr(args, "no_market_data", False):
            conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name('market_data', suffix)} (
                timestamp TIMESTAMP PARQUET(delta_binary_packed, zstd(4)),
                symbol SYMBOL CAPACITY 15000 PARQUET(rle_dictionary, zstd(4), bloom_filter),
                bids DOUBLE[][] PARQUET(default, zstd(4)),
                asks DOUBLE[][] PARQUET(default, zstd(4)),
                best_bid DOUBLE PARQUET(default, zstd(4)),
                best_ask DOUBLE PARQUET(default, zstd(4))
            ) timestamp(timestamp) PARTITION BY HOUR {retention_clause(short_ttl, enterprise, '3 DAYS', TABLE_ENTERPRISE_POLICY)};
            """)
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name('core_price', suffix)} (
            timestamp TIMESTAMP PARQUET(delta_binary_packed, zstd(4)),
            symbol SYMBOL CAPACITY 15000 PARQUET(rle_dictionary, zstd(4), bloom_filter),
            ecn SYMBOL PARQUET(rle_dictionary, zstd(4), bloom_filter),
            bid_price DOUBLE PARQUET(default, zstd(4)),
            bid_volume LONG PARQUET(default, zstd(4)),
            ask_price DOUBLE PARQUET(default, zstd(4)),
            ask_volume LONG PARQUET(default, zstd(4)),
            reason SYMBOL PARQUET(rle_dictionary, zstd(4)),
            indicator1 DOUBLE PARQUET(default, zstd(4)),
            indicator2 DOUBLE PARQUET(default, zstd(4))
        ) timestamp(timestamp) PARTITION BY HOUR {retention_clause(short_ttl, enterprise, '3 DAYS', TABLE_ENTERPRISE_POLICY)};
        """)
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name('fx_trades', suffix)} (
            timestamp TIMESTAMP_NS PARQUET(delta_binary_packed, zstd(4)),
            symbol SYMBOL CAPACITY 15000 PARQUET(rle_dictionary, zstd(4), bloom_filter),
            ecn SYMBOL PARQUET(rle_dictionary, zstd(4), bloom_filter),
            trade_id UUID PARQUET(default, zstd(4)),
            side SYMBOL PARQUET(rle_dictionary, zstd(4), bloom_filter),
            passive BOOLEAN PARQUET(default, zstd(4)),
            price DOUBLE PARQUET(default, zstd(4)),
            quantity DOUBLE PARQUET(default, zstd(4)),
            counterparty SYMBOL PARQUET(rle_dictionary, zstd(4)),
            order_id UUID PARQUET(default, zstd(4))
        ) timestamp(timestamp) PARTITION BY HOUR {retention_clause(short_ttl, enterprise, '1 MONTH', TABLE_ENTERPRISE_POLICY)} DEDUP UPSERT KEYS(timestamp, trade_id);
        """)
        if getattr(args, "forwards", False):
            conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name('fx_forward_points', suffix)} (
                timestamp TIMESTAMP PARQUET(delta_binary_packed, zstd(4)),
                symbol SYMBOL CAPACITY 15000 PARQUET(rle_dictionary, zstd(4), bloom_filter),
                tenor SYMBOL PARQUET(rle_dictionary, zstd(4)),
                tenor_days INT PARQUET(default, zstd(4)),
                value_date TIMESTAMP PARQUET(delta_binary_packed, zstd(4)),
                bid_points DOUBLE PARQUET(default, zstd(4)),
                ask_points DOUBLE PARQUET(default, zstd(4)),
                spot_ref DOUBLE PARQUET(default, zstd(4)),
                source SYMBOL PARQUET(rle_dictionary, zstd(4))
            ) timestamp(timestamp) PARTITION BY HOUR {retention_clause(short_ttl, enterprise, '3 DAYS', TABLE_ENTERPRISE_POLICY)} DEDUP UPSERT KEYS(timestamp, symbol, tenor);
            """)

def ensure_materialized_views_exist(args, suffix):
    conn_str = f"user={args.user} password={args.password} host={pg_host(args.host)} port={args.pg_port} dbname=qdb"
    short_ttl = args.short_ttl
    enterprise = args.enterprise
    # Storage policies are not yet supported on materialized views in QuestDB.
    # To re-enable storage policies for matviews, swap the lambdas below.
    # rc = lambda oss_ttl, ep: retention_clause(short_ttl, enterprise, oss_ttl, ep)
    rc = lambda oss_ttl, ep: mv_retention_clause(short_ttl, enterprise, oss_ttl, ep)
    # bbo_* and market_data_ohlc_* views read from market_data; skip them when it is disabled.
    _md = not getattr(args, "no_market_data", False)
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
        ) PARTITION BY HOUR {rc('4 HOURS', 'TO REMOTE 1 hour, TO PARQUET 2 days, DROP LOCAL 12 months')};
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
        ) PARTITION BY MONTH {rc('1 MONTH', 'TO REMOTE 1 month, TO PARQUET 1 month, DROP LOCAL 12 months')};
        """)
        if _md: conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('bbo_1s', suffix)} AS (
            SELECT timestamp, symbol,
                last(best_bid) AS bid,
                last(best_ask) AS ask
            FROM {table_name('market_data', suffix)}
            SAMPLE BY 1s
        ) PARTITION BY HOUR {rc('3 DAYS', 'TO REMOTE 1 hour, TO PARQUET 2 days, DROP LOCAL 12 months')};
        """)

        if _md: conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('bbo_1m', suffix)} REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM {table_name('bbo_1s', suffix)}
            SAMPLE BY 1m
        ) PARTITION BY DAY {rc('3 DAYS', 'TO REMOTE 1 day, TO PARQUET 7 days, DROP LOCAL 12 months')};
        """)

        if _md: conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('bbo_1h', suffix)} REFRESH EVERY 10m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM  {table_name('bbo_1m', suffix)}
            SAMPLE BY 1h
        ) PARTITION BY MONTH {rc('1 MONTH', 'TO REMOTE 1 month, TO PARQUET 1 month, DROP LOCAL 12 months')};
        """)

        if _md: conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('bbo_1d', suffix)} REFRESH EVERY 1h DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM {table_name('bbo_1h', suffix)}
            SAMPLE BY 1d
        ) PARTITION BY MONTH {rc('1 MONTH', 'TO REMOTE 1 month, TO PARQUET 1 month, DROP LOCAL 12 months')};
        """)

        if _md: conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('market_data_ohlc_1m', suffix)}  AS (
            SELECT timestamp, symbol,
                first(best_bid) AS open,
                max(best_bid) AS high,
                min(best_bid) AS low,
                last(best_bid) AS close,
                SUM(bids[2][1]) AS total_volume
            FROM {table_name('market_data', suffix)}
            SAMPLE BY 1m
        ) PARTITION BY HOUR {rc('1 DAY', 'TO REMOTE 1 hour, TO PARQUET 2 days, DROP LOCAL 12 months')};
        """)

        if _md: conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('market_data_ohlc_15m', suffix)} AS (
            SELECT timestamp, symbol,
                first(open) AS open,
                max(high) AS high,
                min(low) AS low,
                last(close) AS close,
                SUM(total_volume) AS total_volume

            FROM {table_name('market_data_ohlc_1m', suffix)}
            SAMPLE BY 15m
        ) PARTITION BY HOUR {rc('2 DAYS', 'TO REMOTE 1 day, TO PARQUET 7 days, DROP LOCAL 12 months')};
        """)

        if _md: conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('market_data_ohlc_1d', suffix)} REFRESH EVERY 1h DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                first(best_bid) AS open,
                max(best_bid) AS high,
                min(best_bid) AS low,
                last(best_bid) AS close,
                SUM(bids[2][1]) AS total_volume
            FROM {table_name('market_data', suffix)}
            SAMPLE BY 1d
        ) PARTITION BY MONTH {rc('1 MONTH', 'TO REMOTE 1 month, TO PARQUET 1 month, DROP LOCAL 12 months')};
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
        ) PARTITION BY HOUR {rc('2 DAYS', 'TO REMOTE 1 hour, TO PARQUET 2 days, DROP LOCAL 12 months')};
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
       ) PARTITION BY MONTH {rc('1 MONTH', 'TO REMOTE 1 month, TO PARQUET 1 month, DROP LOCAL 12 months')};
        """)

        # 2 DAYS rather than core_price_1s's 4 HOURS: the planted bad point (09:20 UTC by
        # default) must still be in the view for an afternoon demo querying '$today'.
        if getattr(args, "forwards", False): conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name('fx_forward_points_1m', suffix)} AS (
            SELECT timestamp, symbol, tenor,
                last(bid_points) AS bid_points,
                last(ask_points) AS ask_points,
                last(spot_ref) AS spot_ref,
                last(value_date) AS value_date,
                last(tenor_days) AS tenor_days
            FROM {table_name('fx_forward_points', suffix)}
            SAMPLE BY 1m
        ) PARTITION BY HOUR {rc('2 DAYS', 'TO REMOTE 1 hour, TO PARQUET 2 days, DROP LOCAL 12 months')};
        """)


# used only with incremental mode
def load_initial_state(args, suffix):
    state = {}
    conn_str = f"user={args.user} password={args.password} host={pg_host(args.host)} port={args.pg_port} dbname=qdb"
    with pg.connect(conn_str) as conn:
        cur = conn.execute(f"SELECT symbol, bid_price, ask_price, indicator1, indicator2 FROM {table_name('core_price', suffix)} LATEST BY symbol")
        for row in cur.fetchall():
            state[row[0]] = {
                "bid_price": row[1], "ask_price": row[2],
                "indicator1": row[3], "indicator2": row[4],
                "spread": 0.0004
            }
    return state

def complete_initial_state(state, fx_pairs):
    # Incremental runs start from the latest row per symbol; a pair with no rows yet (e.g. the
    # first run with --metals) starts from its bracket instead of failing on a missing key.
    missing = [p for p in fx_pairs if p[0] not in state]
    if missing:
        state.update(load_initial_state_from_brackets(missing))
    for symbol, _low, _high, _precision, pip, _rank in fx_pairs:
        prm = METAL_PARAMS.get(symbol)
        if prm is not None:
            s = state[symbol]
            s["spread"] = max(prm["spread"][0] * pip, quantize_to_pip(s["ask_price"] - s["bid_price"], pip))
    return state

def load_initial_state_from_brackets(fx_pairs):
    state = {}
    for symbol, low, high, precision, pip, rank in fx_pairs:
        mid = (low + high) / 2
        prm = METAL_PARAMS.get(symbol)
        spread = quantize_to_pip(0.0004, pip) if prm is None else prm["spread"][0] * pip
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
    mids = {}
    print("[INFO] Refreshing reference data from Yahoo Finance.", flush=True)
    for symbol, _low, _high, precision, pip, rank in fx_pairs_template:
        prm = METAL_PARAMS.get(symbol)
        if symbol in YAHOO_DERIVED:
            out.append(None)  # filled below, once its legs are known
            continue
        ysym = symbol + "=X" if prm is None else prm["yahoo"]
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
                mids[symbol] = mid
            else:
                print(f"[YF] {symbol}: DataFrame EMPTY. Fallback used.", flush=True)
                raise ValueError("No data from Yahoo")
        except Exception:
            low = _low
            high = _high
            print(f"[YF] {symbol}: Fallback to template low/high: {low}, {high}", flush=True)
        out.append((symbol, low, high, precision, pip, rank))
    for i, (symbol, _low, _high, precision, pip, rank) in enumerate(fx_pairs_template):
        if out[i] is not None:
            continue
        num, den = YAHOO_DERIVED[symbol]
        if num in mids and den in mids:
            mid = mids[num] / mids[den]
            out[i] = (symbol, mid * (1 - pct), mid * (1 + pct), precision, pip, rank)
        else:
            out[i] = (symbol, _low, _high, precision, pip, rank)
            print(f"[YF] {symbol}: Fallback to template low/high: {_low}, {_high}", flush=True)
    return out

def fx_pairs_refresher(shared_fx_pairs, interval=300, bracket_pct=1.0, first_ready_event=None,
                       shutdown_event=None):
    _ignore_sigint()
    while not _shutting_down(shutdown_event):
        new_fx_pairs = fetch_fx_pairs_from_yahoo(list(shared_fx_pairs), bracket_pct)
        for i in range(len(shared_fx_pairs)):
            shared_fx_pairs[i] = new_fx_pairs[i]
        if first_ready_event is not None and not first_ready_event.is_set():
            first_ready_event.set()
        _interruptible_sleep(shutdown_event, interval)

def quantize_to_pip(price, pip):
    decimals = abs(int(round(np.log10(pip))))
    return round(round(price / pip) * pip, decimals)

def evolve_mid_price(prev_mid, low, high, precision, pip, drift=0.9, shock_prob=0.010, rng=random, shock_pips=20):
    change = rng.uniform(-drift * pip, drift * pip)
    if rng.random() < shock_prob:
        change += rng.uniform(-shock_pips * pip, shock_pips * pip)
    new_mid = prev_mid + change
    return quantize_to_pip(max(low, min(high, new_mid)), pip)

def evolve_indicator(value, drift=0.01, shock_prob=0.01, rng=random):
    value += rng.uniform(-drift, drift)
    if rng.random() < shock_prob:
        value += rng.uniform(-0.2, 0.2)
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
        prm = METAL_PARAMS.get(symbol)
        if prm is None:
            # FX pairs: the original model, drawing from the global stream.
            rng = random
            new_mid = evolve_mid_price(prev_mid, low, high, precision, pip, drift=drift)
            # Majors: 1 to 8 pips
            min_pips, max_pips, step = 1, 8, 1
        else:
            # Metals draw from their own stream so the FX pairs' paths do not depend on --metals.
            # Moves, shocks and the spread walk are scaled to each metal's price level in pips.
            rng = _METALS_RNG
            new_mid = evolve_mid_price(prev_mid, low, high, precision, pip, drift=drift * prm["move"],
                                       shock_prob=prm["shock_prob"], rng=rng,
                                       shock_pips=20 * prm["move"])
            min_pips, max_pips = prm["spread"]
            step = max(1, round((max_pips - min_pips) / 8))

        # Work in pips to avoid the tiny deltas disappearing on quantize
        spread_pips = int(round(prev_spread / pip))

        # Small random walk in pips, mostly flat, sometimes tighter or wider
        spread_pips += rng.choice([-1, 0, 0, 0, 1]) * step

        # Rare stress widening
        if rng.random() < 0.0005:  # tweak this probability for more or less frequency
            spread_pips += rng.randint(3, 10) * step

        # Keep spreads in a sane range per pair
        spread_pips = max(min_pips, min(max_pips, spread_pips))

        new_spread = spread_pips * pip

        # new_spread = quantize_to_pip(max(pip, prev_spread + random.uniform(-0.2 * pip, 0.2 * pip)), pip)
        next_state[symbol] = {
            "bid_price": quantize_to_pip(new_mid - new_spread / 2, pip),
            "ask_price": quantize_to_pip(new_mid + new_spread / 2, pip),
            "spread": new_spread,
            "indicator1": evolve_indicator(prev_state[symbol]["indicator1"], rng=rng),
            "indicator2": evolve_indicator(prev_state[symbol]["indicator2"], rng=rng)
        }
    return next_state

# --- Forwards layer (--forwards) --- #
# Forward points over spot at standard tenors, from covered interest parity on per-currency
# short-rate curves (lease-rate curves for the metals), with a slow random walk on top.
# Points are in pips. ON and TN follow market convention: they hold the points of the short
# swaps today->tomorrow and tomorrow->spot as quoted, their value_date is today / tomorrow and
# their tenor_days is negative (days before spot), so the outright for those dates is spot
# MINUS the cumulative points (see the forwards queries in demo_queries.sql). Every row also
# carries spot_ref, the generator's own mid the ladder was priced on.
FWD_TENORS = ["ON", "TN", "1W", "2W", "1M", "2M", "3M", "6M", "9M", "1Y"]
FWD_CYCLE_NS = 5_000_000_000      # one full ladder per pair every 5 seconds
FWD_JITTER_NS = 400_000_000       # random delay on top of each pair's fixed phase in the cycle
DAY_NS = 86_400_000_000_000

# Covered pairs as (symbol, base, quote). EURJPY and XAUEUR are crosses derived from their legs.
FWD_MAJORS = [("EURUSD", "EUR", "USD"), ("USDJPY", "USD", "JPY"), ("GBPUSD", "GBP", "USD"),
              ("USDCHF", "USD", "CHF"), ("AUDUSD", "AUD", "USD"), ("USDCAD", "USD", "CAD"),
              ("NZDUSD", "NZD", "USD"), ("EURJPY", "EUR", "JPY")]
FWD_METALS = [("XAUUSD", "XAU", "USD"), ("XAGUSD", "XAG", "USD"), ("XPTUSD", "XPT", "USD"),
              ("XPDUSD", "XPD", "USD"), ("XAUEUR", "XAU", "EUR")]
# Cross -> (leg1, leg2, op): EURJPY = EURUSD x USDJPY, XAUEUR = XAUUSD / EURUSD, so the
# triangular consistency check holds by construction (except for the planted bad point).
FWD_CROSSES = {"EURJPY": ("EURUSD", "USDJPY", "mul"), "XAUEUR": ("XAUUSD", "EURUSD", "div")}

# Half the bid/ask spread in pips per tenor for the majors, widening with tenor; scaled per
# pair for crosses and metals (metals points are large numbers of pips: gold 3M is ~3,800).
FWD_HALF_SPREAD_PIPS = np.array([0.03, 0.03, 0.05, 0.08, 0.12, 0.20, 0.25, 0.50, 0.75, 1.00])
FWD_SPREAD_MULT = {"EURJPY": 1.5, "XAUUSD": 50.0, "XAGUSD": 15.0, "XPTUSD": 100.0,
                   "XPDUSD": 200.0, "XAUEUR": 60.0}

# Planted feed error: once per simulated day the bad symbol's 3M mid is published this many
# pips away from what its legs imply, for FWD_BAD_POINT_MINUTES from --bad_point_utc.
FWD_BAD_POINT_PIPS = 6.0
FWD_BAD_POINT_TENOR = "3M"
FWD_BAD_POINT_MINUTES = 20

# Short-rate curves in percent per tenor (ON, TN, 1W, 2W, 1M, 2M, 3M, 6M, 9M, 1Y), set on
# 2026-10-13. Eyeball them before a demo. Anchors are the policy rates as published by
# centralbank.watch (as of 2026-10-09) and finobird.com/central-banks (as of 2026-10-12):
# Fed 3.75-4.00% (mid 3.875), ECB deposit 2.50%, BoE 3.75%, BoJ 1.25%, SNB 0.00%,
# RBA 4.60%, BoC 2.25%, RBNZ 2.75%. Overnight sits at the policy rate (EUR at the deposit
# rate less the usual ~8 bp to EUR STR); the term shape follows the stated hike or cut bias.
# Metals hold lease rates: silver 1M 5.6 to 6.3% and platinum 1M about 12.5% from 2026
# BullionVault and Kitco reports (both curves inverted after the squeezes); gold in the 0 to 1%
# band (published 1M gold lease was slightly negative); palladium has no published figure and
# is an estimate, higher and noisier than gold.
RATE_CURVES = {
    "USD": [3.88, 3.88, 3.88, 3.89, 3.90, 3.92, 3.93, 3.95, 3.95, 3.94],
    "EUR": [2.42, 2.42, 2.43, 2.44, 2.46, 2.49, 2.52, 2.58, 2.62, 2.65],
    "GBP": [3.73, 3.73, 3.72, 3.71, 3.70, 3.68, 3.66, 3.61, 3.57, 3.54],
    "JPY": [1.23, 1.23, 1.23, 1.24, 1.25, 1.26, 1.27, 1.30, 1.33, 1.35],
    "CHF": [-0.05, -0.05, -0.04, -0.04, -0.03, -0.02, -0.01, 0.01, 0.03, 0.05],
    "AUD": [4.58, 4.58, 4.59, 4.60, 4.61, 4.62, 4.63, 4.66, 4.68, 4.70],
    "CAD": [2.24, 2.24, 2.24, 2.23, 2.22, 2.21, 2.20, 2.18, 2.16, 2.15],
    "NZD": [2.73, 2.73, 2.74, 2.75, 2.77, 2.80, 2.83, 2.88, 2.92, 2.95],
    "XAU": [0.05, 0.05, 0.08, 0.10, 0.15, 0.25, 0.35, 0.55, 0.70, 0.85],
    "XAG": [6.20, 6.10, 6.00, 5.90, 5.70, 5.20, 4.80, 3.90, 3.30, 2.90],
    "XPT": [13.00, 13.00, 12.80, 12.60, 12.50, 11.50, 10.50, 8.00, 6.50, 5.50],
    "XPD": [6.00, 6.00, 5.90, 5.80, 5.60, 5.30, 5.00, 4.60, 4.30, 4.00],
}
# Day count basis: Act/365 for GBP, Act/360 for every other currency and for lease rates.
RATE_DAY_COUNT = {"GBP": 365}
# Random walk on top of the static curves, in basis points: a parallel shift per currency
# (daily volatility, mean-reverting over about a week) plus per-tenor noise (stationary
# standard deviation, mean-reverting over about an hour).
RATE_SHIFT_DAILY_BP = {"XAU": 2.0, "XAG": 8.0, "XPT": 10.0, "XPD": 15.0}   # others 3.0
RATE_TENOR_NOISE_BP = {"XAU": 1.0, "XAG": 3.0, "XPT": 5.0, "XPD": 8.0}     # others 0.5


def _next_business_day(d):
    while d.weekday() >= 5:
        d += datetime.timedelta(days=1)
    return d


def _add_business_days(d, n):
    for _ in range(n):
        d = _next_business_day(d + datetime.timedelta(days=1))
    return d


def _last_business_day_of_month(year, month):
    d = datetime.date(year, month, 1) + datetime.timedelta(days=32)
    d = d.replace(day=1) - datetime.timedelta(days=1)
    while d.weekday() >= 5:
        d -= datetime.timedelta(days=1)
    return d


def _modified_following(d):
    f = _next_business_day(d)
    if f.month != d.month:
        f = d
        while f.weekday() >= 5:
            f -= datetime.timedelta(days=1)
    return f


def _add_months_eom(spot, months):
    # Whole-month tenors roll from spot; end-of-month rule: a spot on the last business day of
    # its month maps to the last business day of the target month.
    y, m = divmod(spot.month - 1 + months, 12)
    y, m = spot.year + y, m + 1
    if spot == _last_business_day_of_month(spot.year, spot.month):
        return _last_business_day_of_month(y, m)
    last_day = (datetime.date(y, m, 1) + datetime.timedelta(days=32)).replace(day=1) - datetime.timedelta(days=1)
    return _modified_following(datetime.date(y, m, min(spot.day, last_day.day)))


def forward_value_dates(today):
    """Value dates for every tenor on a given UTC trade date (weekday calendar, no holidays).

    Returns (value_date, tenor_days, accrual_days) per tenor in FWD_TENORS order. Spot is T+2
    business days for every pair (USDCAD is T+1 in the real market; kept at T+2 here). On a
    weekend the trade date rolls to Monday. tenor_days counts calendar days from spot to the
    value date, so it is negative for ON (today) and TN (tomorrow). accrual_days is the period
    the points cover: today->tomorrow for ON, tomorrow->spot for TN, spot->value date otherwise.
    """
    td = _next_business_day(today)
    tom = _add_business_days(td, 1)
    spot = _add_business_days(td, 2)
    out = [(td, (td - spot).days, (tom - td).days),
           (tom, (tom - spot).days, (spot - tom).days)]
    for weeks in (1, 2):
        vd = _modified_following(spot + datetime.timedelta(days=7 * weeks))
        out.append((vd, (vd - spot).days, (vd - spot).days))
    for months in (1, 2, 3, 6, 9, 12):
        vd = _add_months_eom(spot, months)
        out.append((vd, (vd - spot).days, (vd - spot).days))
    return out


class ForwardsModel:
    """Forward point ladders for the covered pairs, emitted on a fixed 5-second cadence.

    Each pair has a fixed phase in the cycle plus a random jitter, so pairs do not emit together.
    The rate curves step once per cycle. All randomness comes from the rng passed in (its own
    stream, never the global one), so spot data is identical with --forwards on or off. The
    model is used the same way in both modes: faster-than-life calls emissions() per chunk in
    the parent and hands the plan to one worker; real-time calls it every second in the worker.
    """

    def __init__(self, metals, rng, start_ns, bad_point_ns_of_day):
        self.pairs = FWD_MAJORS + (FWD_METALS if metals else [])
        self.symbols = [p[0] for p in self.pairs]
        self.legs = {s: (b, q) for s, b, q in self.pairs}
        self.ccys = sorted({c for _, b, q in self.pairs for c in (b, q)})
        self.rng = rng
        self.base = {c: np.asarray(RATE_CURVES[c], dtype=np.float64) for c in self.ccys}
        self.shift = {c: 0.0 for c in self.ccys}
        self.noise = {c: np.zeros(len(FWD_TENORS)) for c in self.ccys}
        cycles_per_day = DAY_NS // FWD_CYCLE_NS
        self.shift_theta = 1.0 / (7 * cycles_per_day)
        self.noise_theta = 1.0 / (3600 * 1_000_000_000 // FWD_CYCLE_NS)
        self.shift_sigma = {c: RATE_SHIFT_DAILY_BP.get(c, 3.0) / math.sqrt(cycles_per_day) for c in self.ccys}
        self.noise_sigma = {c: RATE_TENOR_NOISE_BP.get(c, 0.5) * math.sqrt(2 * self.noise_theta) for c in self.ccys}
        # Phases leave room for the jitter so every ladder of a cycle stays inside its 5-second
        # slot. Cycles tile the minute, so the 1-minute view's last ladder per pair always comes
        # from the same cycle (same rate step) for every pair; otherwise a cross and its legs
        # could be a rate step apart, which on XAUEUR 1Y is about 2 pips of false gap.
        n = len(self.symbols)
        self.phase = {s: k * (FWD_CYCLE_NS - FWD_JITTER_NS) // n for k, s in enumerate(self.symbols)}
        self.cycle = -(-start_ns // FWD_CYCLE_NS)   # first cycle starting at or after start_ns
        self.pending = []
        self.bad_symbol = "XAUEUR" if metals else "EURJPY"
        self.bad_tenor_idx = FWD_TENORS.index(FWD_BAD_POINT_TENOR)
        self.bad_from = bad_point_ns_of_day
        self.bad_to = bad_point_ns_of_day + FWD_BAD_POINT_MINUTES * 60 * 1_000_000_000
        self._dates = {}

    def _step_rates(self):
        g = self.rng.gauss
        for c in self.ccys:
            self.shift[c] = self.shift[c] * (1 - self.shift_theta) + self.shift_sigma[c] * g(0.0, 1.0)
            eps = np.array([g(0.0, 1.0) for _ in FWD_TENORS])
            self.noise[c] = self.noise[c] * (1 - self.noise_theta) + self.noise_sigma[c] * eps

    def rates(self, ccy):
        # Percent per tenor: static curve plus the walk (kept in basis points).
        return self.base[ccy] + (self.shift[ccy] + self.noise[ccy]) / 100.0

    def _dates_for(self, day_idx):
        d = self._dates.get(day_idx)
        if d is None:
            if len(self._dates) > 8:
                self._dates.clear()
            today = datetime.date(1970, 1, 1) + datetime.timedelta(days=int(day_idx))
            vds = forward_value_dates(today)
            epoch = datetime.date(1970, 1, 1)
            d = (np.array([(v - epoch).days * DAY_NS for v, _, _ in vds], dtype=np.int64),
                 np.array([t for _, t, _ in vds], dtype=np.int32),
                 np.array([a for _, _, a in vds], dtype=np.float64))
            self._dates[day_idx] = d
        return d

    def _factor(self, symbol, accrual_days):
        base, quote = self.legs[symbol]
        rb = self.rates(base) / 100.0
        rq = self.rates(quote) / 100.0
        return ((1 + rq * accrual_days / RATE_DAY_COUNT.get(quote, 360))
                / (1 + rb * accrual_days / RATE_DAY_COUNT.get(base, 360)))

    def premium(self, symbol, accrual_days):
        """Forward premium F/S - 1 per tenor (for ON and TN, over their own short period)."""
        cross = FWD_CROSSES.get(symbol)
        if cross is None:
            return self._factor(symbol, accrual_days) - 1
        leg1, leg2, op = cross
        f1 = self._factor(leg1, accrual_days)
        f2 = self._factor(leg2, accrual_days)
        return (f1 * f2 if op == "mul" else f1 / f2) - 1

    def is_bad(self, symbol, ts):
        if symbol != self.bad_symbol:
            return False
        tod = ts % DAY_NS
        # A window starting late in the day wraps past midnight.
        return self.bad_from <= tod < self.bad_to or tod < self.bad_to - DAY_NS

    def emissions(self, t0, t1):
        """Ladders due in [t0, t1), sorted by timestamp, as
        (ts, symbol, premium[10], value_date_ns[10], tenor_days[10], bad).

        Cycles are processed whole and in order, so the result does not depend on how the
        caller slices time. Ladders before t0 (e.g. after a real-time stall) are dropped, but
        their cycles still step the rate walk."""
        while self.cycle * FWD_CYCLE_NS < t1:
            c0 = self.cycle * FWD_CYCLE_NS
            self._step_rates()
            for s in self.symbols:
                ts = c0 + self.phase[s] + int(self.rng.random() * FWD_JITTER_NS)
                vd, tdays, accrual = self._dates_for(ts // DAY_NS)
                self.pending.append((ts, s, self.premium(s, accrual), vd, tdays, self.is_bad(s, ts)))
            self.cycle += 1
        self.pending.sort(key=lambda e: e[0])
        due = [e for e in self.pending if e[0] < t1]
        self.pending = [e for e in self.pending if e[0] >= t1]
        return [e for e in due if e[0] >= t0]


def emit_forward_ladder(emitter, emission, spot_mid, pip):
    """Turn one planned ladder into fx_forward_points rows at the given spot mid.

    spot_ref is the mid the ladder was priced on (rounded to 1/100 pip), as forward feeds publish
    a reference spot. Points scale with spot, so a consistency check across pairs must convert
    points to forward premium (points x pip / spot_ref) on this spot, not on a spot sampled at
    another instant: for gold, a minute of spot drift moves 1Y points by several pips."""
    ts, symbol, prem, vd, tdays, bad = emission
    spot_mid = round(spot_mid, abs(int(round(math.log10(pip)))) + 2)
    mid = spot_mid * prem / pip
    if bad:
        mid = mid.copy()
        mid[FWD_TENORS.index(FWD_BAD_POINT_TENOR)] += FWD_BAD_POINT_PIPS
    half = FWD_HALF_SPREAD_PIPS * FWD_SPREAD_MULT.get(symbol, 1.0)
    emitter.emit_fwd_ladder(ts, symbol, FWD_TENORS, tdays, vd,
                            np.round(mid - half, 2), np.round(mid + half, 2), spot_mid)


def report_bad_points(bad_ts, symbol):
    """Print the planted bad point's timestamps per day, ready to paste as DECLARE values."""
    if not bad_ts:
        print(f"[FORWARDS] No planted bad point in this run ({symbol} {FWD_BAD_POINT_TENOR} window not covered).",
              flush=True)
        return
    by_day = {}
    for ts in sorted(bad_ts):
        by_day.setdefault(ts // DAY_NS, []).append(ts)
    for day, tss in sorted(by_day.items()):
        print(f"[FORWARDS] Planted bad point {symbol} {FWD_BAD_POINT_TENOR} (+{FWD_BAD_POINT_PIPS:g} pips) "
              f"on {ns_to_iso(day * DAY_NS)[:10]}: {len(tss)} ladders, "
              f"@bad_point_from := '{ns_to_iso(tss[0])}', @bad_point_to := '{ns_to_iso(tss[-1])}'",
              flush=True)


def parse_bad_point_utc(s):
    try:
        hh, mm = s.split(":")
        hh, mm = int(hh), int(mm)
        if not (0 <= hh < 24 and 0 <= mm < 60):
            raise ValueError
    except ValueError:
        print(f"ERROR: --bad_point_utc must be HH:MM in UTC, got {s!r}.")
        sys.exit(1)
    return (hh * 60 + mm) * 60 * 1_000_000_000


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
    lei_pool=None,
    window_ns=1_000_000_000
):
    """
    Generate events for a single second (or a sub-slice of one when window_ns < 1e9):
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
    reason_pool = ["normal", "news_event", "liquidity_event"]
    n_pairs = len(fx_pairs)

    # Vectorized generation: offsets, symbol picks, level counts, level prices and
    # ladder volumes are drawn in bulk with numpy instead of per-event Python loops.
    # Semantics are unchanged: per-symbol events interpolate bid/ask between the
    # second's open and close state by event ordinal (first event = open, last =
    # close, exactly), level i sits i pips away from the best, and volumes come
    # from the same ladder bands as get_random_volume_for_level().

    # Ladder bands as vectors so volumes can be drawn one level-column at a time.
    vol_lo = np.empty(max_levels, dtype=np.int64)
    vol_hi = np.empty(max_levels, dtype=np.int64)
    vol_lo[0] = ladder[0] // 2
    vol_hi[0] = ladder[0]
    for i in range(1, max_levels):
        vol_lo[i] = ladder[i - 1]
        vol_hi[i] = ladder[i]

    def draw_volumes(count):
        # (count, max_levels) matrix of ladder-band volumes. float64 because the
        # book arrays are ingested as DOUBLE[][] (as before).
        vols = np.empty((count, max_levels), dtype=np.float64)
        for i in range(max_levels):
            vols[:, i] = np.random.randint(vol_lo[i], vol_hi[i] + 1, count)
        return vols

    level_pips = np.arange(max_levels, dtype=np.float64)

    def scale_vols(vols, prm):
        # Metals: the same ladder draws, rescaled to troy ounces (FX rows are untouched).
        return np.maximum(1.0, np.round(vols * prm["vol"]))

    def interp_open_close(symbol, n):
        # Interpolated best bid/ask per event ordinal, first/last pinned exactly
        # to the open/close state (the OHLC continuity guarantee).
        o = open_state_for_second[symbol]
        c = close_state_for_second[symbol]
        if n == 1:
            return np.array([o["bid_price"]]), np.array([o["ask_price"]])
        frac = np.arange(n, dtype=np.float64) / (n - 1)
        bid = o["bid_price"] + frac * (c["bid_price"] - o["bid_price"])
        ask = o["ask_price"] + frac * (c["ask_price"] - o["ask_price"])
        bid[0] = o["bid_price"]
        bid[-1] = c["bid_price"]
        ask[0] = o["ask_price"]
        ask[-1] = c["ask_price"]
        return bid, ask

    def price_grids(bid_vec, ask_vec, pip, prm=None):
        # Level i price = best -/+ i pips, quantized exactly like quantize_to_pip():
        # round(round(p / pip) * pip, decimals). Metals space levels prm["level"] pips apart.
        decimals = abs(int(round(math.log10(pip))))
        steps = level_pips * pip if prm is None else level_pips * (prm["level"] * pip)
        pb = np.round(np.round((bid_vec[:, None] - steps) / pip) * pip, decimals)
        pa = np.round(np.round((ask_vec[:, None] + steps) / pip) * pip, decimals)
        return pb, pa

    # --- Generate market_data events (LOTS of them - orderbook updates) ---
    if market_event_count > 0:
        offsets_market = np.sort(np.random.randint(0, window_ns, market_event_count, dtype=np.int64))
        sym_idx_market = np.random.randint(0, n_pairs, market_event_count)
        levels_market = np.random.randint(min_levels, max_levels + 1, market_event_count)
        md_bid_vols = draw_volumes(market_event_count)
        md_ask_vols = draw_volumes(market_event_count)
        row_ts_market = ts + offsets_market

        for k, (symbol, low, high, precision, pip, rank) in enumerate(fx_pairs):
            idx = np.flatnonzero(sym_idx_market == k)
            if idx.size == 0:
                continue
            prm = METAL_PARAMS.get(symbol)
            bid_vec, ask_vec = interp_open_close(symbol, idx.size)
            pb, pa = price_grids(bid_vec, ask_vec, pip, prm)
            if prm is None:
                bvol_rows, avol_rows = md_bid_vols, md_ask_vols   # indexed by event e
            else:
                bvol_rows = scale_vols(md_bid_vols[idx], prm)    # indexed by ordinal j
                avol_rows = scale_vols(md_ask_vols[idx], prm)
            for j, e in enumerate(idx):
                row_ts = int(row_ts_market[e])
                if end_ns is not None and row_ts >= end_ns:
                    continue
                r = e if prm is None else j
                lv = levels_market[e]
                bids = np.empty((2, lv))
                bids[0] = pb[j, :lv]
                bids[1] = bvol_rows[r, :lv]
                asks = np.empty((2, lv))
                asks[0] = pa[j, :lv]
                asks[1] = avol_rows[r, :lv]
                emitter.emit_market(row_ts, symbol, bids, asks)

    # --- Generate core_price events (FEWER - BBO snapshots with metadata) ---
    # Per symbol, every core_price column is computed vectorized and the whole
    # block is handed to the emitter in one emit_core_bulk() call. We keep each
    # symbol's book arrays so trades can sample from them lazily: only the handful
    # of events that are actually traded need a materialized book, instead of one
    # dict per core event (which dominated generation at high core EPS).
    ecn_arr = np.asarray(ecn_pool)
    reason_arr = np.asarray(reason_pool)
    symbol_books = []  # (symbol, pb, pa, bid_vols, ask_vols, levels, ts, ecn, tick, rank, n, size_scale)
    if core_count > 0:
        offsets_core = np.sort(np.random.randint(0, window_ns, core_count, dtype=np.int64))
        sym_idx_core = np.random.randint(0, n_pairs, core_count)
        levels_core = np.random.randint(min_levels, max_levels + 1, core_count)
        core_bid_vols = draw_volumes(core_count)
        core_ask_vols = draw_volumes(core_count)
        reason_idx = np.random.randint(0, len(reason_pool), core_count)
        ecn_idx = np.random.randint(0, len(ecn_pool), core_count)
        row_ts_core = ts + offsets_core

        for k, (symbol, low, high, precision, pip, rank) in enumerate(fx_pairs):
            idx = np.flatnonzero(sym_idx_core == k)
            n_events = idx.size
            if n_events == 0:
                continue
            o_state = open_state_for_second[symbol]
            c_state = close_state_for_second[symbol]
            ind1_open = float(round(o_state["indicator1"], 3))
            ind2_open = float(round(o_state["indicator2"], 3))
            ind1_close = float(round(c_state["indicator1"], 3))
            ind2_close = float(round(c_state["indicator2"], 3))
            prm = METAL_PARAMS.get(symbol)
            bid_vec, ask_vec = interp_open_close(symbol, n_events)
            pb, pa = price_grids(bid_vec, ask_vec, pip, prm)

            # Indicators use the open state for every event except the last, which
            # uses the close state (first event = open, last event = close), exactly
            # as the per-event path did.
            ind1 = np.full(n_events, ind1_open, dtype=np.float64)
            ind2 = np.full(n_events, ind2_open, dtype=np.float64)
            ind1[-1] = ind1_close
            ind2[-1] = ind2_close

            ts_sym = row_ts_core[idx]
            ecn_sym = ecn_arr[ecn_idx[idx]]
            reason_sym = reason_arr[reason_idx[idx]]
            bvols = core_bid_vols[idx]
            avols = core_ask_vols[idx]
            if prm is not None:
                bvols = scale_vols(bvols, prm)
                avols = scale_vols(avols, prm)
            lvls = levels_core[idx]

            # In faster-than-life the time window can end mid-second; drop events at
            # or past end_ns (the per-event path skipped them with `continue`). The
            # interpolation and indicators were computed over the full event set
            # first, so surviving events keep identical values.
            if end_ns is not None:
                keep = ts_sym < end_ns
                if not keep.all():
                    if not keep.any():
                        continue
                    pb = pb[keep]; pa = pa[keep]
                    ts_sym = ts_sym[keep]; ecn_sym = ecn_sym[keep]; reason_sym = reason_sym[keep]
                    bvols = bvols[keep]; avols = avols[keep]; lvls = lvls[keep]
                    ind1 = ind1[keep]; ind2 = ind2[keep]
                    n_events = ts_sym.size

            # Core price always uses level 0 (best bid/offer).
            emitter.emit_core_bulk(
                ts_sym, np.full(n_events, symbol), ecn_sym, reason_sym,
                pb[:, 0], bvols[:, 0], pa[:, 0], avols[:, 0], ind1, ind2,
            )

            # Book kept as (prices, volumes) row views into the grids, so a sampled
            # trade executes against exactly the book whose level 0 was published.
            # Metals trade on their own level spacing and in ounces.
            tick = pip if prm is None else prm["level"] * pip
            symbol_books.append((symbol, pb, pa, bvols, avols, lvls, ts_sym,
                                 ecn_sym, tick, rank, n_events,
                                 None if prm is None else prm["size"]))

    # --- Generate trades from core_price events ---
    # Weight orders by liquidity rank (lower rank = more liquid = higher weight).
    # The per-event weight is (11 - rank); since rank is constant within a symbol,
    # weighting each symbol by (11 - rank) * n_events and then drawing a uniform
    # event inside the chosen symbol reproduces the identical per-event selection
    # distribution, while materializing a book only for the events actually traded.
    total_core = sum(bk[10] for bk in symbol_books)
    if orders_count > 0 and lei_pool and total_core > 0:
        # Cap orders to available core_price events to avoid excessive duplication
        actual_orders_count = min(orders_count, total_core)
        sym_weights = [(11 - bk[9]) * bk[10] for bk in symbol_books]
        chosen_books = random.choices(symbol_books, weights=sym_weights, k=actual_orders_count)

        selected_events = []
        for bk in chosen_books:
            symbol, pb, pa, bvols, avols, lvls, ts_sym, ecn_sym, pip, rank, n_events, size_scale = bk
            p = random.randrange(n_events)
            selected_events.append({
                "timestamp": int(ts_sym[p]),
                "symbol": symbol,
                "ecn": str(ecn_sym[p]),
                "bids": (pb[p], bvols[p]),
                "asks": (pa[p], avols[p]),
                "levels": int(lvls[p]),
                "pip": pip,
                "rank": rank,
                "size_scale": size_scale,
            })

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
                cp_event["ecn"],
                cp_event["size_scale"]
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
                    new_uuid(),
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

def _ignore_sigint():
    """Child processes ignore Ctrl+C so it does not spray KeyboardInterrupt tracebacks;
    the parent's handler sets the shared shutdown event and they exit via that instead."""
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except (ValueError, OSError):
        pass  # not on the main thread of a child; safe to ignore

def _shutting_down(shutdown_event):
    return shutdown_event is not None and shutdown_event.is_set()

def _interruptible_sleep(shutdown_event, seconds):
    """Sleep, but wake immediately if a shutdown is requested (so background
    processes stop promptly on Ctrl+C instead of sitting in a long time.sleep)."""
    if shutdown_event is not None:
        shutdown_event.wait(seconds)
    else:
        time.sleep(seconds)

def _install_parent_sigint(shutdown_event):
    """Turn Ctrl+C in the parent into a graceful shutdown: the first signal asks the
    workers to finish the current batch, flush/drain and exit; a second forces exit.
    Child processes ignore SIGINT (see _ignore_sigint) and stop via shutdown_event."""
    state = {"n": 0}
    def handler(signum, frame):
        state["n"] += 1
        if state["n"] == 1:
            print("\n[SHUTDOWN] Ctrl+C received - draining buffers and stopping cleanly "
                  "(press Ctrl+C again to force).", flush=True)
            shutdown_event.set()
        else:
            print("\n[SHUTDOWN] Forcing immediate exit.", flush=True)
            os._exit(130)
    signal.signal(signal.SIGINT, handler)

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
    lei_pool,
    shutdown_event=None,
    fwd_plan=None  # faster-than-life + --forwards: this chunk's ladders (worker 0 only)
):
    _ignore_sigint()
    seed_process(args.seed, "worker", args.mode, start_ns, process_idx)
    buffer_limit = compute_buffer_limit(args)

    auto_flush_interval = buffer_limit * 2  # safety net in the ILP client

    if args.protocol == "http":
        conf = f"http::addr={args.host}:9000;auto_flush_interval={auto_flush_interval};" if not args.token else f"https::addr={args.host}:9000;token={args.token};tls_verify=unsafe_off;auto_flush_interval={auto_flush_interval};"
    elif args.protocol == "qwp":
        # QWP/WebSocket. Same Sender.from_conf() + dataframe() API as ILP; only the conf
        # differs. QWP has its own versioning, so protocol_version is NOT set (it errors on
        # QWP). Each worker gets a unique sender_id + store-and-forward dir so un-acked frames
        # survive reconnects/failover without colliding between processes.
        scheme = "wss" if args.qwp_tls else "ws"
        sender_id = f"fx-{process_idx}"
        sf_dir = os.path.join(args.store_forward_dir, sender_id)
        os.makedirs(sf_dir, exist_ok=True)
        # --host may be a comma-separated host:port list for multi-host HA failover.
        parts = [f"{scheme}::addr={qwp_addr_list(args.host)};", "auto_flush=off;"]
        if args.token:
            parts.append(f"token={args.token};")
        if args.qwp_tls:
            parts.append("tls_verify=unsafe_off;")
        parts.append(f"sender_id={sender_id};")
        parts.append(f"sf_dir={sf_dir};")
        parts.append("reconnect_max_duration_millis=300000;")
        parts.append("reconnect_initial_backoff_millis=100;")
        parts.append("reconnect_max_backoff_millis=5000;")
        if args.durable_ack:
            parts.append("request_durable_ack=on;")
        conf = "".join(parts)
    else:
        conf = f"tcp::addr={args.host}:9009;protocol_version=2;auto_flush_interval={auto_flush_interval};" if not args.token else f"tcps::addr={args.host}:9009;username={args.ilp_user};token={args.token};token_x={args.token_x};token_y={args.token_y};tls_verify=unsafe_off;protocol_version=2;auto_flush_interval={auto_flush_interval};"

    prebuilt_bids = [np.zeros((2, lvl), dtype=np.float64) for lvl in range(1, args.max_levels + 1)]
    prebuilt_asks = [np.zeros((2, lvl), dtype=np.float64) for lvl in range(1, args.max_levels + 1)]
    ts = start_ns
    sent = 0
    ladder = make_ladder(args.max_levels)

    # <-- NEW: Track last close per symbol for continuity
    last_close_per_symbol = {}

    # Forwards: spot for a ladder is the generator's own mid for the pair at that instant,
    # interpolated on the second's open->close line like every other event.
    pip_by_symbol = {p[0]: p[4] for p in list(fx_pairs)}

    def spot_mid_at(o_state, c_state, symbol, f):
        o = o_state[symbol]
        c = c_state[symbol]
        om = (o["bid_price"] + o["ask_price"]) / 2
        cm = (c["bid_price"] + c["ask_price"]) / 2
        return om + min(1.0, max(0.0, f)) * (cm - om)

    fwd_i = 0
    fwd_model = None
    fwd_bad_ts = []
    if getattr(args, "forwards", False) and args.mode == "real-time":
        fwd_rng = random.Random(None if args.seed is None else _derive_seed(args.seed, "forwards", "real-time"))
        fwd_model = ForwardsModel(args.metals, fwd_rng, time.time_ns(), parse_bad_point_utc(args.bad_point_utc))
        fwd_cursor = None

    with Sender.from_conf(conf) as sender:
        emitter = SortedEmitter(sender, buffer_limit=buffer_limit, suffix=args.suffix,
                                no_market_data=args.no_market_data)
        wall_start = None  # for real-time alignment

        if args.mode == "faster-than-life":
            open_per_second, close_per_second = global_states
            for sec_idx, (market_total, core_total) in enumerate(per_second_plan):
                if _shutting_down(shutdown_event):
                    emitter.flush_all()
                    break
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
                # Forward ladders planned for this second (only the worker given a plan).
                if fwd_plan:
                    sec_end = ts + 1_000_000_000
                    while fwd_i < len(fwd_plan) and fwd_plan[fwd_i][0] < sec_end:
                        em = fwd_plan[fwd_i]
                        fwd_i += 1
                        if em[0] < ts or (end_ns is not None and em[0] >= end_ns):
                            continue
                        mid = spot_mid_at(open_state, close_state, em[1], (em[0] - ts) / 1e9)
                        emit_forward_ladder(emitter, em, mid, pip_by_symbol[em[1]])
                # Stop budget counts core_price events when market_data is disabled.
                sent += core_total if args.no_market_data else market_total

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

            # Optional sub-second slicing: split each real-time second into fixed-duration slices
            # so rows arrive smoothly (~every slice_ms) instead of in one burst per second. Off by
            # default (n_slices == 1 reproduces the original per-second behaviour exactly).
            slice_ms = max(0, getattr(args, "realtime_slice_ms", 0) or 0)
            n_slices = max(1, 1000 // slice_ms) if slice_ms else 1
            slice_ns = 1_000_000_000 // n_slices
            slices_done = 0

            while not (end_ns and ts >= end_ns) and (total_events == 0 or sent < total_events):
                if _shutting_down(shutdown_event):
                    break
                fx_pairs_snapshot = list(fx_pairs)
                wait_if_paused(pause_event, process_idx)
                market_total = 0 if args.no_market_data else random.randint(args.market_data_min_eps, args.market_data_max_eps)
                core_total = random.randint(args.core_min_eps, args.core_max_eps)
                orders_total = random.randint(args.orders_min_per_sec, args.orders_max_per_sec)

                # At the beginning of the second: capture OPEN state
                open_state = {k: v.copy() for k, v in current_state.items()}
                # Evolve for close (to be used as interpolation target)
                close_state = evolve_state_one_tick(current_state, fx_pairs_snapshot, 7.0)

                # Forward ladders due in this second. Rows carry the same +2s lead as the spot
                # rows (see generate_events_for_second). After a stall (WAL pause, hiccup) the
                # missed ladders are skipped rather than back-filled behind live spot.
                if fwd_model is not None:
                    sec_ts = time.time_ns() + 2_000_000_000
                    if fwd_cursor is None or fwd_cursor < sec_ts - 10_000_000_000:
                        fwd_cursor = sec_ts
                    for em in fwd_model.emissions(fwd_cursor, sec_ts + 1_000_000_000):
                        mid = spot_mid_at(open_state, close_state, em[1], (em[0] - sec_ts) / 1e9)
                        emit_forward_ladder(emitter, em, mid, pip_by_symbol[em[1]])
                        if em[5]:
                            fwd_bad_ts.append(em[0])
                    fwd_cursor = max(fwd_cursor, sec_ts + 1_000_000_000)

                for s in range(n_slices):
                    # Sub-window open/close land on the same open->close line, so the price
                    # trajectory and second-boundary continuity are identical to the unsliced case.
                    if n_slices == 1:
                        s_open, s_close, s_window = open_state, close_state, 1_000_000_000
                    else:
                        s_open = lerp_state(open_state, close_state, s / n_slices)
                        s_close = lerp_state(open_state, close_state, (s + 1) / n_slices)
                        s_window = slice_ns
                    generate_events_for_second(
                        ts + s * slice_ns, slice_count(market_total, n_slices, s),
                        slice_count(core_total, n_slices, s), fx_pairs_snapshot,
                        s_open, s_close, emitter, ladder,
                        args.min_levels, args.max_levels,
                        prebuilt_bids, prebuilt_asks,
                        end_ns, args.suffix, True,
                        slice_count(orders_total, n_slices, s), lei_pool,
                        window_ns=s_window
                    )
                    # Push this slice out now so delivery is smooth, then align to its wall-clock boundary.
                    if n_slices > 1:
                        emitter.flush_all()
                    slices_done += 1
                    next_tick = wall_start + slices_done * slice_ns / 1_000_000_000
                    sleep_for = next_tick - time.time()
                    if sleep_for > 0:
                        time.sleep(sleep_for)
                    if _shutting_down(shutdown_event):
                        break

                current_state = close_state
                # Stop budget counts core_price events when market_data is disabled.
                sent += core_total if args.no_market_data else market_total
                ts += int(1e9)
                sec_idx += 1
            emitter.flush_all()

        # QWP/WebSocket: drain outstanding store-and-forward frames (wait for acks)
        # before the sender closes, so a clean exit does not lose buffered rows.
        if args.protocol == "qwp":
            emitter.flush_all()
            sender.close_drain()

    # ... main event loop ends here ...

    if fwd_model is not None:
        report_bad_points(fwd_bad_ts, fwd_model.bad_symbol)

    # Print reason for exit
    if _shutting_down(shutdown_event):
        print(f"[WORKER {process_idx}] Shutdown requested; flushed and drained cleanly (last ts: {ns_to_iso(ts)}).")
        sys.exit(0)
    if end_ns is not None and ts >= end_ns:
        print(f"[WORKER {process_idx}] Finished. Exiting because end_ts ({ns_to_iso(end_ns)}) was reached (last ts: {ns_to_iso(ts)}). Events sent from worker {sent} ")
    elif sent >= total_events:
        print(f"[WORKER {process_idx}] Finished. Exiting because total_market_data_events ({total_events}) was reached (last ts: {ns_to_iso(ts)}).")
    else:
        print(f"[WORKER {process_idx}] Finished. Exiting for unknown reason (last ts: {ns_to_iso(ts)}).")

    sys.exit(0)

def wal_monitor(args, pause_event, threshold, suffix='', interval=5, shutdown_event=None):
    _ignore_sigint()
    import time
    import psycopg as pg
    conn_str = f"user={args.user} password={args.password} host={pg_host(args.host)} port={args.pg_port} dbname=qdb"
    last_logged_paused = False

    # Watch the busiest table for WAL lag: market_data normally, core_price when it is disabled.
    mon_table = table_name('core_price' if getattr(args, "no_market_data", False) else 'market_data', suffix)
    print(f"[WAL MONITOR] Watching {mon_table} with lag threshold {threshold}")
    with pg.connect(conn_str, autocommit=True) as conn:
        while not _shutting_down(shutdown_event):
            cur = conn.execute(f"SELECT sequencerTxn, writerTxn FROM wal_tables() WHERE name = '{mon_table}'")
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
            _interruptible_sleep(shutdown_event, interval)

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
    parser.add_argument("--token_file", default=None,
                        help="read the (bearer) token from this file, trimmed; overrides --token. "
                             "Keeps the token off the command line. Used by http/qwp auth.")
    parser.add_argument("--token_x", default=None)
    parser.add_argument("--token_y", default=None)
    parser.add_argument("--ilp_user", default="admin")
    parser.add_argument("--protocol", choices=["http", "tcp", "qwp"], default="http")
    parser.add_argument("--qwp_tls", type=lambda x: str(x).lower() == 'true', default=False,
                        help="QWP only: use wss (TLS) instead of ws, with tls_verify=unsafe_off")
    parser.add_argument("--durable_ack", type=lambda x: str(x).lower() == 'true', default=False,
                        help="QWP only: request_durable_ack=on so a failover cannot lose acked-but-"
                             "unreplicated rows (Enterprise). Independent of --enterprise.")
    parser.add_argument("--store_forward_dir", default=os.path.join(tempfile.gettempdir(), "fx_qwp_sf"),
                        help="QWP only: base dir for per-worker store-and-forward spill; each worker "
                             "gets a <dir>/fx-<idx> subdir (default: <tmp>/fx_qwp_sf)")
    parser.add_argument("--mode", choices=["real-time", "faster-than-life"], required=True)
    parser.add_argument("--market_data_min_eps", type=int, default=1200)
    parser.add_argument("--market_data_max_eps", type=int, default=15000)
    parser.add_argument("--core_min_eps", type=int, default=700)
    parser.add_argument("--core_max_eps", type=int, default=1000)
    parser.add_argument("--total_market_data_events", type=int, default=1_000_000)
    parser.add_argument("--start_ts", type=str)
    parser.add_argument("--end_ts", type=str)
    parser.add_argument("--processes", type=int, default=1)
    parser.add_argument("--market_data_processes", type=int, default=None,
                        help="Toggle for the market_data table (parity shim for the Java per-pool "
                             "flag). 0 = disable market_data (send only core_price + fx_trades, for "
                             "fast two-table demos); 1 = enable (default). Other values are rejected: "
                             "Python has a single shared pool, so use --processes for parallelism. "
                             "When 0, the stop budget (--total_market_data_events) counts core_price "
                             "events instead.")
    parser.add_argument("--wal_lag_threshold", type=int, default=None,
                        help="Pause ingestion when the monitored table's WAL apply lag exceeds this "
                             "many transactions (default: 3*processes with market_data on; ~10s of "
                             "core_price's commit rate when market_data is disabled).")
    parser.add_argument("--realtime_slice_ms", type=int, default=0,
                        help="Real-time only: split each second into slices of this many ms and "
                             "flush after each, so rows arrive smoothly instead of one burst/sec "
                             "(e.g. 100 = 10 evenly-spaced flushes/sec). 0 (default) = off. Ignored "
                             "in faster-than-life.")
    parser.add_argument("--min_levels", type=int, default=40)
    parser.add_argument("--max_levels", type=int, default=40)
    parser.add_argument("--incremental", type=lambda x: str(x).lower() != 'false', default=False)
    parser.add_argument("--create_views", type=lambda x: str(x).lower() != 'false', default=True)
    parser.add_argument("--short_ttl", type=lambda x: str(x).lower() == 'true', default=False)
    parser.add_argument("--enterprise", type=lambda x: str(x).lower() == 'true', default=False)
    parser.add_argument("--suffix", type=str, default="")
    parser.add_argument("--yahoo_refresh_secs", type=int, default=300)
    parser.add_argument("--orders_min_per_sec", type=int, default=5)
    parser.add_argument("--orders_max_per_sec", type=int, default=30)
    parser.add_argument("--lei_pool_size", type=int, default=2000)
    parser.add_argument("--chunk_seconds", type=int, default=900,
                        help="Max seconds to precompute at once in faster-than-life mode (default: 900 = 15 min)")
    parser.add_argument("--metals", type=lambda x: str(x).lower() == 'true', default=False,
                        help="Add the precious metals pairs XAUUSD, XAGUSD, XPTUSD, XPDUSD and XAUEUR to the "
                             "universe. Total EPS is unchanged, so each FX pair gets 30/35 of its usual rows.")
    parser.add_argument("--forwards", type=lambda x: str(x).lower() == 'true', default=False,
                        help="Create and fill fx_forward_points (+ fx_forward_points_1m with --create_views): "
                             "forward point ladders for the majors and EURJPY, plus the metals with --metals.")
    parser.add_argument("--bad_point_utc", type=str, default="09:20",
                        help="--forwards only: UTC start (HH:MM) of the daily 20-minute planted bad point "
                             "(EURJPY 3M, or XAUEUR 3M with --metals) for the triangular consistency demo.")
    parser.add_argument("--seed", type=int, default=None,
                        help="Seed every random stream so faster-than-life runs are reproducible byte for "
                             "byte (Yahoo brackets aside). Default: unseeded, as before.")

    args = parser.parse_args()
    suffix = args.suffix
    if args.forwards:
        parse_bad_point_utc(args.bad_point_utc)   # validate early
    seed_process(args.seed, "parent", args.mode)

    # Token file wins over inline --token (keeps the token off the command line).
    if args.token_file:
        with open(args.token_file) as tf:
            args.token = tf.read().strip()

    # --market_data_processes is a toggle for the market_data table (parity shim for the Java
    # per-pool count). Python has one shared pool, so only 0 (disable) and 1 (enable) are
    # meaningful; reject anything else. When 0, market_data is dropped and core_price + fx_trades
    # still flow.
    if args.market_data_processes is not None and args.market_data_processes not in (0, 1):
        print("ERROR: --market_data_processes accepts only 0 (disable market_data) or 1 (enable) in "
              "the Python generator. Per-pool worker counts are Java-only; use --processes for parallelism.")
        sys.exit(1)
    args.no_market_data = (args.market_data_processes == 0)

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

    # Validate event rate hierarchy: market_data > core_price > orders.
    # When market_data is disabled, its EPS is unused, so skip that half of the check
    # (demos deliberately push core_price EPS high).
    if not args.no_market_data and args.market_data_min_eps <= args.core_max_eps:
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
    conn_str = f"user={args.user} password={args.password} host={pg_host(args.host)} port={args.pg_port} dbname=qdb"
    with pg.connect(conn_str) as conn:
        # market_data may not exist when disabled (--market_data_processes 0); skip its lookup.
        latest_market_ns = None if args.no_market_data else get_latest_timestamp_ns(conn, table_name('market_data', suffix))
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
    # Shared shutdown flag; Ctrl+C in the parent sets it and every child stops cleanly.
    shutdown_event = mp.Event()
    _install_parent_sigint(shutdown_event)
    refresher_proc = None
    universe = pair_universe(args.metals)
    if args.mode == "real-time":
        # Real-time uses a shared Manager list updated by Yahoo refresher
        fx_pairs = manager.list(universe)
    else:
        # Faster-than-life uses a plain list (no Manager, no refresher)
        fx_pairs = list(universe)

    # Generate LEI pool for counterparties (deterministic and reusable)
    lei_pool = generate_lei_pool(args.lei_pool_size)
    print(f"[INFO] Generated {len(lei_pool)} LEIs for counterparties.")


    if args.incremental:
        if args.mode == "real-time":
            print(f"[ERROR] Incremental load not allowed in real-time, as real-time syncs from yahoo finance.")
            exit(1)
        else:
            state = complete_initial_state(load_initial_state(args, suffix), fx_pairs)
    else:
        if args.mode == "real-time":
            first_ready_event = mp.Event()
            # Start the refresher
            refresher_proc = mp.Process(target=fx_pairs_refresher, args=(fx_pairs, args.yahoo_refresh_secs),
                                        kwargs={'first_ready_event': first_ready_event,
                                                'shutdown_event': shutdown_event})
            refresher_proc.daemon = True
            refresher_proc.start()
            print("[INFO] Waiting for Yahoo FX brackets initial load...", flush=True)
            first_ready_event.wait()
            print("[INFO] FX brackets loaded from Yahoo. Proceeding with ingestion.", flush=True)
            # Initial state is built from the initial brackets
            state = load_initial_state_from_brackets(list(fx_pairs))
        else:
            fx_pairs = fetch_fx_pairs_from_yahoo(universe, bracket_pct=1.0)
            state = load_initial_state_from_brackets(fx_pairs)

    pause_event = Event()
    _buffer_limit = compute_buffer_limit(args)
    _wal_threshold = compute_wal_threshold(args, _buffer_limit)
    print(f"[INFO] Flush buffer_limit={_buffer_limit} rows; WAL lag threshold={_wal_threshold} "
          f"(watching {'core_price' if args.no_market_data else 'market_data'})")
    wal_proc = mp.Process(
        target=wal_monitor,
        args=(args, pause_event, _wal_threshold),
        kwargs={'suffix': suffix, 'shutdown_event': shutdown_event}
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
            # When market_data is disabled, plan tuples carry 0 market events and the stop
            # budget (--total_market_data_events) is measured in core_price events instead.
            market_total = 0 if args.no_market_data else random.randint(args.market_data_min_eps, args.market_data_max_eps)
            core_total = random.randint(args.core_min_eps, args.core_max_eps)
            full_per_second_plan.append((market_total, core_total))
            events_so_far += core_total if args.no_market_data else market_total
        overage = events_so_far - args.total_market_data_events
        if overage > 0 and (max_seconds_from_window is None or len(full_per_second_plan) < max_seconds_from_window):
            market_total, core_total = full_per_second_plan[-1]
            if args.no_market_data:
                full_per_second_plan[-1] = (market_total, core_total - overage)
            else:
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

        # Forwards: the parent owns the model so the rate walk and cadence run continuously
        # across chunks; each chunk's ladders go to worker 0 only (every worker covers every
        # second with a share of the spot events, so one emitter avoids duplicate ladders).
        fwd_model = None
        fwd_bad_ts = []
        if args.forwards:
            fwd_rng = random.Random(None if args.seed is None else _derive_seed(args.seed, "forwards", args.mode))
            fwd_model = ForwardsModel(args.metals, fwd_rng, start_ns, parse_bad_point_utc(args.bad_point_utc))

        for chunk_idx in range(num_chunks):
            chunk_start_sec = chunk_idx * chunk_size
            chunk_end_sec = min((chunk_idx + 1) * chunk_size, total_seconds)
            chunk_seconds = chunk_end_sec - chunk_start_sec

            # Slice the plan for this chunk
            chunk_plan = full_per_second_plan[chunk_start_sec:chunk_end_sec]
            chunk_events = sum((c if args.no_market_data else m) for m, c in chunk_plan)
            chunk_label = "core_price" if args.no_market_data else "market_data"

            print(f"[CHUNK {chunk_idx + 1}/{num_chunks}] Seconds {chunk_start_sec}-{chunk_end_sec - 1}, "
                  f"{chunk_events} {chunk_label} events, starting at {ns_to_iso(chunk_start_ns)}")

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

            fwd_plan = None
            if fwd_model is not None:
                fwd_plan = fwd_model.emissions(chunk_start_ns, effective_end_ns)
                fwd_bad_ts.extend(em[0] for em in fwd_plan if em[5])

            # Spawn workers for this chunk
            pool = []
            for process_idx in range(args.processes):
                p = mp.Process(
                    target=ingest_worker,
                    args=(
                        args,
                        worker_plans[process_idx],
                        sum((c if args.no_market_data else m) for m, c in worker_plans[process_idx]),
                        chunk_start_ns,
                        effective_end_ns,
                        global_states,
                        fx_pairs,
                        process_idx,
                        args.processes,
                        pause_event,
                        global_sec_offsets[process_idx],
                        lei_pool,
                        shutdown_event
                    ),
                    kwargs={'fwd_plan': fwd_plan if process_idx == 0 else None}
                )
                p.start()
                pool.append(p)

            # Wait for all workers to finish this chunk
            for p in pool:
                p.join()

            # Stop launching further chunks if Ctrl+C was pressed mid-chunk.
            if _shutting_down(shutdown_event):
                print("[SHUTDOWN] Stopping after current chunk.", flush=True)
                break

            # Carry forward the final close state for continuity with next chunk
            carry_forward_state = close_per_second[-1]

            # Update start timestamp for next chunk
            chunk_start_ns = chunk_end_ns

            print(f"[CHUNK {chunk_idx + 1}/{num_chunks}] Completed.")

        if fwd_model is not None:
            report_bad_points(fwd_bad_ts, fwd_model.bad_symbol)
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
                lei_pool,
                shutdown_event
            )
        )
        p.start()
        p.join()

    # Stop background processes. Set the flag (already set on a Ctrl+C shutdown, but
    # also needed after a normal finish) so each exits its loop, then join with a short
    # grace period and only hard-terminate if it is still blocked.
    shutdown_event.set()
    for proc in (wal_proc, refresher_proc):
        if proc is None:
            continue
        proc.join(timeout=5)
        if proc.is_alive():
            proc.terminate()
            proc.join()

if __name__ == "__main__":
    main()
