# Java ⇄ Python generator interchangeability — change spec

**Goal.** Make the Java QWP generator and the Python `fx_data_generator.py` produce a
*single consistent dataset* on shared tables, so a demo can ingest with **either**
engine (backfill with one, continue or go live with the other) and downstream
tables, views, and price series stay coherent.

**Authority.** Python is authoritative. Every divergence is resolved by making Java
match Python, never the reverse.

**Out of scope.** Identical future price *paths* (RNG differs by design — only the
*character* of the data and *seam continuity* must match). Performance/throughput
parity. No new demo-only or benchmark data modes.

---

## Accepted behavioral contract (not a bug)

With incremental disabled in real-time on both engines, any **faster-than-life →
real-time** transition snaps to live Yahoo quotes — a one-step price move at that
seam, identical on both engines, by design ("real matches real quotes").
Seam continuity is preserved for **FTL → FTL** (incremental seed) and
**real-time → real-time** (Yahoo + wall-clock + timestamp resume).

---

## Change 1 — Prefix option + `qwp_trades` → `qwp_fx_trades`

**Why.** Python's trades table is `fx_trades` (not `trades`); a single prefix can't
reach it unless the trades *stem* is `fx_trades`. A configurable prefix lets the
Java tables collapse onto the exact Python names when empty.

**Design (verbatim prefix, mirroring the verbatim-suffix rule).**
- New CLI flag `--prefix <s>`, **default `qwp_`** (includes the trailing underscore,
  so existing names `qwp_market_data` / `qwp_core_price` are preserved and trades
  becomes `qwp_fx_trades`). Empty `--prefix ""` yields the Python names.
- Prefix concatenates verbatim, exactly like `suffix`. The user supplies any
  separator.

**Edits — `Cli.java`:**
- Add field `public String prefix = "qwp_";` next to `suffix` (line ~78).
- Parse `case "prefix": c.prefix = req(args, ++i, raw); break;` next to the `suffix`
  case (line ~221).
- Rewrite the three accessors (lines 317–326):
  ```java
  public String tradesTable()     { return prefix + "fx_trades"    + suffix; }
  public String marketDataTable() { return prefix + "market_data"  + suffix; }
  public String corePriceTable()  { return prefix + "core_price"   + suffix; }
  ```
- Update `--help` text (line ~490) and add a `--prefix` line.

**Name matrix after the change:**

| stem | `--prefix qwp_` (default) | `--prefix ""` (Python parity) |
|---|---|---|
| trades | `qwp_fx_trades` | `fx_trades` |
| market data | `qwp_market_data` | `market_data` |
| core price | `qwp_core_price` | `core_price` |

**⚠️ Confirm:** default prefix is `qwp_` *with* the underscore. If you'd rather the
flag value be the bare token `qwp` and have the code insert the underscore, say so —
but verbatim (so the user owns the separator) is consistent with how `suffix`
already works.

---

## Change 2 — Port Python's exact 11 materialized views

**Why.** Tables/views are `IF NOT EXISTS`; the first generator to run dictates the
definition and the second silently inherits it. Consistency requires the Java DDL to
be **byte-identical** to Python's (modulo prefix/suffix substitution).

**Current Java (3 views, to be replaced):** `market_data_ohlc_1m`,
`market_data_ohlc_15m`, `bbo_1h` — different refresh policies and cascade bases than
Python. Replace the whole `createViews(...)` set (QwpTradesGenerator.java ~lines
1016–1039) with the 11 below.

**Target set (names shown with PREFIX/SUFFIX placeholders):**

From `core_price`:
- `{P}core_price_1s{S}` — `SAMPLE BY 1s`, `PARTITION BY HOUR`
- `{P}core_price_1d{S}` — `REFRESH EVERY 1h DEFERRED START '2025-06-01T00:00:00.000000Z'`, `SAMPLE BY 1d`, `PARTITION BY MONTH`
  (both: `first/max/min/last` of `(bid_price+ask_price)/2` as open/high/low/close_mid,
  `last(ask_price)-last(bid_price)` as last_spread, plus max/min/avg of bid and ask)

From `market_data`:
- `{P}bbo_1s{S}` — `last(best_bid) bid, last(best_ask) ask`, `SAMPLE BY 1s`, `PARTITION BY HOUR`
- `{P}bbo_1m{S}` — base `bbo_1s`, `max(bid) bid, min(ask) ask`, `REFRESH EVERY 1m DEFERRED START '2025-06-01…'`, `SAMPLE BY 1m`, `PARTITION BY DAY`
- `{P}bbo_1h{S}` — base `bbo_1m`, same agg, `REFRESH EVERY 10m DEFERRED START …`, `SAMPLE BY 1h`, `PARTITION BY MONTH`
- `{P}bbo_1d{S}` — base `bbo_1h`, same agg, `REFRESH EVERY 1h DEFERRED START …`, `SAMPLE BY 1d`, `PARTITION BY MONTH`
- `{P}market_data_ohlc_1m{S}` — `first/max/min/last(best_bid)`, `SUM(bids[2][1]) total_volume`, `SAMPLE BY 1m`, `PARTITION BY HOUR`
- `{P}market_data_ohlc_15m{S}` — base `…_ohlc_1m`, `first(open)/max(high)/min(low)/last(close)`, `SUM(total_volume)`, `SAMPLE BY 15m`, `PARTITION BY HOUR`
- `{P}market_data_ohlc_1d{S}` — `first/max/min/last(best_bid)`, `SUM(bids[2][1])`, `REFRESH EVERY 1h DEFERRED START …`, `SAMPLE BY 1d`, `PARTITION BY MONTH`

From `fx_trades`:
- `{P}fx_trades_ohlc_1m{S}` — `first/max/min/last(price)`, `SUM(quantity) total_volume`, `SAMPLE BY 1m`, `PARTITION BY HOUR`
- `{P}fx_trades_ohlc_1d{S}` — base `…_ohlc_1m`, `first(open)/max(high)/min(low)/last(close)`, `SUM(total_volume)`, `REFRESH EVERY 1h DEFERRED START …`, `SAMPLE BY 1d`, `PARTITION BY MONTH`

**Rules:**
- Reproduce Python's exact column aliases, `REFRESH`/`DEFERRED START` clauses,
  `PARTITION BY`, and cascade base tables. The canonical source is
  `fx_data_generator.py` lines 454–594 — copy from there, do not paraphrase.
- The bbo and `…_15m`/`…_1d` cascades must reference the *prefixed/suffixed* base
  view name, not the raw table.
- Apply TTL exactly as Python does (see Change 7).
- Gate behind the existing `--create_views`; for a clean demo, let **one** engine own
  view creation (see Guardrails).

---

## Change 3 — Incremental seed from `core_price`, full state (not mid-only from trades)

**Why.** Python's `load_initial_state` seeds **bid, ask, spread, indicator1,
indicator2** from `core_price LATEST BY symbol`. Java's `seedInitialMidFromDb`
(QwpTradesGenerator.java lines 1402–1442) reads only `price` from the **trades**
table and restores `initialMid` alone — spread and indicators restart fresh, which
would step at the seam.

**Edits:**
1. Change the query (line 1411) to:
   ```sql
   SELECT symbol, bid_price, ask_price, indicator1, indicator2
   FROM <corePriceTable()> LATEST ON timestamp PARTITION BY symbol
   ```
   and update the log line (1404) to name `cfg.corePriceTable()`.
2. Add initial-state arrays alongside `initialMid` (line 72/95):
   `initialSpreadPips[]`, `initialInd1[]`, `initialInd2[]`.
3. In the batch handler, reconstruct exactly as Python:
   - `mid = (bid_price + ask_price) / 2` → `initialMid[i] = quantizeToPip(mid, pip, precision)`
   - `spreadPips = round((ask_price - bid_price) / pip)`, clamped 1..8 → `initialSpreadPips[i]`
   - `initialInd1[i] = indicator1`, `initialInd2[i] = indicator2`
   - keep the bracket reset (`resetBracket(mid*0.99, mid*1.01)`) but base it on the mid.
4. Wire the new arrays into worker init where the walk currently starts from defaults:
   - `walkMid[j] = initialMid[g]` already exists (line 406).
   - seed the spread walk from `initialSpreadPips[g]` (currently starts at a default).
   - seed `walkInd1[j]/walkInd2[j]` from `initialInd1[g]/initialInd2[g]`
     (currently start at the default ~0.5).

**Fallback:** if `core_price` has no rows for a symbol, fall back to the
bracket/default exactly as today (`initialMid[i] <= 0` path, line 137).

---

## Change 4 — Disallow `--incremental` in real-time

**Why.** Python errors on incremental + real-time and reseeds from Yahoo
(`fx_data_generator.py` line 1304). Java currently *allows* it. Make Java symmetric.

**Edit (Cli.java validation, near the other `fail(...)` checks ~lines 283–291):**
```java
if (realtime && incremental) {
    fail("--incremental is not allowed in real-time mode (real-time syncs from Yahoo).");
}
```
Keep the existing `realtime && !noYahoo && !incremental` guard on the Yahoo refresher
(QwpTradesGenerator.java line 162) — it becomes simply `realtime && !noYahoo` once the
above rejects the incompatible combo, but leaving it is harmless and defensive.

---

## Change 5 — Real-time mid-walk drift 7.0 (match Python)

**Why.** Python evolves the mid with **drift 7.0 pips/sec in real-time**
(`evolve_state_one_tick(..., 7.0)`, line 1076) and **5.0 in FTL**. Java uses **5.0 at
both** call sites, so live Java prices wiggle calmer than live Python.

**Edit (QwpTradesGenerator.java):** the two `beginSecond(k, 5.0)` calls (lines 441 and
469) — the real-time call site must pass `7.0`; the FTL call site stays `5.0`.
Confirm which of 441/469 is the real-time path and change only that one. Shock prob
(1%) and magnitude (±20 pip) already match — no change there.

---

## Change 6 — Fix stale `FxUniverse` Javadoc

**Why.** The class header (FxUniverse.java lines 14–18) says "There is no order book
and no `market_data`/`core_price` table." That is outdated — Java creates and emits
both. Rewrite the paragraph to describe the current three-table model so the doc
doesn't mislead future readers.

---

## Change 7 — Retention/TTL parity (so schemas match under all flags)

**Why.** `IF NOT EXISTS` means the first creator's retention clause wins. To keep
schemas identical regardless of which engine runs first:
- `market_data` / `core_price`: Python applies `STORAGE POLICY(TO REMOTE 1 hour, TO
  PARQUET 2 days, DROP LOCAL 3 months)` under `--enterprise`, else `TTL 3 DAYS` under
  `--short_ttl`. Java currently uses `TTL 3 DAYS`. Align Java's base-table DDL to the
  same conditional.
- `fx_trades`: both `TTL 1 MONTH` (enterprise → storage policy). Confirm Java matches.
- Matviews: Python and Java both use `TTL` (storage policy isn't supported on
  matviews). Match Python's per-view TTL.

**Lower priority** for pure data-content demos (retention tiering doesn't change rows),
but required if "consistent dataset" includes identical table definitions. Flagging
explicitly so it's a conscious choice, not an accident.

---

## Operational guardrails (demo runbook, not code)

1. **Match the EPS / orders rates across engines (most important).** Per-second row
   counts come from `--core_min_eps/--core_max_eps`,
   `--market_data_min_eps/--market_data_max_eps`, `--orders_min_per_sec/--orders_max_per_sec`
   and `--min_levels/--max_levels` — **not** from the existing data. If the continuing
   engine runs at different EPS, row *density* steps at the seam even though prices stay
   continuous. (Observed in validation: a low-EPS Python backfill continued by Java at
   default EPS jumped ~150× in rows/sec. Prices were continuous; density was not.) Run
   both engines with identical rate flags.
2. **One view owner per demo.** Even with byte-identical DDL, let a single engine
   create the views in any given environment to remove all risk of a first-creator
   mismatch.
3. **Clean-backfill assumption.** The FTL→continuation handoff assumes the backfill
   completed so per-table maxes are aligned. The per-pool resume fix absorbs minor
   skew; a hard-killed backfill can still leave one table behind. Let backfills finish
   before switching engines.
4. **Same `--prefix`/`--suffix`/`--enterprise`/`--short_ttl` on both engines** in a
   shared environment, or they won't address the same tables / same schema.

---

## Acceptance criteria

A run is "interchangeable" when all hold:
1. With `--prefix ""`, Java writes to `fx_trades`, `market_data`, `core_price` and the
   11 views with names identical to Python's.
2. Backfill (FTL) with engine A, continue (FTL, `--incremental`) with engine B →
   **zero gaps** and `open == previous close` at the seam for bid/ask/spread/indicators
   (verify with `SAMPLE BY 1s FILL(NULL)` + `WHERE col IS NULL`, and a boundary
   inspection across the seam second).
3. `--incremental` + real-time is rejected by both engines with the same intent.
4. Live (real-time) data from either engine shows the same per-second volatility
   character (drift 7.0).
5. `schema`/`SHOW CREATE TABLE` for all tables and `SHOW CREATE MATERIALIZED VIEW` for
   all views are identical (modulo prefix/suffix) whichever engine created them.

## Verification plan
- Unit/smoke: two-step FTL cap → resume (already exercised for the resume fix), now
  also asserting bid/ask/spread/indicator seam continuity, not just timestamps.
- Cross-engine: Python FTL backfill of a fixed window → Java FTL `--incremental`
  continuation on the same tables → gap + seam checks above.
- DDL diff: dump `SHOW CREATE …` from a Python-first run and a Java-first run; diff.

## Resolved decisions
1. Default `--prefix` is `qwp_` (verbatim, like `--suffix`); `--prefix ""` yields the
   exact Python names. Trades stem renamed to `fx_trades` so empty prefix gives
   `fx_trades` (not `trades`).
2. Retention parity (Change 7) is **in scope** — TTL **and** STORAGE POLICY must match
   Python (storage policy on base tables under `--enterprise`; per-view TTL on matviews).
3. `--create_views` default flipped to **true**, matching Python.

## Validation status — DONE (local QuestDB)
All changes implemented on branch `jv/qwp_python_parity`; compiles clean. Smoke-tested
against a local QuestDB:
- **Isolated DDL:** a throwaway-prefix FTL run created all 3 tables + **all 11 views**
  without error, and the views populated.
- **Cross-engine seam:** a real Python backfill (00:00–13:00) continued by Java
  (`--prefix "" --incremental`) resumed all three pools contiguously from 13:00:00 with
  **zero gap-seconds** in `fx_trades`/`market_data`/`core_price`; `core_price` mid
  continuity ≤ 0.5 pip (pip-quantization only); the existing Python matviews
  incrementally refreshed across the seam.
- **Findings folded into the runbook:** EPS must be matched across engines (guardrail
  1); the `market_data` best_bid realigns to the `core_price` seed within a few pips at
  the seam (same on both engines).
