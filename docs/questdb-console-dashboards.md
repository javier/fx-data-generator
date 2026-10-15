# QuestDB Web Console dashboards

The two demo dashboards as native QuestDB Web Console notebooks (grid layout). They are ports of
the Grafana dashboards `grafana_dashboard.json` and `fx_forwards_grafana_dashboard.json`, and need
**QuestDB 10.0 or later, or QuestDB Enterprise 4.0 or later**.

- `questdb-fx-orderbook-realtime-demo-dashboard.json`: FX Orderbook - Realtime Demo
- `questdb-fx-and-metals-spots-and-forwards-dashboard.json`: FX and Metals: Spot and Forwards

For the exact SQL of each cell, open the notebook in the console.

## Shared conventions

### Notebook variables

The console injects the variables as a `DECLARE` block into every statement; edit them in the
Variables popover. They replace Grafana's time picker and `$SYMBOL` dropdown.

| Variable | Default | Purpose |
|---|---|---|
| `@asof` | `now()` | End of every time window. Set a fixed timestamp (e.g. `'2026-10-15T08:00:00Z'::timestamp`) to replay a past moment, or `dateadd('d', -1, now())` to replay at live speed with a delay. |
| `@from` | `dateadd('h', -6, @asof)` | The 6-hour window from Grafana's dashboard-level range. Only the Triangular Consistency Gap uses it; every other cell has its own window, matching Grafana's per-panel overrides. |
| `@symbol` | `'EURUSD'` (order book), `'XAUUSD'` (forwards) | Instrument shown by every symbol-specific cell. |

Forwards dashboard only, derived from `@symbol` so they are right for FX, JPY pairs and metals:

| Variable | Definition |
|---|---|
| `@pip` | 0.01 for `%JPY`, 0.001 for XAGUSD, 0.01 for other `X%` metals, otherwise 0.0001 |
| `@dp` | Display decimals for outright prices: 4 for JPY pairs and metals, 5 for XAGUSD, otherwise 6 |
| `@quote` | Quote currency, `right(@symbol, 3)` |
| `@base_basis` | Day-count basis: 365 when the base currency is GBP, otherwise 360 |

### Source tables and views

- `market_data`: order book snapshots. `bids`/`asks` are `DOUBLE[][]` with row 1 = prices and
  row 2 = sizes per level; also `best_bid`/`best_ask`. Depth cells cumulate sizes with
  `array_cum_sum`.
- `core_price`: top of book per ECN (bid/ask price and volume, `ecn`).
- `market_data_ohlc_1m`, `market_data_ohlc_15m`: materialized views of `best_bid` OHLC plus
  `total_volume`.
- `bbo_1s` (refreshes immediately) and `bbo_1h` (refreshes every 10 minutes).
- `fx_forward_points` and its 1-minute materialized view `fx_forward_points_1m`: forward points
  per symbol and tenor (`bid_points`, `ask_points`, `spot_ref`, `tenor_days`, `value_date`).

### Formatting

- Time columns are text: timestamps via `to_str()` as `HH:mm:ss` or `HH:mm:ss.SSS`, value dates
  as `yyyy-MM-dd`, so tables fit at 1080p.
- Spread and rates use fixed decimals: `DECIMAL(18,5)` for spread, `DECIMAL(18,3)` for rate
  percentages. Volumes are cast to `long`.
- Outright prices are text: rounded to `@dp`, cast to `DECIMAL(18,6)`, trailing zeros removed
  with `regexp_replace(..., '\.?0+$', '')`.
- Highlight rules never depend on price level: they compare with the previous refresh, use
  colour steps, or use gradients between the column's current min and max, so they work for any
  symbol.

## Dashboard 1: FX Orderbook - Realtime Demo

Notebook refresh default: 250 ms.

| Cell | Position | Window | Logic | Display | Refresh |
|---|---|---|---|---|---|
| Market Depth (pips from mid) | row 1, left half | latest book in the last 10m | `LATEST ON` the book, mid = (best bid + best ask) / 2, pip from `@symbol`; unnests prices with `array_cum_sum(sizes)` per side; x = (price - mid) / pip | stepArea, `bid_depth` / `ask_depth` | 250 ms |
| OHLC - Bids - 1m | row 1, right half | last 1 minute | 1-second candles of `bids[1,1]` from `market_data`, plus a second query with summed level-1 bid size | candlestick, volume bars on the right axis | 1 s |
| Spread and Volume per second | row 2, left third | 1 minute ending 5 s ago | `SAMPLE BY 1s`, latest 6 complete seconds; spread = avg(best ask - best bid); bid/ask volume = level-1 price x size | table; Spread blue to purple, volumes teal to blue | 250 ms |
| Top-of-book Price vs Core Price | row 2, right two thirds | last 1 minute | latest 6 `core_price` ticks, each `ASOF JOIN`ed to the book; `insertion_point(bids[2], bid_volume)` finds the matching book level | table; Level 1 green, 2 to 3 amber, 4+ red | 250 ms |
| VWAP - RSI 12h - Bollinger Bands (20,2) | row 3, left half | today so far, from `timestamp_floor('d', @asof)` | 4 queries: 15m candles (`market_data_ohlc_15m`); cumulative VWAP from 1m bars; RSI from `bbo_1h` (12-period averages of gains and losses); Bollinger 20-period SMA +/- 2 sigma on 15m closes | candlestick; VWAP and Bollinger on the left axis, RSI on a right axis fixed 0 to 100 | 1 min |
| Bid vs Ask Volume (30s) - BBO (1s) | row 3, right half | last 30 minutes | BBO bid/ask/mid from `bbo_1s`; second query: 30 s summed level-1 bid volume, ask volume negative | lines; prices left axis, volumes right axis | 1 s |

## Dashboard 2: FX and Metals: Spot and Forwards

Notebook refresh default: 250 ms.

| Cell | Position | Window | Logic | Display | Refresh |
|---|---|---|---|---|---|
| Market Depth (pips from mid) | row 1, left half | latest book in the last 10m | as dashboard 1, using `@pip` | stepArea | 500 ms |
| OHLC - Bids - 1m | row 1, right half | last 1 minute | as dashboard 1 | candlestick plus volume bars | 250 ms |
| Spread and Volume per second | row 2, left third | 1 minute ending 5 s ago | as dashboard 1 (not in the Grafana original; added to fill the row) | table, same gradients | 250 ms |
| Top-of-book Price vs Core Price | row 2, right two thirds | last 1 minute | as dashboard 1 | table, Level steps | 250 ms |
| Forward Curve (points in pips) | row 3, left half | latest point per tenor at `@asof` | latest `fx_forward_points` per tenor plus latest `core_price` spot; out bid/ask = spot +/- points x `@pip`, with ON/TN (before spot) cumulated backwards; Impl % from an inline quote-currency rate curve (USD/EUR/JPY/CHF/CAD by tenor), `@base_basis` and the actual days | table; Bid/Ask points flash green up / red down (by Tenor); Impl % teal to purple | 500 ms |
| Swap Ladder from Spot | row 3, right half | latest point per tenor at `@asof` | spot-to-tenor swaps for tenors after spot: swap points, implied swap rate % (360 basis), far outright bid/ask | table; Bid/Ask swap flash green/red (by Far); rate gradient | 1 s |
| 3M Points vs Spot | row 4, left half | last 15 minutes | two queries on `fx_forward_points`, tenor 3M: `spot_ref`, and mid points | lines; spot left axis, 3M points right | 1 s |
| Triangular Consistency Gap (pips) | row 4, right half | `@from` to `@asof`, last completed minute only | from `fx_forward_points_1m`, per tenor: how far EURJPY and XAUEUR points are from what EURUSD x USDJPY and XAUUSD / EURUSD imply; always those five symbols, whatever `@symbol` is | bar chart by tenor, series EURJPY and XAUEUR | 1 min |

To show the planted bad point (XAUEUR 3M, 09:20 to 09:40 UTC daily), set `@asof` to a moment in
that window.

## Differences from the Grafana dashboards

- **Depth chart axis:** Grafana plots absolute price with Plotly; here the x axis is pips from
  the mid, so the book stays still. The console redraws the chart on every refresh, which is
  slower than Grafana's in-place updates.
- **Spread and Volume** cell added to the forwards dashboard.
- **RSI:** Grafana also plots the hourly bid line; here only RSI is shown.
- **Indicators volume:** Grafana shows volume bars under the 15m candles; this version does not.
- **Table colours and decimals:** Grafana uses per-column decimals and background shading; here
  formatting is done in SQL and colours come from the console's highlight rules.
- **Titles** don't include the symbol: Grafana's `${SYMBOL}` in titles has no console
  equivalent.
