# Forwards and swaps demo script

Talk track for the forwards side of the FX and precious metals demo. Data comes from the
generator with `--metals true --forwards true`; queries are the forwards section of
`demo_queries.sql`; the dashboard is "FX and Metals: Spot and Forwards" (Grafana) or the
equivalent QuestDB Web Console notebook.

## 1. What is in the data

**Table `fx_forward_points`.** Every 5 seconds each covered pair publishes a full forward
ladder, one row per tenor:

| Column | Meaning |
|---|---|
| `symbol` | EURUSD, USDJPY, GBPUSD, USDCHF, AUDUSD, USDCAD, NZDUSD, EURJPY, and the metals XAUUSD, XAGUSD, XPTUSD, XPDUSD, XAUEUR |
| `tenor` | ON, TN, 1W, 2W, 1M, 2M, 3M, 6M, 9M, 1Y |
| `tenor_days` | Calendar days from spot to the value date (negative for ON and TN) |
| `value_date` | The settlement date |
| `bid_points`, `ask_points` | Forward points in pips |
| `spot_ref` | The spot the ladder was priced on |
| `source` | `FWD_FEED` |

**View `fx_forward_points_1m`.** Last points per pair, tenor and minute: the history and
checking layer, refreshed incrementally as data arrives.

## 2. The tenors

Spot settles 2 business days after today. Each tenor is a settlement date relative to spot.

| Tenor | Meaning |
|---|---|
| ON (overnight) | Swap from today to tomorrow |
| TN (tom-next) | Swap from tomorrow to spot |
| 1W, 2W | 1 or 2 weeks after spot |
| 1M, 2M, 3M, 6M, 9M | 1 to 9 months after spot |
| 1Y | 1 year after spot |

ON and TN settle **before** spot, so their outrights are spot **minus** the points. All other
tenors: outright = spot + points x pip.

**Say:** "Forward points are how much more or less you pay to settle on a later date instead of
at spot. They come from the interest-rate gap between the two currencies; for gold, between the
dollar rate and gold's lease rate."

## 3. Where the numbers come from

**In this demo:** synthetic, generated alongside the spot data. Each currency has a short-rate
curve anchored to real policy rates (October 2026: Fed about 3.9%, ECB 2.5%, BoJ 1.25%...);
metals use lease rates (the cost of borrowing the metal). Points = spot x the rate difference
over the tenor's days, with a small drift so they move. Spot is the generator's own live price,
so forwards line up with `core_price`. Gold shows contango (positive points rising with tenor),
platinum and short-dated silver show backwardation.

**In real life** the ladder comes from:
- the bank's own forwards engine (spot plus deposit and OIS curves), the same approach as here;
- dealer streams and platforms (360T, FXall, Bloomberg FXGO, direct bank feeds);
- interbank swap brokers (TP ICAP, BGC), the reference for swap points;
- vendors (Bloomberg, LSEG), composite ladders for marking;
- for metals, bullion banks around the LBMA market and COMEX futures.

**Say:** "In production this table would be fed by the bank's forwards engine or by vendor and
platform streams, often several at once. QuestDB stores them side by side and checks them
against each other."

## 4. The queries (forwards section of `demo_queries.sql`)

| # | Query | QuestDB features | Say |
|---|---|---|---|
| 1 | **Forward curve now**: points and outrights for every tenor | `LATEST ON ... PARTITION BY tenor`, `LATEST ON` for spot, running-sum window function for ON/TN, `CROSS JOIN`, `DECLARE` | "The whole curve in one query, consistent with the spot the desk sees." |
| 2 | **Forward curve as of T** | Same, with `WHERE timestamp <= @asof` before `LATEST ON` | "Any instant in the past, same query." |
| 3 | **Implied carry and lease rate**: the rate the forward implies per tenor | `LATEST ON`, `lead()` window function, `JOIN` to an inline rate table | "What the forward says the metal costs to borrow." Gold: about 0.05% overnight to 0.85% at 1Y. |
| 4 | **Swap points**: far tenor minus near tenor, bid and ask on the right sides, annualised rate | `LATEST ON`, `CROSS JOIN` of the two legs | "A swap is two tenors subtracted; nothing extra to store." |
| 5 | **Triangular consistency**: EURJPY vs EURUSD x USDJPY, XAUEUR vs XAUUSD / EURUSD, minute by minute | Materialized view (`SAMPLE BY 1m`), conditional aggregation, `timestamp_floor` | "A bad price in one feed, caught by the other two." |
| 6 | **Points history vs spot**: 3M points next to spot over the day | **`ASOF JOIN`** of the 1-minute views | "The forward moving with its spot." |

Highlights:
- `LATEST ON` + `WHERE timestamp <= @asof`: the full curve at any instant, one query.
- `ASOF JOIN`: line the forward up with the spot that was valid at that moment.
- Materialized view: the minute-by-minute check stays cheap over a whole day.

(`HORIZON JOIN` is not used here; it is in the spot section, for trade markouts.)

## 5. The planted bad point

Once a day, **09:20 to 09:40 UTC**, the **XAUEUR 3M** point is published about **6 pips** away
from what XAUUSD and EURUSD imply (EURJPY 3M when metals are off). It is a feed error, not a
manual mark. Query 5 over `$today` returns exactly that one window and nothing else.

**Say:** "Nobody had to flag it. The consistency check found the only window where one feed
disagreed with the other two."

## 6. Dashboard tips

- Select **XAUUSD**. Market Depth, top of book and the 1-minute OHLC are the same panels as the
  FX dashboard.
- **Forward Curve** ticks with spot (outrights = live spot + points).
- **3M Points vs Spot** shows every ladder of the last 15 minutes; both lines move together.
- **Swap Ladder** shows calm swap points (as in the real market) with far outrights that move
  with spot.
- **Triangular Consistency Gap** sits near zero by design. To show the planted point turn a bar
  red, set the time range to end between 09:20 and 09:40 UTC today (or set `@asof` there in the
  QuestDB notebook).
