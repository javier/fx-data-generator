tables();


--show tables;
-- dataset intro
select * from market_data order by timestamp desc;
select * from core_price order by timestamp desc;
select * from fx_trades order by timestamp desc;

select * from market_data  where timestamp in '$yesterday';
select * from core_price where timestamp  in '$yesterday';
select * from fx_trades where timestamp in '$yesterday';

select * from market_data where timestamp in '$now-4h..$now';

select * from market_data
where symbol in 'GBPUSD' and timestamp in  '$today'
limit -10;

select * from fx_trades
where symbol in 'GBPUSD'
and  timestamp IN '$yesterday#XNYS'
limit -1
;

select first(timestamp), last(timestamp) from fx_trades
WHERE symbol in 'GBPUSD'
and timestamp IN '$yesterday#XNYS'
--and timestamp IN '2026-04-22#XNYS'
;


select timestamp, symbol,
    bids[1,1] as bprice, bids[2,1] as bvolume,
    asks[1,1] as aprice, asks[2,1] as avolume,
    bids[1,-1] as bprice2, bids[2,39] as bvolume2,
    asks[1,-1] as aprice2, asks[2,39] as avolume2,
    array_sum(bids[2]) as total_volume
from market_data where timestamp  in '$today' limit -20;


-- latest on. SQL extensions

select * from core_price latest by symbol;

select * from core_price latest by symbol, ecn;

select * from core_price latest by symbol, ecn
where timestamp < '2026-06-10';


-- parquet
table_partitions('market_data');
table_partitions('core_price');
table_partitions('fx_trades');


select timestamp, count(), symbol,
    avg(bid_price) as bprice, avg(ask_price) as aprice
from core_price  sample by 1d;


with parts as (
    select name, last(isParquet) from table_partitions('core_price')
), totals as (
select timestamp, count(),
    avg(bid_price) as bprice, avg(ask_price) as aprice
from core_price
WHERE symbol = 'GBPUSD' sample by 1h
)
select * from totals join parts ON to_str(timestamp, 'yyyy-MM-ddTHH') = name;


read_parquet('trades.parquet');

select timestamp, count() from (select * from (read_parquet('trades.parquet') order by timestamp) timestamp(timestamp) )
sample by 1d;


-- 15 minutes candles
  select timestamp, symbol,
            first(best_bid) as open,
            max(best_bid) as high,
            min(best_bid) as low,
            last(best_bid) as close,
            avg(best_bid) as avgr,
            sum(bids[2][1]) as volume
        from market_data
        where timestamp  in '$yesterday'
        and   symbol = 'GBPUSD'
        sample by 1m;

select * from market_data
        where timestamp  in '2026-05';

-- mat views
CREATE MATERIALIZED VIEW IF NOT EXISTS 'market_data_ohlc_1m' WITH BASE 'market_data' REFRESH IMMEDIATE AS (
            SELECT timestamp, symbol,
                first(bids[1][1]) AS open,
                max(bids[1][1]) AS high,
                min(bids[1][1]) AS low,
                last(bids[1][1]) AS close,
                SUM(bids[2][1]) AS total_volume
            FROM market_data
            SAMPLE BY 1m

) PARTITION BY HOUR
TTL 1y
OWNED BY 'admin';

  materialized_views();

select * from market_data_ohlc_1m where
symbol = 'GBPUSD' AND timestamp in '$today' order by timestamp desc;

select timestamp, avg(open), last(close) from market_data_ohlc_1m
where timestamp in '$now - 2m..$now';

select * from bbo_1s
where timestamp in '$today'
order by timestamp desc, symbol asc ;

select * from market_data_ohlc_1m
where timestamp in '$today'
order by timestamp desc, symbol asc ;



CREATE MATERIALIZED VIEW IF NOT EXISTS 'bbo_1s' WITH BASE 'market_data' REFRESH IMMEDIATE AS (
            SELECT timestamp, symbol,
                last(bids[1][1]) AS bid,
                last(asks[1][1]) AS ask
            FROM market_data
            SAMPLE BY 1s

) PARTITION BY HOUR
TTL 3d
OWNED BY 'admin';






CREATE MATERIALIZED VIEW IF NOT EXISTS 'bbo_1m' WITH BASE 'bbo_1s' REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM bbo_1s
            SAMPLE BY 1m

) PARTITION BY DAY
TTL 1y
OWNED BY 'admin';

select * from bbo_1m
where timestamp in '$today'
order by timestamp desc, symbol asc ;

CREATE MATERIALIZED VIEW IF NOT EXISTS 'bbo_1h' WITH BASE 'bbo_1m' REFRESH EVERY 10m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM  bbo_1m
            SAMPLE BY 1h

) PARTITION BY MONTH
TTL 1y
OWNED BY 'admin';


-- create view with overridable variables
CREATE OR REPLACE VIEW single_pair AS (
DECLARE
  OVERRIDABLE @pair := 'EURUSD' ,
  OVERRIDABLE @range := '$today'
select * from market_data where timestamp in @range and symbol = @pair
);



SELECT * from single_pair;



DECLARE
  OVERRIDABLE @pair := 'GBPUSD' ,
  OVERRIDABLE @range := '$now-10m..$now'
  SELECT * from single_pair;



-- Array dimensions
select timestamp, symbol,
    bids[1] as bprices, bids[2] as bsizes, array_count(bids[1]) as bid_levels,
    asks[1] as aprices, asks[2] as asizes, array_count(asks[1]) as ask_levels
from market_data latest by symbol;


-- spread
SELECT timestamp, symbol,
    best_ask - best_bid
FROM market_data
where symbol IN ('GBPUSD', 'EURUSD')
and timestamp in '$today';

-- spread
SELECT timestamp, symbol,
    asks[-1][1] - bids[-1][1]
FROM market_data
where symbol IN ('GBPUSD', 'EURUSD')
and timestamp in '$today';

-- moving averages
select timestamp, symbol, best_bid as l1_bid_price,
avg(best_bid) over (partition by symbol order by timestamp) as moving_l1_bid_price,

bids[2,1] as l1_bid_volume, sum(bids[2,1]) over (partition by symbol order by timestamp) as moving_l1_bid_volume,
 sum(bids[2,1]) over (order by timestamp) as moving_l1_total_bid_volume
from market_data
where timestamp in '$today'
and symbol='GBPUSD'
;


-- basic anomaly detection. Ask further from average than a given multiple of stddev
DECLARE
    @l1_ask := best_ask,
    @low_threshold := 1.9,
    @medium_threshold := 2.0,
    @high_threshold := 2.1
WITH s AS (
    SELECT avg(@l1_ask) as avg_ask, stddev(@l1_ask ) as stddev_ask
    FROM market_data
    where symbol IN ('GBPUSD')
    and timestamp IN '$now-1h..$now'
), combined AS (
SELECT timestamp, @l1_ask as l1_ask, avg_ask, stddev_ask, abs(l1_ask - avg_ask) as delta
FROM market_data m CROSS JOIN s
where m.symbol IN ('GBPUSD')
and m.timestamp IN '$now-1h..$now'
)
SELECT timestamp, l1_ask, avg_ask, delta,
    CASE
        WHEN delta >= stddev_ask * @high_threshold THEN 'High'
        WHEN delta >= stddev_ask * @medium_threshold THEN 'Medium'
        ELSE 'Low'
    END AS anomaly
 from combined where delta >= stddev_ask * @low_threshold;



-- Volume is available within 1% of the best price?
-- How much volume I can capture at a cheap price because of a relatively flat orderbook
DECLARE
    @prices := asks[1],
    @volumes := asks[2],
    @best_price := @prices[1],
    @multiplier := 1.01,
    @target_price := @multiplier *  @best_price,
    @relevant_volume_levels := @volumes[1:insertion_point(@prices, @target_price)]
SELECT timestamp, asks,
     @relevant_volume_levels as volume_levels,
     array_sum(@relevant_volume_levels) as total_volume
    FROM market_data where timestamp in '$today' AND symbol = 'GBPUSD';

-- Equivalent query without declare. Volume is available within 1% of the best price?
SELECT asks,
     asks[2, 1:insertion_point(asks[1], 1.01 * asks[1, 1])] volume_levels,
     array_sum(asks[2, 1:insertion_point(asks[1], 1.01 * asks[1, 1])]) total_volume
    FROM market_data where timestamp in '$today' AND symbol = 'GBPUSD' ;

-- What price level will a buy order for the given volume reach?
WITH
    q1 AS (
    SELECT timestamp, symbol, asks,
        array_cum_sum(asks[2]) cum_volumes
    FROM market_data
    where symbol = 'GBPUSD' and timestamp in '$today'),
    q2 AS (
    SELECT timestamp, symbol,
        asks, cum_volumes,
        insertion_point(cum_volumes, 1_500_000, true) target_level
        FROM q1 )
SELECT timestamp, symbol,
    cum_volumes, target_level, asks[1, target_level] price
FROM q2;

select * from core_price where timestamp in '$yesterday';
select * from fx_trades where timestamp in '$yesterday';
select * from market_data where timestamp in '$yesterday';

select * from core_price
    asof join market_data on symbol
    where core_price.symbol = 'GBPUSD'
        and core_price.timestamp in '$yesterday';

select * from fx_trades
    asof join core_price on symbol
    asof join market_data on symbol
    where fx_trades.symbol = 'GBPUSD'
        and fx_trades.timestamp in '$yesterday';

-- Use ASOF JOIN to pair each trade with the most recent order book snapshot,
-- then calculate slippage in basis points
SELECT
    t.timestamp,
    t.symbol,
    t.ecn,
    t.counterparty,
    t.side,
    t.passive,
    t.price,
    t.quantity,
    m.best_bid,
    m.best_ask,
    (m.best_bid + m.best_ask) / 2 AS mid,
    (m.best_ask - m.best_bid) AS spread,
    CASE t.side
        WHEN 'buy'  THEN (t.price - mid)
                         / (mid) * 10000
        WHEN 'sell' THEN (mid - t.price)
                         / (mid) * 10000
    END AS slippage_bps,
    CASE t.side
        WHEN 'buy'  THEN (t.price - m.best_ask) / m.best_ask * 10000
        WHEN 'sell' THEN (m.best_bid - t.price) / m.best_bid * 10000
    END AS slippage_vs_tob_bps
FROM fx_trades t
ASOF JOIN market_data m ON (symbol)
WHERE t.timestamp IN '$yesterday'
ORDER BY t.timestamp;

/* Find the minimum ask and maximum bid in
the 10 seconds before and after each trade
*/
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
WHERE t.timestamp IN '$yesterday';

-- fixed horizons
SELECT
    t.symbol,
    t.counterparty,
    h.offset / 1000000000 AS horizon_sec,
    count() AS n,
    avg(((m.best_bid + m.best_ask) / 2 - t.price) / t.price * 10000) AS avg_markout_bps,
    sum(t.quantity) AS total_volume
FROM fx_trades t
HORIZON JOIN market_data m ON (symbol)
    LIST (-10s,0, 1s, 5s, 10s,
           1m, 5m) AS h
WHERE t.side = 'buy'
    AND t.timestamp IN '$yesterday'
GROUP BY t.symbol, t.counterparty, horizon_sec
ORDER BY t.symbol, t.counterparty, horizon_sec;




-- horizon at 30s for 10 minutes
SELECT
    t.symbol,
    h.offset / 1000000000 AS horizon_sec,   count() AS n,
    avg(((m.best_bid + m.best_ask) / 2 - t.price) / t.price * 10000) AS avg_markout_bps,
    sum(((m.best_bid + m.best_ask) / 2 - t.price) * t.quantity) AS total_pnl
FROM fx_trades t
HORIZON JOIN market_data m ON (symbol)
    RANGE FROM -1m TO 5m STEP 30s AS h
WHERE t.side = 'buy'
  AND t.timestamp IN '$yesterday'
GROUP BY t.symbol, horizon_sec
ORDER BY t.symbol, horizon_sec;

SELECT
    t.symbol,
    h.offset / 1000000000 AS horizon_sec,  count() AS n,
    avg(((m.best_bid + m.best_ask) / 2 - t.price) / t.price * 10000) AS avg_markout_bps,
    sum(((m.best_bid + m.best_ask) / 2 - t.price) * t.quantity) AS total_pnl
FROM fx_trades t
HORIZON JOIN market_data m ON (symbol)
    RANGE FROM -1m TO 5m STEP 30s AS h
WHERE t.side = 'buy'
  AND t.timestamp IN '2026-05-30'
GROUP BY t.symbol, horizon_sec
ORDER BY t.symbol, horizon_sec;



--------------------------------------------
-- forwards and swaps (run the generator with --forwards true, and --metals true for XAUUSD)
--------------------------------------------
-- Points are in pips. ON and TN hold the short swaps today->tomorrow and tomorrow->spot as
-- quoted, with value dates before spot (negative tenor_days), so their outrights are spot MINUS
-- the cumulative points, with bid and ask sides swapped. spot_ref is the spot each ladder was
-- priced on.

-- 1. Forward curve now: the whole curve in one query, outrights consistent with the spot the desk sees.
DECLARE
  @symbol := 'XAUUSD',
  @pip := CASE WHEN @symbol LIKE '%JPY' THEN 0.01 WHEN @symbol = 'XAGUSD' THEN 0.001
               WHEN @symbol LIKE 'X%' THEN 0.01 ELSE 0.0001 END,
  @dp := CASE WHEN @symbol LIKE '%JPY' OR @symbol LIKE 'X%' THEN 4 WHEN @symbol = 'XAGUSD' THEN 5 ELSE 6 END
WITH
  spot AS (
    SELECT bid_price, ask_price FROM core_price
    WHERE symbol = @symbol
    LATEST ON timestamp PARTITION BY symbol
  ),
  curve AS (
    SELECT tenor, tenor_days, value_date, bid_points, ask_points FROM fx_forward_points
    WHERE symbol = @symbol
    LATEST ON timestamp PARTITION BY tenor
  ),
  pre_spot AS (
    SELECT tenor, tenor_days, value_date, bid_points, ask_points,
      sum(CASE WHEN tenor_days < 0 THEN bid_points END)
        OVER (ORDER BY tenor_days DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_bid,
      sum(CASE WHEN tenor_days < 0 THEN ask_points END)
        OVER (ORDER BY tenor_days DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_ask
    FROM curve
  )
SELECT tenor, tenor_days, value_date, bid_points, ask_points,
  bid_price AS spot_bid, ask_price AS spot_ask,
  round(CASE WHEN tenor_days < 0 THEN bid_price - cum_ask * @pip ELSE bid_price + bid_points * @pip END, @dp) AS outright_bid,
  round(CASE WHEN tenor_days < 0 THEN ask_price - cum_bid * @pip ELSE ask_price + ask_points * @pip END, @dp) AS outright_ask
FROM pre_spot CROSS JOIN spot
ORDER BY tenor_days;

-- 2. Forward curve as of T: any instant, same query.
DECLARE
  @symbol := 'XAUUSD',
  @asof := dateadd('h', -1, now()),
  @pip := CASE WHEN @symbol LIKE '%JPY' THEN 0.01 WHEN @symbol = 'XAGUSD' THEN 0.001
               WHEN @symbol LIKE 'X%' THEN 0.01 ELSE 0.0001 END,
  @dp := CASE WHEN @symbol LIKE '%JPY' OR @symbol LIKE 'X%' THEN 4 WHEN @symbol = 'XAGUSD' THEN 5 ELSE 6 END
WITH
  spot AS (
    SELECT bid_price, ask_price FROM core_price
    WHERE symbol = @symbol AND timestamp <= @asof
    LATEST ON timestamp PARTITION BY symbol
  ),
  curve AS (
    SELECT tenor, tenor_days, value_date, bid_points, ask_points FROM fx_forward_points
    WHERE symbol = @symbol AND timestamp <= @asof
    LATEST ON timestamp PARTITION BY tenor
  ),
  pre_spot AS (
    SELECT tenor, tenor_days, value_date, bid_points, ask_points,
      sum(CASE WHEN tenor_days < 0 THEN bid_points END)
        OVER (ORDER BY tenor_days DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_bid,
      sum(CASE WHEN tenor_days < 0 THEN ask_points END)
        OVER (ORDER BY tenor_days DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_ask
    FROM curve
  )
SELECT tenor, tenor_days, value_date, bid_points, ask_points,
  bid_price AS spot_bid, ask_price AS spot_ask,
  round(CASE WHEN tenor_days < 0 THEN bid_price - cum_ask * @pip ELSE bid_price + bid_points * @pip END, @dp) AS outright_bid,
  round(CASE WHEN tenor_days < 0 THEN ask_price - cum_bid * @pip ELSE ask_price + ask_points * @pip END, @dp) AS outright_ask
FROM pre_spot CROSS JOIN spot
ORDER BY tenor_days;

-- 3. Implied carry and lease rate: what the forward says the metal costs to borrow.
-- Backs out the base-currency rate per tenor from the points and the quote-currency rate
-- (for a metal pair, the lease rate). Quote rates are the generator's static curves
-- (RATE_CURVES, set 2026-10-13); the live curves walk a few basis points around them.
DECLARE
  @symbol := 'XAUUSD',
  @base := left(@symbol, 3),
  @quote := right(@symbol, 3),
  @pip := CASE WHEN @symbol LIKE '%JPY' THEN 0.01 WHEN @symbol = 'XAGUSD' THEN 0.001
               WHEN @symbol LIKE 'X%' THEN 0.01 ELSE 0.0001 END,
  @base_basis := CASE WHEN @base = 'GBP' THEN 365.0 ELSE 360.0 END,
  @quote_basis := CASE WHEN @quote = 'GBP' THEN 365.0 ELSE 360.0 END
WITH
  quote_curve AS (
    SELECT tenor, CASE @quote WHEN 'USD' THEN usd WHEN 'EUR' THEN eur WHEN 'JPY' THEN jpy
                              WHEN 'CHF' THEN chf WHEN 'CAD' THEN cad END AS quote_rate
    FROM (
                SELECT 'ON' tenor, 3.88 usd, 2.42 eur, 1.23 jpy, -0.05 chf, 2.24 cad FROM long_sequence(1)
      UNION ALL SELECT 'TN', 3.88, 2.42, 1.23, -0.05, 2.24 FROM long_sequence(1)
      UNION ALL SELECT '1W', 3.88, 2.43, 1.23, -0.04, 2.24 FROM long_sequence(1)
      UNION ALL SELECT '2W', 3.89, 2.44, 1.24, -0.04, 2.23 FROM long_sequence(1)
      UNION ALL SELECT '1M', 3.90, 2.46, 1.25, -0.03, 2.22 FROM long_sequence(1)
      UNION ALL SELECT '2M', 3.92, 2.49, 1.26, -0.02, 2.21 FROM long_sequence(1)
      UNION ALL SELECT '3M', 3.93, 2.52, 1.27, -0.01, 2.20 FROM long_sequence(1)
      UNION ALL SELECT '6M', 3.95, 2.58, 1.30, 0.01, 2.18 FROM long_sequence(1)
      UNION ALL SELECT '9M', 3.95, 2.62, 1.33, 0.03, 2.16 FROM long_sequence(1)
      UNION ALL SELECT '1Y', 3.94, 2.65, 1.35, 0.05, 2.15 FROM long_sequence(1)
    )
  ),
  curve AS (
    SELECT tenor, tenor_days, (bid_points + ask_points) / 2 AS mid_points, spot_ref FROM fx_forward_points
    WHERE symbol = @symbol
    LATEST ON timestamp PARTITION BY tenor
  ),
  -- Days each quote covers: spot to value date, or the short swap's own period for ON and TN.
  accrual AS (
    SELECT tenor, tenor_days, mid_points, spot_ref,
      CASE WHEN tenor_days < 0
           THEN least(lead(tenor_days) OVER (ORDER BY tenor_days), 0) - tenor_days
           ELSE tenor_days END AS days
    FROM curve
  )
SELECT a.tenor, a.tenor_days, a.mid_points, q.quote_rate,
  round(((1 + q.quote_rate / 100 * a.days / @quote_basis)
          / (1 + a.mid_points * @pip / a.spot_ref) - 1) * @base_basis / a.days * 100, 3) AS implied_base_rate
FROM accrual a
JOIN quote_curve q ON q.tenor = a.tenor
ORDER BY a.tenor_days;

-- 4. Swap points: a swap is two tenors subtracted; nothing to store.
-- The desk buys near and sells far on the bid: bid swap = far bid - near ask, ask swap =
-- far ask - near bid. The implied swap rate is the annualised forward premium of the period.
-- For tenors from 1W out; ON and TN sit before spot.
DECLARE
  @symbol := 'XAUUSD',
  @near := '1M',
  @far := '3M',
  @pip := CASE WHEN @symbol LIKE '%JPY' THEN 0.01 WHEN @symbol = 'XAGUSD' THEN 0.001
               WHEN @symbol LIKE 'X%' THEN 0.01 ELSE 0.0001 END
WITH
  curve AS (
    SELECT tenor, tenor_days, bid_points, ask_points, spot_ref FROM fx_forward_points
    WHERE symbol = @symbol AND tenor IN (@near, @far)
    LATEST ON timestamp PARTITION BY tenor
  ),
  near_leg AS (SELECT * FROM curve WHERE tenor = @near),
  far_leg AS (SELECT * FROM curve WHERE tenor = @far)
SELECT n.tenor AS near_tenor, f.tenor AS far_tenor, f.tenor_days - n.tenor_days AS swap_days,
  round(f.bid_points - n.ask_points, 2) AS bid_swap,
  round(f.ask_points - n.bid_points, 2) AS ask_swap,
  round(((f.bid_points + f.ask_points) - (n.bid_points + n.ask_points)) / 2 * @pip / f.spot_ref
        * 360 / (f.tenor_days - n.tenor_days) * 100, 3) AS implied_swap_rate
FROM near_leg n CROSS JOIN far_leg f;

-- 5. Triangular consistency: a bad point in one feed is caught by the other two.
-- For every minute and tenor, the EURJPY points implied by EURUSD and USDJPY (and the XAUEUR
-- points implied by XAUUSD and EURUSD) against the published points, compared in forward
-- premium terms on each ladder's own spot_ref so spot moves do not leak into the check.
-- Returns one row per offending window. The generator plants one a day: EURJPY 3M, or XAUEUR
-- 3M with --metals, 6 pips off from 09:20 to 09:40 UTC (--bad_point_utc). Only finished minutes
-- are compared: within a 5-second cycle EURUSD publishes before the gold pairs, so in the minute
-- still being written live a cross and its legs can be a rate step apart.
DECLARE
  @range := '$today',
  @tolerance_pips := 1.0
WITH
  prem AS (
    SELECT timestamp, tenor,
      max(CASE WHEN symbol = 'EURUSD' THEN (bid_points + ask_points) / 2 * 0.0001 / spot_ref END) AS eurusd,
      max(CASE WHEN symbol = 'USDJPY' THEN (bid_points + ask_points) / 2 * 0.01 / spot_ref END) AS usdjpy,
      max(CASE WHEN symbol = 'XAUUSD' THEN (bid_points + ask_points) / 2 * 0.01 / spot_ref END) AS xauusd,
      max(CASE WHEN symbol = 'EURJPY' THEN (bid_points + ask_points) / 2 END) AS eurjpy_points,
      max(CASE WHEN symbol = 'EURJPY' THEN spot_ref END) AS eurjpy_spot,
      max(CASE WHEN symbol = 'XAUEUR' THEN (bid_points + ask_points) / 2 END) AS xaueur_points,
      max(CASE WHEN symbol = 'XAUEUR' THEN spot_ref END) AS xaueur_spot
    FROM fx_forward_points_1m
    WHERE timestamp IN @range AND timestamp < timestamp_floor('m', now())
      AND symbol IN ('EURUSD', 'USDJPY', 'EURJPY', 'XAUUSD', 'XAUEUR')
    GROUP BY timestamp, tenor
  ),
  gaps AS (
    SELECT timestamp, tenor, 'EURJPY' AS cross_pair, eurjpy_points AS published,
      eurjpy_spot * ((1 + eurusd) * (1 + usdjpy) - 1) / 0.01 AS implied
    FROM prem WHERE eurjpy_points IS NOT NULL
    UNION ALL
    SELECT timestamp, tenor, 'XAUEUR', xaueur_points,
      xaueur_spot * ((1 + xauusd) / (1 + eurusd) - 1) / 0.01
    FROM prem WHERE xaueur_points IS NOT NULL
  ),
  flagged AS (
    SELECT timestamp, timestamp_floor('d', timestamp) AS day, cross_pair, tenor, published - implied AS gap
    FROM gaps
    WHERE abs(published - implied) > @tolerance_pips
  )
SELECT day, cross_pair, tenor, min(timestamp) AS window_from, dateadd('m', 1, max(timestamp)) AS window_to,
  count() AS minutes, round(avg(gap), 2) AS avg_gap_pips, round(max(abs(gap)), 2) AS max_gap_pips
FROM flagged
GROUP BY day, cross_pair, tenor
ORDER BY day, cross_pair, tenor;

-- 6. Points history versus spot: one tenor over the day, the forward next to the spot it rides on.
DECLARE
  @symbol := 'XAUUSD',
  @tenor := '3M',
  @range := '$today'
WITH
  points AS (
    SELECT timestamp, (bid_points + ask_points) / 2 AS mid_points FROM fx_forward_points_1m
    WHERE symbol = @symbol AND tenor = @tenor AND timestamp IN @range
  ),
  spot AS (
    SELECT timestamp, last(close_mid) AS spot_mid FROM core_price_1s
    WHERE symbol = @symbol AND timestamp IN @range
    SAMPLE BY 1m
  )
SELECT p.timestamp, p.mid_points, s.spot_mid
FROM points p
ASOF JOIN spot s;



--------------------------------------------
-- demo end --
--------------------------------------------------

--- dedup
;

drop table trades_test;
CREATE TABLE 'trades_test' (
	timestamp TIMESTAMP_NS,
	symbol SYMBOL,
	price DOUBLE
) timestamp(timestamp) PARTITION BY HOUR
DEDUP UPSERT KEYS(timestamp,symbol);







insert into trades_test values ('2026-02-27T00:00:00', 'EURUSD', 1);
select * from trades_test;




insert into trades_test values ('2026-02-27T01:00:00', 'EURUSD', 2);
select * from trades_test;




insert into trades_test values ('2026-02-27T01:00:00', 'EURUSD', 3);
select * from trades_test;
insert into trades_test values ('2026-02-27T01:00:00', 'GBPUSD', 4);
select * from trades_test;
insert into trades_test values ('2026-02-27T01:00:00', 'GBPUSD', 5);

-- just another asof join

with p as (
        select * from core_price
        where symbol = 'GBPUSD'
        and timestamp in '2025-11-12' -- yesterday() --today()
        )
select bids[1][insertion_point(bids[2], bid_volume)],
        bids[2][insertion_point(bids[2], bid_volume)],
        insertion_point(bids[2], bid_volume), *
from p asof join market_data on symbol TOLERANCE 1s;





select * from _query_trace where principal <> 'admin';





select ts, principal, count(), sum(execution_micros) from _query_trace
sample by 1h;

--------------------------------------------
-- demo end --
--------------------------------------------------

-- select touch(select timestamp, bids from market_data where timestamp in yesterday());


--wal_tables();
--materialized_views();

--table_partitions('market_data') where name like '2025-07-09%';
--select view_name, base_table_name, view_status, last_refresh_start_timestamp,last_refresh_finish_timestamp,refresh_base_table_txn, base_table_txn from materialized_views() order by view_status;

--select * from (table_storage()) order by tableName;

--(show parameters) where value_source <> 'default';

--show partitions from market_data;
--table_partitions('market_data');

--wal_tables() where name ilike '%core%' or name ilike '%bbo%' or name ilike '%market%';
