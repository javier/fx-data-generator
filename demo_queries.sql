tables();

--show tables;
-- dataset intro
select * from market_data order by timestamp desc;

select * from market_data  where timestamp in '$yesterday';
select * from core_price where timestamp  in '$yesterday';
select * from fx_trades where timestamp in '$yesterday';


select * from market_data
where symbol in 'GBPUSD' and timestamp in  '$today'
limit -10;

select * from market_data
where symbol in 'GBPUSD'
and  timestamp IN '2026-02-24#XNYS';

select first(timestamp), last(timestamp) from fx_trades
WHERE timestamp IN '2026-02-19#XNYS';


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
where timestamp < '2026-02-11';


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
        where timestamp  in '$today'
        and   symbol = 'USDJPY'
        sample by 15m;

-- mat views

CREATE MATERIALIZED VIEW IF NOT EXISTS 'market_data_ohlc_1m'
    WITH BASE 'market_data' REFRESH IMMEDIATE AS (

            SELECT timestamp, symbol,
                first(best_bid) AS open,
                max(best_bid) AS high,
                min(best_bid) AS low,
                last(best_bid) AS close,
                SUM(bids[2][1]) AS total_volume
            FROM market_data
            SAMPLE BY 1m

) PARTITION BY HOUR TTL 2 DAYS
OWNED BY 'admin';



 select * from market_data_ohlc_1m where
symbol = 'GBPUSD' AND timestamp in '$today';


select * from market_data_ohlc_1m
where timestamp in '$today'
order by timestamp desc, symbol asc ;



-- TTL and cascading
CREATE MATERIALIZED VIEW IF NOT EXISTS 'bbo_1s'
    WITH BASE 'market_data' REFRESH IMMEDIATE AS (
            SELECT timestamp, symbol,
                last(best_bid) AS bid,
                last(best_ask) AS ask
            FROM market_data
            SAMPLE BY 1s
) PARTITION BY HOUR TTL 3 DAYS
OWNED BY 'admin';

CREATE MATERIALIZED VIEW IF NOT EXISTS 'bbo_1m'
WITH BASE 'bbo_1s' REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM bbo_1s
            SAMPLE BY 1m
) PARTITION BY DAY TTL 7 DAYS
OWNED BY 'admin';


CREATE MATERIALIZED VIEW IF NOT EXISTS 'bbo_1h'
WITH BASE 'bbo_1m' REFRESH EVERY 10m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM  bbo_1m
            SAMPLE BY 1h
) PARTITION BY MONTH TTL 1 MONTH
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


select * from core_price asof join market_data on symbol
where core_price.symbol = 'GBPUSD'  and core_price.timestamp in '$today';

select * from fx_trades asof join core_price on symbol asof join market_data on symbol
where fx_trades.symbol = 'GBPUSD'  and fx_trades.timestamp in '$today';

-- Use ASOF JOIN to pair each trade with the most recent order book snapshot, then calculate slippage in basis points
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
        WHEN 'buy'  THEN (t.price - (m.best_bid + m.best_ask) / 2)
                         / ((m.best_bid + m.best_ask) / 2) * 10000
        WHEN 'sell' THEN ((m.best_bid + m.best_ask) / 2 - t.price)
                         / ((m.best_bid + m.best_ask) / 2) * 10000
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
WHERE t.timestamp IN '$today';

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
    LIST (0, 1s, 5s, 10s,
          30s, 1m, 5m) AS h
WHERE t.side = 'buy'
    AND t.timestamp IN '$yesterday'
GROUP BY t.symbol, t.counterparty, horizon_sec
ORDER BY t.symbol, t.counterparty, horizon_sec;



-- horizon at 30s for 10 minutes
SELECT
    t.symbol,
    h.offset / 1000000000 AS horizon_sec,
    count() AS n,
    avg(((m.best_bid + m.best_ask) / 2 - t.price) / t.price * 10000) AS avg_markout_bps,
    sum(((m.best_bid + m.best_ask) / 2 - t.price) * t.quantity) AS total_pnl
FROM fx_trades t
HORIZON JOIN market_data m ON (symbol)
    RANGE FROM 0s TO 10m STEP 30s AS h
WHERE t.side = 'buy'
  AND t.timestamp IN '$yesterday'
GROUP BY t.symbol, horizon_sec
ORDER BY t.symbol, horizon_sec;


--------------------------------------------
-- demo end --
--------------------------------------------------


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

select * from _query_trace;

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
