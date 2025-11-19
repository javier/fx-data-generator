show tables;

select * from market_data;

select *
from market_data  where timestamp in '2025-11-12'; --today() order by timestamp desc;

select *
from market_data  where timestamp in yesterday();


select * from core_price where timestamp in today();


select * from market_data
where symbol in 'GBPUSD' and timestamp in  '2025-11-12'; --today()
limit -10;




select timestamp, symbol,
    bids[1,1] as bprice, bids[2,1] as bvolume,
    asks[1,1] as aprice, asks[2,1] as avolume,
    bids[1,2999] as bprice2, bids[2,2999] as bvolume2,
    asks[1,2999] as aprice2, asks[2,2999] as avolume2,
    array_sum(bids[2]) as total_volume
from market_data where timestamp in today() limit -20;




select * from core_price latest by symbol;

select * from core_price latest by symbol, ecn;

select * from core_price latest by symbol, ecn
where timestamp <= '2025-11-13T00:00';


select timestamp, symbol, count(bids[1][1]) from market_data
        where timestamp in '2025-11-13T00'
        and   symbol = 'GBPUSD'
        sample by 10s;

table_partitions('market_data');
table_partitions('core_price');


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
 select timestamp,
            first(bids[1][1]) as open,
            max(bids[1][1]) as high,
            min(bids[1][1]) as low,
            last(bids[1][1]) as close,
            avg(bids[1][1]) as avgr,
            sum(bids[2][1]) as volume
        from market_data
        where timestamp in '2025-11-12' -- yesterday()
        and   symbol = 'GBPUSD'
        sample by 15m
        order by timestamp desc;

CREATE MATERIALIZED VIEW 'market_data_ohlc_1m'
    WITH BASE 'market_data' REFRESH IMMEDIATE AS (

            SELECT timestamp, symbol,
                first(bids[1][1]) AS open,
                max(bids[1][1]) AS high,
                min(bids[1][1]) AS low,
                last(bids[1][1]) AS close,
                SUM(bids[2][1]) AS total_volume
            FROM market_data
            SAMPLE BY 1m

) PARTITION BY HOUR TTL 2 DAYS
OWNED BY 'admin';






select * from market_data_ohlc_1m where
symbol = 'GBPUSD' AND timestamp in '2025-11-12'; --yesterday(); --yesterday();







select * from market_data_ohlc_1m
where timestamp in today()
order by timestamp desc, symbol asc ;




CREATE MATERIALIZED VIEW 'bbo_1s'
    WITH BASE 'market_data' REFRESH IMMEDIATE AS (
            SELECT timestamp, symbol,
                last(bids[1][1]) AS bid,
                last(asks[1][1]) AS ask
            FROM market_data
            SAMPLE BY 1s
) PARTITION BY HOUR TTL 3 DAYS
OWNED BY 'admin';

CREATE MATERIALIZED VIEW 'bbo_1m'
WITH BASE 'bbo_1s' REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM bbo_1s
            SAMPLE BY 1m
) PARTITION BY DAY TTL 7 DAYS
OWNED BY 'admin';


CREATE MATERIALIZED VIEW 'bbo_1h'
WITH BASE 'bbo_1m' REFRESH EVERY 10m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
            SELECT timestamp, symbol,
                max(bid) AS bid,
                min(ask) AS ask
            FROM  bbo_1m
            SAMPLE BY 1h
) PARTITION BY MONTH TTL 1 MONTH
OWNED BY 'admin';



select timestamp, symbol,
    bids[1] as bprices, bids[2] as bsizes,
    asks[1] as aprices, asks[2] as asizes
from market_data latest by symbol;


-- spread
SELECT timestamp, symbol,
    asks[1][1] - bids[1][1]
FROM market_data
where symbol IN ('GBPUSD', 'EURUSD')
and timestamp in yesterday(); --today();


-- basic anomaly detection. Ask further from average than a given multiple of stddev
DECLARE
    @l1_ask := asks[1,1],
    @low_threshold := 1.9,
    @medium_threshold := 2.0,
    @high_threshold := 2.1
WITH s AS (
    SELECT avg(@l1_ask) as avg_ask, stddev(@l1_ask ) as stddev_ask
    FROM market_data
    where symbol IN ('GBPUSD')
    and timestamp >= dateadd('h', -1, now())
), combined AS (
SELECT timestamp, @l1_ask as l1_ask, avg_ask, stddev_ask, abs(l1_ask - avg_ask) as delta
FROM market_data m CROSS JOIN s
where m.symbol IN ('GBPUSD')
and m.timestamp >= dateadd('h', -1, now())
)
SELECT timestamp, l1_ask, avg_ask, delta,
    CASE
        WHEN delta >= stddev_ask * @high_threshold THEN 'High'
        WHEN delta >= stddev_ask * @medium_threshold THEN 'Medium'
        ELSE 'Low'
    END AS anomaly
 from combined where delta >= stddev_ask * @low_threshold;


select timestamp, symbol, bids[1,1] as l1_bid_price,
avg(bids[1,1]) over (partition by symbol order by timestamp) as moving_l1_bid_price,
bids[2,1] as l1_bid_volume, sum(bids[2,1]) over (partition by symbol order by timestamp) as moving_l1_bid_volume,
 sum(bids[2,1]) over (order by timestamp) as moving_l1_total_bid_volume
from market_data
where timestamp in yesterday(); --today()
--and symbol='GBPUSD'
;

-- select touch(select timestamp, bids from market_data where timestamp in yesterday());

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
    FROM market_data where symbol = 'GBPUSD' limit -200 ;

-- Equivalent query without declare. Volume is available within 1% of the best price?
SELECT asks,
     asks[2, 1:insertion_point(asks[1], 1.01 * asks[1, 1])] volume_levels,
     array_sum(asks[2, 1:insertion_point(asks[1], 1.01 * asks[1, 1])]) total_volume
    FROM market_data where symbol = 'GBPUSD' limit -100 ;

-- What price level will a buy order for the given volume reach?
WITH
    q1 AS (
    SELECT timestamp, symbol, asks,
        array_cum_sum(asks[2]) cum_volumes
    FROM market_data
    where symbol = 'GBPUSD' and timestamp in today()),
    q2 AS (
    SELECT timestamp, symbol,
        asks, cum_volumes,
        insertion_point(cum_volumes, 1_500_000, true) target_level
        FROM q1 )
SELECT timestamp, symbol,
    cum_volumes, target_level, asks[1, target_level] price
FROM q2;


select * from core_price asof join market_data on symbol
where core_price.symbol = 'GBPUSD'  and core_price.timestamp in '2025-11-12'; --yesterday(); --today();


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

wal_tables();
materialized_views();

table_partitions('market_data') where name like '2025-07-09%';
select view_name, base_table_name, view_status, last_refresh_start_timestamp,last_refresh_finish_timestamp,refresh_base_table_txn, base_table_txn from materialized_views() order by view_status;

select * from (table_storage()) order by tableName;

(show parameters) where value_source <> 'default';

show partitions from market_data;
table_partitions('market_data');

wal_tables() where name ilike '%core%' or name ilike '%bbo%' or name ilike '%market%';


---- XXX

--alter table market_data convert partition to parquet where timestamp < '2025-08-08';



-- refresh materialized view bbo_1s full;
-- refresh materialized view bbo_1m full;
-- refresh materialized view bbo_1h full;
-- refresh materialized view bbo_1d full;
-- refresh materialized view core_price_1s full;
-- refresh materialized view core_price_1d full;
-- refresh materialized view market_data_ohlc_1m full;
-- refresh materialized view market_data_ohlc_15m full;
-- refresh materialized view market_data_ohlc_1d full;


--drop table market_data;
--drop table core_price;
--drop materialized view bbo_1s;
--drop materialized view bbo_1m;
--drop materialized view bbo_1h;
--drop materialized view bbo_1d;
--drop materialized view core_price_1s;
--drop materialized view core_price_1d;
--drop materialized view market_data_ohlc_1m;
--drop materialized view market_data_ohlc_15m;
--drop materialized view market_data_ohlc_1d;

--alter table market_data squash partitions;
--alter table core_price squash partitions;

--alter table market_data drop partition where timestamp >= '2025-07-11';
--alter table core_price drop partition where timestamp >= '2025-07-11';

--alter table market_data set type bypass wal;
--alter table market_data set type  wal;
--alter table core_price set type bypass wal;
--alter table core_price set type  wal;



--rename table core_price to ORIG_core_price;
--rename table market_data to ORIG_market_data;
--create TABLE market_data(like ORIG_market_data);
--create TABLE core_price(like ORIG_core_price);

--INSERT BATCH 100_000_000 INTO core_price
--SELECT dateadd('d',7,timestamp) as timestamp, symbol, ecn, bid_price, bid_volume, ask_price, ask_volume, reason, indicator1, indicator2 from ORIG_core_price;


--INSERT BATCH 100_000_000 INTO market_data
--SELECT dateadd('d',7,timestamp) as timestamp, symbol, bids, asks from ORIG_market_data;


--rename table ORIG_core_price to core_price;
--rename table ORIG_market_data to market_data;

-- SELECT touch(SELECT * from market_data where timestamp in '2025-07-04');

-- SELECT *, transpose(bids) from market_data where timestamp in '2025-07-04';



/**
CREATE MATERIALIZED VIEW IF NOT EXISTS core_price_1s AS (
        SELECT
            timestamp,
            symbol,
            first((bid_price + ask_price) / 2) AS open_mid,
            max((bid_price + ask_price) / 2) AS high_mid,
            min((bid_price + ask_price) / 2) AS low_mid,
            last((bid_price + ask_price) / 2) AS close_mid,
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

CREATE MATERIALIZED VIEW IF NOT EXISTS bbo_1s AS (
        select timestamp, symbol,
            last(bids[1][1]) as bid,
            last(asks[1][1]) as ask
        from market_data
        sample by 1s
    ) PARTITION BY HOUR;

 CREATE MATERIALIZED VIEW IF NOT EXISTS market_data_ohlc_15m AS (
        select timestamp, symbol,
            first((bids[1][1]+asks[1][1])/2) as open,
            max((bids[1][1]+asks[1][1])/2) as high,
            min((bids[1][1]+asks[1][1])/2) as low,
            last((bids[1][1]+asks[1][1])/2) as close
        from market_data
        sample by 15m

    ) PARTITION BY HOUR TTL 2 DAYS;

select timestamp, symbol,
            first((bids[1][1]+asks[1][1])/2) as open,
            max((bids[1][1]+asks[1][1])/2) as high,
            min((bids[1][1]+asks[1][1])/2) as low,
            last((bids[1][1]+asks[1][1])/2) as close
        from market_data
        where timestamp in '2025-07-03'
        sample by 15m;

explain select timestamp,
            first(bids[1][1]) as open,
            max(bids[1][1]) as high,
            min(bids[1][1]) as low,
            last(bids[1][1]) as close
        from market_data
        where timestamp in '2025-07-02' and symbol = 'GBPJPY'
        sample by 15m;





     CREATE MATERIALIZED VIEW IF NOT EXISTS market_data_ohlc_1d REFRESH START '2025-06-01T00:00:00.000000Z' EVERY 1h AS (
        select timestamp, symbol,
            first(open) as open,
            max(high) as high,
            min(low) as low,
            last(close) as close
        from market_data_ohlc_15m
        sample by 1d
    ) PARTITION BY DAY;

select * from core_price;


CREATE MATERIALIZED VIEW 'market_data_ohlc_1d' WITH BASE 'market_data_ohlc_15m' REFRESH INCREMENTAL AS (

            SELECT timestamp, symbol,
                first(open) AS open,
                max(high) AS high,
                min(low) AS low,
                last(close) AS close
            FROM market_data_ohlc_15m
            SAMPLE BY 1d

) PARTITION BY MONTH
OWNED BY 'admin';

**/
