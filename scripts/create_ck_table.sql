CREATE DATABASE IF NOT EXISTS streamforge;

CREATE TABLE IF NOT EXISTS streamforge.orders_metrics
(
    window_start DateTime,
    window_end   DateTime,
    store_id     String,
    order_cnt    UInt32,
    gmv_total    Float64,
    user_cnt     UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, store_id)
SETTINGS index_granularity = 8192;

