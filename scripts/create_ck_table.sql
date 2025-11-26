CREATE DATABASE IF NOT EXISTS streamforge;

-- 使用 ReplacingMergeTree 实现幂等性
-- 基于 (window_start, store_id) 作为唯一键，相同窗口和店铺的数据会被自动去重
-- 配合 Flink Checkpoint，实现端到端精确一次语义
CREATE TABLE IF NOT EXISTS streamforge.orders_metrics
(
    window_start DateTime,
    window_end   DateTime,
    store_id     String,
    order_cnt    UInt32,
    gmv_total    Float64,
    user_cnt     UInt32,
    update_time  DateTime DEFAULT now()  -- 用于 ReplacingMergeTree 的去重依据
)
ENGINE = ReplacingMergeTree(update_time)  -- 使用 ReplacingMergeTree，按 update_time 保留最新数据
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, store_id)  -- 唯一键：窗口开始时间 + 店铺ID
SETTINGS index_granularity = 8192;

