-- CCXT2ClickHouse database schema

CREATE TABLE IF NOT EXISTS quotes
(
    source LowCardinality(String) COMMENT 'Exchange name (binance, bybit, etc.)',
    symbol LowCardinality(String) COMMENT 'Trading pair symbol (BTC/USDT, ETH/USDT, etc.)',
    timeframe LowCardinality(String) COMMENT 'Timeframe: 1s, 1m, 5m, 1h, 1d, 1w, etc.',
    time DateTime64(3, 'UTC') COMMENT 'Timestamp in milliseconds precision',
    open Float64 COMMENT 'Opening price',
    high Float64 COMMENT 'Highest price',
    low Float64 COMMENT 'Lowest price',
    close Float64 COMMENT 'Closing price',
    volume Float64 COMMENT 'Trading volume'
)
ENGINE = MergeTree()
PARTITION BY (source, toYYYYMM(time))
ORDER BY (source, symbol, timeframe, time)
SETTINGS index_granularity = 8192;

CREATE VIEW IF NOT EXISTS quotes_view AS
SELECT
    source,
    symbol,
    timeframe,
    time,
    open,
    high,
    low,
    close,
    volume
FROM quotes;

CREATE TABLE IF NOT EXISTS db_quotes_version
(
    version UInt32 COMMENT 'Database schema version'
)
ENGINE = ReplacingMergeTree()
ORDER BY version
SETTINGS index_granularity = 8192;

INSERT INTO db_quotes_version (version) VALUES (1);