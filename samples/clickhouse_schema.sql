-- ClickHouse schema for sample products table
-- Run with: docker exec -i clickhouse clickhouse-client < samples/clickhouse_schema.sql

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.products (
    id UInt64,
    name String,
    description Nullable(String),
    price Decimal(10, 2),
    category Nullable(String),
    stock_quantity Int32 DEFAULT 0,
    sku Nullable(String),
    is_active UInt8 DEFAULT 1,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    ver UInt64 DEFAULT 0
) ENGINE = ReplacingMergeTree(ver)
ORDER BY id;
