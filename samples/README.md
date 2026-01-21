# Sample Development Setup

This guide walks you through setting up a minimal development environment to test Shirath with sample data.

## Prerequisites

- PostgreSQL 10+ running locally
- ClickHouse running locally (or via Docker)
- Elixir 1.14+

## 1. Create PostgreSQL Database

```bash
# Connect to PostgreSQL and create the eshop database
psql -U postgres -c "CREATE DATABASE eshop;"
```

## 2. Configure PostgreSQL for Logical Replication

```sql
-- Run these commands as superuser
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;
```

Restart PostgreSQL after making these changes.

## 3. Create Sample Table and Data

```bash
# Execute the sample SQL file
psql -U postgres -d eshop -f samples/product_data.sql
```

Or run manually:

```sql
-- Connect to eshop database
\c eshop

-- Create products table (see samples/product_data.sql for full script)
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    category VARCHAR(100),
    stock_quantity INTEGER DEFAULT 0,
    sku VARCHAR(50) UNIQUE,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert 50 sample products (see samples/product_data.sql)
```

## 4. Create Heartbeat Table

```sql
-- Required for preventing stale replication slots
CREATE TABLE IF NOT EXISTS _heartbeat (
    id INTEGER PRIMARY KEY,
    heartbeat INTEGER NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO _heartbeat (id, heartbeat) VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;
```

## 5. Create Publication

```sql
-- Create publication for CDC
CREATE PUBLICATION eshop_pub FOR ALL TABLES;
```

## 6. Create ClickHouse Schema

Start ClickHouse (if using Docker):

```bash
docker run -d \
  --name clickhouse \
  -p 8123:8123 \
  -p 9000:9000 \
  -e CLICKHOUSE_DB=analytics \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=password \
  clickhouse/clickhouse-server
```

Create the database and table:

```bash
# Using docker exec
docker exec -it clickhouse clickhouse-client --query "
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
"
```

Or save to file and execute:

```bash
# Execute samples/clickhouse_schema.sql
docker exec -i clickhouse clickhouse-client < samples/clickhouse_schema.sql
```

## 7. Configure Mapper

Copy the sample mapper configuration:

```bash
cp samples/mapper.sample.json mapper.json
```

The mapper defines which PostgreSQL tables to replicate to ClickHouse:

```json
{
    "data": [
        {
            "source_table": "products",
            "dest_table": "products"
        }
    ]
}
```

## 8. Configure Shirath

Update `config/dev.exs` with your settings:

```elixir
config :shirath, ClickhouseMaster,
  connection_strings: "http://default:password@localhost:8123/analytics"

config :shirath, ClickhouseSlave,
  connection_strings: "http://default:password@localhost:8123/analytics"

config :shirath, Shirath.Repo,
  username: "postgres",
  password: "postgres",
  database: "eshop",
  hostname: "localhost",
  pool_size: 10

config :shirath, Shirath.ObanRepo,
  username: "postgres",
  password: "postgres",
  database: "shirath",
  hostname: "localhost",
  pool_size: 10

config :shirath, :cdc,
  slot: "eshop_slot",
  publications: ["eshop_pub"]
```

## 9. Create Shirath Database and Run Migrations

```bash
# Create shirath database for Oban queue
psql -U postgres -c "CREATE DATABASE shirath;"

# Run migrations
mix ecto.migrate -r Shirath.ObanRepo
```

## 10. Start Shirath

```bash
iex -S mix
```

## 11. Backfill Existing Data

Once Shirath is running, backfill the existing products:

```bash
# Using mix task
mix shirath.backfill products

# Or in IEx
Shirath.Backfill.start(["products"])
```

## 12. Verify Data in ClickHouse

```bash
docker exec -it clickhouse clickhouse-client --query "
SELECT count(*) FROM analytics.products;
"
```

## Testing CDC

Insert a new product in PostgreSQL:

```sql
INSERT INTO products (name, description, price, category, stock_quantity, sku)
VALUES ('Test Product', 'Testing CDC', 9.99, 'Test', 10, 'TEST-001');
```

Check if it appears in ClickHouse:

```bash
docker exec -it clickhouse clickhouse-client --query "
SELECT * FROM analytics.products WHERE sku = 'TEST-001';
"
```

## File Reference

| File | Description |
|------|-------------|
| `samples/product_data.sql` | PostgreSQL table schema and 50 sample products |
| `samples/clickhouse_schema.sql` | ClickHouse table schema |
| `samples/mapper.sample.json` | Sample mapper configuration |
