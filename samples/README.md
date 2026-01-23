# Sample Development Setup

This folder contains everything you need to test Shirath locally with a complete CDC pipeline.

## Quick Start (Docker)

Run the entire stack with one command:

```bash
cd samples
docker compose -f docker-compose.dev.yml up
```

This starts:
- **PostgreSQL** - Source database with logical replication enabled and 50 sample products
- **ClickHouse** - Destination analytics database
- **Shirath** - CDC bridge streaming changes in real-time

No ports are exposed to the host to avoid conflicts with your local databases. Use `docker exec` to interact with the containers.

Wait for all services to be healthy (you'll see "Starting Shirath..." in the logs).

## Test the CDC Pipeline

### 1. Insert a new product in PostgreSQL

```bash
docker exec -it shirath-postgres psql -U postgres -d eshop -c \
  "INSERT INTO products (name, price, category, sku) VALUES ('Test Product', 9.99, 'Test', 'TEST-001');"
```

### 2. Verify it appears in ClickHouse

```bash
docker exec -it shirath-clickhouse clickhouse-client --query \
  "SELECT id, name, price, category, sku FROM analytics.products WHERE sku = 'TEST-001';"
```

### 3. Try an update

```bash
docker exec -it shirath-postgres psql -U postgres -d eshop -c \
  "UPDATE products SET price = 19.99 WHERE sku = 'TEST-001';"
```

Check ClickHouse again - the price should be updated.

## Backfill Existing Data

The 50 sample products were inserted before Shirath started, so they won't appear in ClickHouse automatically. To backfill them:

```bash
# Using the API (via docker exec)
docker exec -it shirath curl -s -X POST http://localhost:4000/api/backfill \
  -H "Content-Type: application/json" \
  -d '{"tables": ["products"]}'

# Check backfill progress
docker exec -it shirath curl -s http://localhost:4000/api/backfill
```

Then verify all products are in ClickHouse:

```bash
docker exec -it shirath-clickhouse clickhouse-client --query \
  "SELECT count(*) FROM analytics.products;"
```

## Useful Commands

### View PostgreSQL data
```bash
docker exec -it shirath-postgres psql -U postgres -d eshop -c "SELECT * FROM products LIMIT 5;"
```

### View ClickHouse data
```bash
docker exec -it shirath-clickhouse clickhouse-client --query "SELECT * FROM analytics.products LIMIT 5;"
```

### View Shirath logs
```bash
docker logs -f shirath
```

### Connect to PostgreSQL
```bash
docker exec -it shirath-postgres psql -U postgres -d eshop
```

### Connect to ClickHouse
```bash
docker exec -it shirath-clickhouse clickhouse-client
```

### Stop everything
```bash
docker compose -f docker-compose.dev.yml down
```

### Stop and remove all data
```bash
docker compose -f docker-compose.dev.yml down -v
```

---

## Manual Setup (Without Docker)

If you prefer to run Shirath directly on your machine, follow the steps below.

### Prerequisites

- PostgreSQL 10+ running locally
- ClickHouse running locally (or via Docker)
- Elixir 1.14+

### 1. Create PostgreSQL Database

```bash
psql -U postgres -c "CREATE DATABASE eshop;"
```

### 2. Configure PostgreSQL for Logical Replication

```sql
-- Run these commands as superuser
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;
```

Restart PostgreSQL after making these changes.

### 3. Create Sample Table and Data

```bash
psql -U postgres -d eshop -f samples/product_data.sql
```

### 4. Create Heartbeat Table

```sql
CREATE TABLE IF NOT EXISTS _heartbeat (
    id INTEGER PRIMARY KEY,
    heartbeat INTEGER NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO _heartbeat (id, heartbeat) VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;
```

### 5. Create Publication

```sql
CREATE PUBLICATION eshop_pub FOR ALL TABLES;
```

### 6. Create ClickHouse Schema

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

Create the schema:

```bash
docker exec -i clickhouse clickhouse-client < samples/clickhouse_schema.sql
```

### 7. Configure Mapper

```bash
cp samples/mapper.sample.json mapper.json
```

### 8. Configure Shirath

Update your environment or `config/dev.exs`:

```elixir
config :shirath, ClickhouseMaster,
  connection_strings: "http://default:password@localhost:8123/analytics"

config :shirath, Shirath.Repo,
  username: "postgres",
  password: "postgres",
  database: "eshop",
  hostname: "localhost"

config :shirath, :cdc,
  slot: "eshop_slot",
  publications: ["eshop_pub"]
```

### 9. Create Shirath Database and Run Migrations

```bash
psql -U postgres -c "CREATE DATABASE shirath;"
mix ecto.migrate -r Shirath.ObanRepo
```

### 10. Start Shirath

```bash
iex -S mix
```

---

## File Reference

| File | Description |
|------|-------------|
| `docker-compose.dev.yml` | Complete development stack (PostgreSQL + ClickHouse + Shirath) |
| `init-postgres.sql` | PostgreSQL init script (schema + data + publication) |
| `docker-entrypoint.sh` | Shirath container entrypoint (waits for deps, runs migrations) |
| `product_data.sql` | PostgreSQL table schema and 50 sample products |
| `clickhouse_schema.sql` | ClickHouse table schema |
| `mapper.sample.json` | Sample mapper configuration |
