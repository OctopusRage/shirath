# Shirath

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Elixir](https://img.shields.io/badge/Elixir-1.14+-blueviolet)](https://elixir-lang.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-10+-blue)](https://www.postgresql.org/)

Shirath is a Change Data Capture (CDC) bridge that streams changes from PostgreSQL to ClickHouse in real-time.

## How It Works

```
┌────────────┐    WAL Stream    ┌──────────┐    Batch Insert    ┌────────────┐
│ PostgreSQL │ ───────────────► │  Shirath │ ─────────────────► │ ClickHouse │
│  (Source)  │   pg_output      │          │                    │   (Sink)   │
└────────────┘                  └──────────┘                    └────────────┘
```

Shirath uses PostgreSQL's logical replication with the `pg_output` plugin to capture INSERT, UPDATE, and DELETE operations. Changes are batched using [Broadway](https://github.com/dashbitco/broadway) for efficient processing and stored in an [Oban](https://github.com/oban-bg/oban) queue for reliable delivery to ClickHouse.

## Why Shirath?

**Lightweight & Resource Efficient** - Shirath runs comfortably on minimal infrastructure. A small EC2 instance with 2 vCPUs and 2GB RAM is sufficient for most production workloads.

**Built on the BEAM** - Powered by Elixir and the Erlang VM, Shirath inherits battle-tested concurrency primitives. Lightweight processes handle thousands of concurrent operations without the memory overhead of OS threads.

**Smart Batching** - Instead of inserting rows one-by-one, Shirath uses [Broadway](https://github.com/dashbitco/broadway) to batch changes into files, then processes them in optimized bulk inserts to ClickHouse. This dramatically reduces network overhead and leverages ClickHouse's strength in batch operations.

**No Kafka Required** - Unlike many CDC solutions that require Kafka or other message brokers, Shirath connects directly to PostgreSQL's WAL stream. Fewer moving parts means less infrastructure to manage and fewer points of failure.

**Fault Tolerant** - Changes are persisted to an [Oban](https://github.com/oban-bg/oban) queue backed by PostgreSQL. If ClickHouse is temporarily unavailable, no data is lost. Processing resumes automatically when connectivity is restored.

## Features

- Real-time CDC from PostgreSQL to ClickHouse
- Configurable table mapping with field transformation
- Fault-tolerant with persistent queue ([Oban](https://github.com/oban-bg/oban))
- Batched inserts for optimal ClickHouse performance
- Support for multiple source tables to single destination
- Minimal resource footprint (2 vCPU / 2GB RAM)

## Requirements

- PostgreSQL 10+ (with logical decoding support)
- ClickHouse
- Elixir 1.14+

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/shirath.git
   cd shirath
   ```

2. Install dependencies:
   ```bash
   mix deps.get
   ```

3. Configure environment (see [Configuration](#configuration))

4. Run migrations:
   ```bash
   mix ecto.migrate -r Shirath.ObanRepo
   ```

5. Start the application:
   ```bash
   iex -S mix
   ```

## Configuration

### PostgreSQL Setup

Enable logical replication on your PostgreSQL server:

```sql
-- Enable logical decoding
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;
```

Restart PostgreSQL, then create a publication:

```sql
-- For all tables
CREATE PUBLICATION my_publication FOR ALL TABLES;

-- Or for specific tables
CREATE PUBLICATION my_publication FOR TABLE users, orders, products;
```

### Heartbeat Table (Required)

Shirath uses a heartbeat table to prevent replication slots from becoming stale during periods of inactivity. Create this table in your source database:

```sql
-- Heartbeat table to prevent stale replication slots
CREATE TABLE IF NOT EXISTS _heartbeat (
    id INTEGER PRIMARY KEY,
    heartbeat INTEGER NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial row
INSERT INTO _heartbeat (id, heartbeat) VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;
```

Shirath automatically updates this table every minute to ensure WAL keeps flowing, preventing the replication slot from holding onto old WAL segments.

### Environment Variables

Copy the example environment file and configure:

```bash
cp .env.example .env
```

| Variable | Required | Description |
|----------|----------|-------------|
| `CH_CONNECTION` | Yes | ClickHouse connection string (e.g., `http://user:pass@host:8123/db`) |
| `CH_CONNECTION_SLAVE` | No | ClickHouse replica connection (defaults to `CH_CONNECTION`) |
| `DB_USERNAME` | Yes | PostgreSQL username |
| `DB_PASSWORD` | Yes | PostgreSQL password |
| `DB_NAME` | Yes | PostgreSQL database name |
| `DB_HOST` | Yes | PostgreSQL host |
| `CDC_PUBLICATION` | Yes | PostgreSQL publication name |
| `CDC_SLOT` | Yes | Replication slot name (will be created if not exists) |
| `MAPPER_FILE` | No | Path to mapper config (defaults to `mapper.json`) |
| `SENTRY_DSN` | No | Sentry DSN for error tracking |

### Mapper Configuration

The `mapper.json` file defines which tables to capture and how to transform the data. See `mapper.example.json` for a starting point.

```bash
cp mapper.example.json mapper.json
```

#### Basic Mapping

Direct 1:1 table replication:

```json
{
    "data": [
        {
            "source_table": "users",
            "dest_table": "users"
        }
    ]
}
```

#### Field Remapping

Transform fields during replication:

```json
{
    "data": [
        {
            "source_table": "products",
            "dest_table": "product_analytics",
            "remap": {
                "id": "{id}",
                "product_name": "{name}",
                "price": "{price}",
                "imported_at": "{created_at}"
            }
        }
    ]
}
```

- `{field_name}` - References a column from the source table
- Static values (without braces) are inserted as constants

#### Custom Processing (require code modification)

For advanced transformations, you can specify a custom module:

```json
{
    "data": [
        {
            "source_table": "events",
            "script": {
                "module": "MyApp.EventProcessor",
                "function": "process"
            }
        }
    ]
}
```

## Backfill Existing Data

When you start Shirath, it begins capturing changes from the current WAL position - existing data isn't automatically included. Use the backfill feature to copy historical data to ClickHouse.

### How Backfill Works

- Auto-detects primary key from PostgreSQL
- Processes rows in descending order (newest first)
- Batches of 5,000 rows for optimal performance
- Retryable via [Oban](https://github.com/oban-bg/oban) - failed batches are automatically retried
- Progress is persisted - can resume after restarts

### Mix Task

```bash
# Backfill specific tables
mix shirath.backfill users orders products

# Backfill all tables defined in mapper.json
mix shirath.backfill --all

# Check progress
mix shirath.backfill --list

# Pause a running job
mix shirath.backfill --pause 123

# Resume a paused job
mix shirath.backfill --resume 123
```

### API Endpoints

```bash
# Start backfill for specific tables
curl -X POST http://localhost:4000/api/backfill \
  -H "Content-Type: application/json" \
  -d '{"tables": ["users", "orders"]}'

# Start backfill for all tables
curl -X POST http://localhost:4000/api/backfill \
  -H "Content-Type: application/json" \
  -d '{"all": true}'

# List all backfill jobs
curl http://localhost:4000/api/backfill

# Get specific job status
curl http://localhost:4000/api/backfill/123

# Pause a job
curl -X POST http://localhost:4000/api/backfill/123/pause

# Resume a job
curl -X POST http://localhost:4000/api/backfill/123/resume
```

### Backfill Response Example

```json
{
  "jobs": [
    {
      "id": 1,
      "source_table": "users",
      "dest_table": "users",
      "status": "running",
      "total_rows": 150000,
      "processed_rows": 45000,
      "progress_percent": 30.0,
      "last_processed_id": 105000
    }
  ]
}
```

## Docker Deployment

```yaml
version: '3.8'
services:
  shirath:
    image: shirath:latest
    container_name: shirath
    build:
      context: .
      dockerfile: Dockerfile
    environment:
      CH_CONNECTION: "http://default:password@clickhouse:8123/analytics"
      DB_USERNAME: "postgres"
      DB_PASSWORD: "postgres"
      DB_NAME: "myapp"
      DB_HOST: "host.docker.internal"
      CDC_PUBLICATION: "shirath_pub"
      CDC_SLOT: "shirath_slot"
    depends_on:
      clickhouse:
        condition: service_healthy
    extra_hosts:
      - "host.docker.internal:host-gateway"

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse
    environment:
      CLICKHOUSE_DB: analytics
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: password
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  clickhouse_data:
```

> **Note:** This example assumes PostgreSQL is running on your host machine. Shirath connects to it via `host.docker.internal`. If PostgreSQL is also in Docker, replace `DB_HOST` with the container name and add it to the same network.

Run migrations in the container:

```bash
docker exec -it shirath mix ecto.migrate -r Shirath.ObanRepo
```

## Development

```bash
# Install dependencies
mix deps.get

# Run tests
mix test

# Format code
mix format

# Start interactive shell
iex -S mix
```

For a complete local development setup with sample data and schemas, see the [samples](samples/) folder.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
