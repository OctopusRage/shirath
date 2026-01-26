# Materialized View Support - Implementation Plan

## Overview

This document outlines the plan to add materialized view (MV) support to Shirath, including:
- MV creation and management via API only (not in mapper.json)
- Cluster-aware MV creation (`ON CLUSTER`)
- Two-phase backfill to handle data overlap
- API and CLI for managing MVs

## Architecture

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MATERIALIZED VIEW FLOW                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────────────────────┐ │
│  │ PostgreSQL  │ CDC  │   Shirath   │      │        ClickHouse          │ │
│  │   (Source)  │─────▶│  Ingestor   │─────▶│                             │ │
│  └─────────────┘      └─────────────┘      │  ┌─────────────────────┐   │ │
│                                             │  │   products (local)  │   │ │
│                                             │  └──────────┬──────────┘   │ │
│                                             │             │              │ │
│                                             │             ▼ MV trigger   │ │
│                                             │  ┌─────────────────────┐   │ │
│                                             │  │ products_by_category│   │ │
│                                             │  │   (local MV data)   │   │ │
│                                             │  └──────────┬──────────┘   │ │
│                                             │             │              │ │
│                                             │             ▼ query        │ │
│                                             │  ┌─────────────────────┐   │ │
│                                             │  │ products_by_category│   │ │
│                                             │  │      _dist          │   │ │
│                                             │  │  (Distributed)      │   │ │
│                                             │  └─────────────────────┘   │ │
│                                             └─────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Cluster-Aware Architecture

```
                         ┌──────────────────────┐
                         │    Shirath Server    │
                         │                      │
                         │  MVManager (GenServer)│
                         └──────────┬───────────┘
                                    │
            ┌───────────────────────┼───────────────────────┐
            │                       │                       │
            ▼                       ▼                       ▼
     ┌─────────────┐         ┌─────────────┐         ┌─────────────┐
     │   Node 1    │         │   Node 2    │         │   Node 3    │
     │  (Shard 1)  │         │  (Shard 2)  │         │  (Shard 3)  │
     │             │         │             │         │             │
     │ products    │         │ products    │         │ products    │
     │ products_mv │         │ products_mv │         │ products_mv │
     └─────────────┘         └─────────────┘         └─────────────┘
            │                       │                       │
            └───────────────────────┼───────────────────────┘
                                    │
                         ┌──────────▼───────────┐
                         │  products_mv_dist    │
                         │   (Distributed)      │
                         └──────────────────────┘
```

## Configuration

MVs are managed entirely via API - no configuration in mapper.json required.

### API Request Options

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | MV name (used for target table: `{name}_data`, distributed: `{name}_dist`) |
| `source_table` | Yes | ClickHouse source table (local table name) |
| `target_table` | No | Explicit target table name (defaults to `{name}_data`) |
| `distributed_table` | No | Distributed table name (defaults to `{name}_dist`) |
| `engine` | Yes | MergeTree engine type (`SummingMergeTree`, `AggregatingMergeTree`, etc.) |
| `order_by` | Yes | ORDER BY columns for the target table |
| `columns` | Yes | Column definitions for target table |
| `select_query` | Yes | SELECT query for MV (use `{source_table}` placeholder) |
| `cluster` | No | Cluster name or `"auto"` for auto-detection, `null` for single-node |

## Database Schema

### New Migration: `mv_jobs` Table

```sql
CREATE TABLE mv_jobs (
  id BIGSERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  source_table VARCHAR(255) NOT NULL,
  target_table VARCHAR(255) NOT NULL,
  status VARCHAR(50) NOT NULL DEFAULT 'pending',
  -- pending, creating, backfilling, optimizing, completed, failed, paused
  
  -- Cluster info
  cluster_name VARCHAR(255),
  nodes JSONB DEFAULT '[]',
  
  -- Backfill tracking
  cutoff_id BIGINT,
  total_rows BIGINT DEFAULT 0,
  processed_rows BIGINT DEFAULT 0,
  last_processed_id BIGINT,
  
  -- Metadata
  config JSONB NOT NULL,
  error_message TEXT,
  
  inserted_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_mv_jobs_name ON mv_jobs(name);
CREATE INDEX idx_mv_jobs_status ON mv_jobs(status);
```

## Implementation Components

### 1. MVJob Schema (`lib/backfill/mv_job.ex`)

```elixir
defmodule Shirath.Backfill.MVJob do
  use Ecto.Schema
  import Ecto.Changeset

  schema "mv_jobs" do
    field :name, :string
    field :source_table, :string
    field :target_table, :string
    field :status, :string, default: "pending"
    field :cluster_name, :string
    field :nodes, {:array, :string}, default: []
    field :cutoff_id, :integer
    field :total_rows, :integer, default: 0
    field :processed_rows, :integer, default: 0
    field :last_processed_id, :integer
    field :config, :map
    field :error_message, :string

    timestamps()
  end
end
```

### 2. MVManager Module (`lib/mv/mv_manager.ex`)

Main context module for MV operations:

```elixir
defmodule Shirath.MV.MVManager do
  @moduledoc """
  Manages materialized view lifecycle:
  - Creation (with ON CLUSTER support)
  - Two-phase backfill
  - Status tracking
  """

  # Detect cluster configuration from ClickHouse
  def detect_cluster() do
    query = "SELECT cluster, host_name FROM system.clusters"
    # Returns {:ok, %{name: "cluster", nodes: [...]}} or {:ok, nil}
  end

  # Create MV with all required objects
  def create_mv(config) do
    # 1. Get cutoff_id BEFORE creating MV
    # 2. Create target table (ON CLUSTER if clustered)
    # 3. Create materialized view (ON CLUSTER if clustered)
    # 4. Create distributed table (if clustered)
    # 5. Save job with cutoff_id
    # 6. Enqueue backfill worker
  end

  # Backfill data <= cutoff_id
  def backfill_mv(job_id) do
    # 1. Read from source table WHERE id <= cutoff_id
    # 2. Apply aggregation query
    # 3. Insert into target table
    # 4. Update progress
  end

  # Final optimization
  def optimize_mv(job_id) do
    # OPTIMIZE TABLE target_table FINAL
  end
end
```

### 3. MV Creation Worker (`lib/workers/mv_create_worker.ex`)

```elixir
defmodule Shirath.Workers.MVCreateWorker do
  use Oban.Worker, queue: :mat_view, max_attempts: 3

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"mv_job_id" => job_id}}) do
    with {:ok, job} <- MVManager.get_job(job_id),
         {:ok, _} <- MVManager.create_tables(job),
         {:ok, _} <- MVManager.create_mv(job),
         {:ok, _} <- MVManager.create_distributed(job) do
      # Enqueue backfill
      %{mv_job_id: job_id}
      |> MVBackfillWorker.new()
      |> Oban.insert()
    end
  end
end
```

### 4. MV Backfill Worker (`lib/workers/mv_backfill_worker.ex`)

```elixir
defmodule Shirath.Workers.MVBackfillWorker do
  use Oban.Worker, queue: :mat_view, max_attempts: 5, priority: 3

  @batch_size 10_000

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"mv_job_id" => job_id}}) do
    job = MVManager.get_job!(job_id)
    
    case process_batch(job) do
      {:ok, :continue} ->
        # Re-enqueue for next batch
        %{mv_job_id: job_id}
        |> __MODULE__.new()
        |> Oban.insert()
        :ok

      {:ok, :complete} ->
        # Enqueue optimize step
        %{mv_job_id: job_id}
        |> MVOptimizeWorker.new()
        |> Oban.insert()
        :ok

      {:error, reason} ->
        MVManager.mark_failed(job_id, reason)
        {:error, reason}
    end
  end

  defp process_batch(job) do
    # 1. Fetch batch from ClickHouse source table
    # 2. Apply SELECT query with GROUP BY
    # 3. Insert into target table
    # 4. Update job progress
  end
end
```

### 5. SQL Generation (`lib/mv/sql_builder.ex`)

```elixir
defmodule Shirath.MV.SQLBuilder do
  @moduledoc """
  Generates SQL for MV creation with cluster support.
  """

  def create_target_table(config, cluster) do
    cluster_clause = if cluster, do: "ON CLUSTER '#{cluster}'", else: ""
    
    """
    CREATE TABLE IF NOT EXISTS #{config.target_table}
    #{cluster_clause}
    (
      #{format_columns(config.columns)}
    )
    ENGINE = #{config.engine}
    ORDER BY (#{Enum.join(config.order_by, ", ")})
    """
  end

  def create_materialized_view(config, cluster) do
    cluster_clause = if cluster, do: "ON CLUSTER '#{cluster}'", else: ""
    source = String.replace(config.select_query, "{source_table}", config.source_table)
    
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS #{config.name}
    #{cluster_clause}
    TO #{config.target_table}
    AS #{source}
    """
  end

  def create_distributed_table(config, cluster) do
    """
    CREATE TABLE IF NOT EXISTS #{config.distributed_table}
    ON CLUSTER '#{cluster}'
    AS #{config.target_table}
    ENGINE = Distributed('#{cluster}', currentDatabase(), '#{config.target_table}', rand())
    """
  end

  def backfill_query(config, cutoff_id, last_id, batch_size) do
    base_query = String.replace(config.select_query, "{source_table}", config.source_table)
    
    # Add WHERE clause for batching
    """
    INSERT INTO #{config.target_table}
    #{add_where_clause(base_query, cutoff_id, last_id, batch_size)}
    """
  end
end
```

### 6. HTTP API (`lib/controllers/mv_controller.ex`)

```elixir
defmodule Shirath.Controllers.MVController do
  use Plug.Router

  # GET /api/mv - List all MV jobs
  get "/" do
    jobs = MVManager.list_jobs()
    send_json(conn, 200, %{jobs: jobs})
  end

  # POST /api/mv - Create new MV
  post "/" do
    with {:ok, body} <- parse_body(conn),
         {:ok, job} <- MVManager.create(body) do
      send_json(conn, 201, %{job: job})
    end
  end

  # GET /api/mv/:name - Get MV status
  get "/:name" do
    case MVManager.get_by_name(name) do
      {:ok, job} -> send_json(conn, 200, %{job: job})
      {:error, :not_found} -> send_json(conn, 404, %{error: "Not found"})
    end
  end

  # POST /api/mv/:name/backfill - Trigger backfill
  post "/:name/backfill" do
    case MVManager.start_backfill(name) do
      {:ok, job} -> send_json(conn, 200, %{job: job})
      {:error, reason} -> send_json(conn, 400, %{error: reason})
    end
  end

  # DELETE /api/mv/:name - Drop MV
  delete "/:name" do
    case MVManager.drop(name) do
      :ok -> send_json(conn, 200, %{status: "deleted"})
      {:error, reason} -> send_json(conn, 400, %{error: reason})
    end
  end
end
```

### 7. Mix Task (`lib/mix/tasks/shirath.mv.ex`)

```bash
# Create MV from mapper.json config
mix shirath.mv create products_by_category

# Create all MVs defined in config
mix shirath.mv create --all

# List MV jobs
mix shirath.mv list

# Check status
mix shirath.mv status products_by_category

# Trigger backfill (if MV exists but not backfilled)
mix shirath.mv backfill products_by_category

# Drop MV (removes MV, target table, distributed table)
mix shirath.mv drop products_by_category
```

## Two-Phase Backfill Process

### Phase 1: Create MV (captures new data)

```
Timeline:
────────────────────────────────────────────────────────────►
    │
    ▼
    1. Record cutoff_id = SELECT max(id) FROM products
    2. CREATE MATERIALIZED VIEW (now captures id > cutoff_id)
    3. Save job with cutoff_id
```

### Phase 2: Backfill historical data

```
Timeline:
────────────────────────────────────────────────────────────►
                │
                ▼
                4. INSERT INTO target_table
                   SELECT ... FROM products WHERE id <= cutoff_id
                   (batched, 10K rows at a time)
                5. OPTIMIZE TABLE target_table FINAL
```

### Handling Edge Cases

| Scenario | Solution |
|----------|----------|
| Backfill fails mid-way | Job tracks `last_processed_id`, can resume |
| Duplicate data | Target table uses appropriate MergeTree engine to handle |
| Cluster node failure | Job tracks per-node progress, can retry individual nodes |
| New data during backfill | MV captures it automatically (id > cutoff_id) |

## File Structure

```
lib/
├── mv/
│   ├── mv_manager.ex        # Main context module
│   ├── mv_job.ex            # Ecto schema
│   ├── sql_builder.ex       # SQL generation
│   └── cluster.ex           # Cluster detection/management
├── workers/
│   ├── mv_create_worker.ex  # Creates MV objects
│   ├── mv_backfill_worker.ex # Backfills data
│   └── mv_optimize_worker.ex # Runs OPTIMIZE
├── controllers/
│   └── mv_controller.ex     # HTTP API
└── mix/tasks/
    └── shirath.mv.ex        # CLI

priv/oban_repo/migrations/
└── YYYYMMDD_create_mv_jobs.exs
```

## API Reference

### POST /api/mv

Create a new materialized view.

**Request:**
```json
{
  "name": "products_by_category",
  "source_table": "products",
  "engine": "SummingMergeTree()",
  "order_by": ["category"],
  "columns": [
    {"name": "category", "type": "String"},
    {"name": "cnt", "type": "UInt64"},
    {"name": "total_price", "type": "Decimal(18,2)"}
  ],
  "select_query": "SELECT category, count() as cnt, sum(price) as total_price FROM {source_table} GROUP BY category"
}
```

**Response:**
```json
{
  "job": {
    "id": 1,
    "name": "products_by_category",
    "status": "creating",
    "cutoff_id": 50000,
    "total_rows": 50000,
    "processed_rows": 0
  }
}
```

### GET /api/mv/:name

Get MV job status.

**Response:**
```json
{
  "job": {
    "id": 1,
    "name": "products_by_category",
    "source_table": "products",
    "target_table": "products_by_category_data",
    "status": "backfilling",
    "cluster_name": "my_cluster",
    "cutoff_id": 50000,
    "total_rows": 50000,
    "processed_rows": 35000,
    "progress_percent": 70.0,
    "created_at": "2024-01-20T10:00:00Z",
    "updated_at": "2024-01-20T10:05:00Z"
  }
}
```

## Implementation Order

1. **Phase 1: Core Infrastructure**
   - [ ] Create migration for `mv_jobs` table
   - [ ] Implement `MVJob` schema
   - [ ] Implement `SQLBuilder` module
   - [ ] Implement cluster detection

2. **Phase 2: MV Creation**
   - [ ] Implement `MVManager.create/1`
   - [ ] Implement `MVCreateWorker`
   - [ ] Add basic API endpoint

3. **Phase 3: Backfill**
   - [ ] Implement `MVBackfillWorker`
   - [ ] Implement `MVOptimizeWorker`
   - [ ] Add progress tracking

4. **Phase 4: API & CLI**
   - [ ] Complete `MVController`
   - [ ] Implement `mix shirath.mv` task
   - [ ] Add to router

5. **Phase 5: Testing & Docs**
   - [ ] Unit tests
   - [ ] Integration tests with ClickHouse cluster
   - [ ] Update README

## Example Usage

### 1. Create the MV via API

```bash
curl -X POST http://localhost:4000/api/mv \
  -H "Content-Type: application/json" \
  -d '{
    "name": "products_by_category",
    "source_table": "products",
    "engine": "SummingMergeTree()",
    "order_by": ["category"],
    "columns": [
      {"name": "category", "type": "String"},
      {"name": "cnt", "type": "UInt64"},
      {"name": "total_price", "type": "Decimal(18,2)"}
    ],
    "select_query": "SELECT category, count() as cnt, sum(price) as total_price FROM {source_table} GROUP BY category"
  }'
```

Or via CLI:
```bash
mix shirath.mv create \
  --name products_by_category \
  --source products \
  --engine "SummingMergeTree()" \
  --order-by category \
  --query "SELECT category, count() as cnt, sum(price) as total_price FROM {source_table} GROUP BY category"
```

### 2. Monitor progress

```bash
# Via CLI
mix shirath.mv status products_by_category

# Via API
curl http://localhost:4000/api/mv/products_by_category
```

### 3. Query the MV

```sql
-- Single node
SELECT * FROM products_by_category_data;

-- Cluster (aggregates from all shards)
SELECT category, sum(cnt), sum(total_price)
FROM products_by_category_dist
GROUP BY category;
```
