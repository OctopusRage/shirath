defmodule Shirath.MV.SQLBuilder do
  @moduledoc """
  Generates SQL for materialized view creation with cluster support.
  """

  @doc """
  Generate CREATE TABLE statement for the target table.
  """
  def create_target_table(config, cluster \\ nil) do
    cluster_clause = cluster_clause(cluster)
    columns = format_columns(config["columns"])
    order_by = format_order_by(config["order_by"])
    engine = config["engine"] || "MergeTree()"

    """
    CREATE TABLE IF NOT EXISTS #{config["target_table"]}
    #{cluster_clause}
    (
    #{columns}
    )
    ENGINE = #{engine}
    ORDER BY #{order_by}
    """
    |> String.trim()
  end

  @doc """
  Generate CREATE MATERIALIZED VIEW statement.
  """
  def create_materialized_view(config, cluster \\ nil) do
    cluster_clause = cluster_clause(cluster)
    select_query = build_select_query(config)

    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS #{config["name"]}
    #{cluster_clause}
    TO #{config["target_table"]}
    AS #{select_query}
    """
    |> String.trim()
  end

  @doc """
  Generate CREATE TABLE statement for the distributed table.
  """
  def create_distributed_table(config, cluster) do
    distributed_table = config["distributed_table"] || "#{config["name"]}_dist"

    """
    CREATE TABLE IF NOT EXISTS #{distributed_table}
    ON CLUSTER '#{cluster}'
    AS #{config["target_table"]}
    ENGINE = Distributed('#{cluster}', currentDatabase(), '#{config["target_table"]}', rand())
    """
    |> String.trim()
  end

  @doc """
  Generate query to get max ID from source table (for cutoff).
  """
  def get_max_id_query(source_table, id_column \\ "id") do
    "SELECT max(#{id_column}) as max_id FROM #{source_table}"
  end

  @doc """
  Generate query to get row count from source table.
  """
  def get_row_count_query(source_table) do
    "SELECT count() as cnt FROM #{source_table}"
  end

  @doc """
  Generate backfill INSERT query with batching.
  Inserts aggregated data from source table into target table.
  """
  def backfill_query(config, cutoff_id, last_processed_id \\ nil, batch_size \\ 10_000) do
    select_query = build_select_query(config)
    source_table = config["source_table"]

    # Extract the base query and add WHERE clause for batching
    # We need to modify the query to add ID filtering
    _where_clause = build_backfill_where(cutoff_id, last_processed_id)

    # Wrap the original query to add filtering
    """
    INSERT INTO #{config["target_table"]}
    SELECT * FROM (
      #{select_query}
    ) AS subq
    WHERE EXISTS (
      SELECT 1 FROM #{source_table}
      WHERE id <= #{cutoff_id}
      #{if last_processed_id, do: "AND id < #{last_processed_id}", else: ""}
      LIMIT #{batch_size}
    )
    """
    |> String.trim()
  end

  @doc """
  Generate a simpler backfill query that processes data in batches by ID range.
  This approach reads source data with ID filtering, then aggregates.
  """
  def backfill_batch_query(config, cutoff_id, last_processed_id, batch_size) do
    source_table = config["source_table"]
    target_table = config["target_table"]
    select_query = config["select_query"]

    # Replace {source_table} with a subquery that filters by ID
    filtered_source = """
    (SELECT * FROM #{source_table}
     WHERE id <= #{cutoff_id}
     #{if last_processed_id, do: "AND id < #{last_processed_id}", else: ""}
     ORDER BY id DESC
     LIMIT #{batch_size})
    """

    modified_query = String.replace(select_query, "{source_table}", filtered_source)

    """
    INSERT INTO #{target_table}
    #{modified_query}
    """
    |> String.trim()
  end

  @doc """
  Generate query to get the minimum ID from the current batch (for pagination).
  """
  def get_batch_min_id_query(source_table, cutoff_id, last_processed_id, batch_size) do
    """
    SELECT min(id) as min_id FROM (
      SELECT id FROM #{source_table}
      WHERE id <= #{cutoff_id}
      #{if last_processed_id, do: "AND id < #{last_processed_id}", else: ""}
      ORDER BY id DESC
      LIMIT #{batch_size}
    )
    """
    |> String.trim()
  end

  @doc """
  Generate OPTIMIZE TABLE statement.
  """
  def optimize_table(target_table, cluster \\ nil) do
    if cluster do
      "OPTIMIZE TABLE #{target_table} ON CLUSTER '#{cluster}' FINAL"
    else
      "OPTIMIZE TABLE #{target_table} FINAL"
    end
  end

  @doc """
  Generate DROP statements for MV cleanup.
  """
  def drop_materialized_view(name, cluster \\ nil) do
    cluster_clause = cluster_clause(cluster)

    "DROP VIEW IF EXISTS #{name} #{cluster_clause}"
    |> String.trim()
  end

  def drop_table(table_name, cluster \\ nil) do
    cluster_clause = cluster_clause(cluster)

    "DROP TABLE IF EXISTS #{table_name} #{cluster_clause}"
    |> String.trim()
  end

  @doc """
  Query to detect cluster configuration.
  """
  def detect_cluster_query do
    "SELECT cluster, host_name, host_address, port FROM system.clusters ORDER BY cluster, host_name"
  end

  # Private functions

  defp cluster_clause(nil), do: ""
  defp cluster_clause(cluster), do: "ON CLUSTER '#{cluster}'"

  defp format_columns(columns) when is_list(columns) do
    columns
    |> Enum.map(fn col ->
      name = col["name"]
      type = col["type"]
      default = if col["default"], do: " DEFAULT #{col["default"]}", else: ""
      "  #{name} #{type}#{default}"
    end)
    |> Enum.join(",\n")
  end

  defp format_order_by(order_by) when is_list(order_by) do
    "(#{Enum.join(order_by, ", ")})"
  end

  defp format_order_by(order_by) when is_binary(order_by) do
    "(#{order_by})"
  end

  defp build_select_query(config) do
    config["select_query"]
    |> String.replace("{source_table}", config["source_table"])
  end

  defp build_backfill_where(cutoff_id, nil) do
    "id <= #{cutoff_id}"
  end

  defp build_backfill_where(cutoff_id, last_processed_id) do
    "id <= #{cutoff_id} AND id < #{last_processed_id}"
  end
end
