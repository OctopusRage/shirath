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
  Generate query to get max value from source table for the primary key column (for cutoff).
  Always uses descending order for backfill.
  """
  def get_max_pk_query(source_table, primary_key) do
    "SELECT max(#{primary_key}) as max_pk FROM #{source_table}"
  end

  @doc """
  Generate query to get row count from source table up to cutoff.
  """
  def get_row_count_query(source_table, primary_key, cutoff_value) do
    "SELECT count() as cnt FROM #{source_table} WHERE #{primary_key} <= #{format_value(cutoff_value)}"
  end

  @doc """
  Generate a backfill query that processes data in batches.
  Uses descending order on primary_key for pagination.
  """
  def backfill_batch_query(config, primary_key, cutoff_value, last_processed_value, batch_size) do
    source_table = config["source_table"]
    target_table = config["target_table"]
    select_query = config["select_query"]

    # Build WHERE clause for pagination (descending order)
    where_clause =
      if last_processed_value do
        "WHERE #{primary_key} <= #{format_value(cutoff_value)} AND #{primary_key} < #{format_value(last_processed_value)}"
      else
        "WHERE #{primary_key} <= #{format_value(cutoff_value)}"
      end

    # Replace {source_table} with a subquery that filters by primary key
    filtered_source = """
    (SELECT * FROM #{source_table}
     #{where_clause}
     ORDER BY #{primary_key} DESC
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
  Generate query to get the minimum primary key value from the current batch (for pagination).
  Since we're going in descending order, min gives us the last value processed.
  """
  def get_batch_min_pk_query(
        source_table,
        primary_key,
        cutoff_value,
        last_processed_value,
        batch_size
      ) do
    where_clause =
      if last_processed_value do
        "WHERE #{primary_key} <= #{format_value(cutoff_value)} AND #{primary_key} < #{format_value(last_processed_value)}"
      else
        "WHERE #{primary_key} <= #{format_value(cutoff_value)}"
      end

    """
    SELECT min(#{primary_key}) as min_pk FROM (
      SELECT #{primary_key} FROM #{source_table}
      #{where_clause}
      ORDER BY #{primary_key} DESC
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

  # Format value for SQL - handle strings vs numbers
  defp format_value(value) when is_binary(value), do: "'#{value}'"
  defp format_value(value), do: to_string(value)
end
