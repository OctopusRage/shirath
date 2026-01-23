defmodule Shirath.MV.Cluster do
  @moduledoc """
  Handles ClickHouse cluster detection and management.
  """

  alias Shirath.MV.SQLBuilder

  @doc """
  Detect cluster configuration from ClickHouse.
  Returns {:ok, cluster_info} or {:ok, nil} if no cluster.

  cluster_info structure:
  %{
    name: "my_cluster",
    nodes: [
      %{host_name: "node1", host_address: "10.0.0.1", port: 9000},
      %{host_name: "node2", host_address: "10.0.0.2", port: 9000}
    ]
  }
  """
  def detect_cluster do
    query = SQLBuilder.detect_cluster_query()

    case ClickhouseMaster.select(query) do
      {:ok, %{rows: []}} ->
        {:ok, nil}

      {:ok, %{rows: rows}} ->
        clusters = parse_cluster_rows(rows)

        # Return the first non-system cluster, or nil if only system clusters
        cluster =
          clusters
          |> Enum.reject(fn {name, _} -> system_cluster?(name) end)
          |> List.first()

        case cluster do
          nil -> {:ok, nil}
          {name, nodes} -> {:ok, %{name: name, nodes: nodes}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Get cluster by name.
  """
  def get_cluster(cluster_name) do
    query =
      "SELECT cluster, host_name, host_address, port FROM system.clusters WHERE cluster = '#{cluster_name}'"

    case ClickhouseMaster.select(query) do
      {:ok, %{rows: []}} ->
        {:error, :not_found}

      {:ok, %{rows: rows}} ->
        nodes = Enum.map(rows, &parse_node_row/1)
        {:ok, %{name: cluster_name, nodes: nodes}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Resolve cluster from config.
  - "auto" -> auto-detect
  - nil -> no cluster (single node)
  - "cluster_name" -> specific cluster
  """
  def resolve_cluster("auto") do
    case detect_cluster() do
      {:ok, nil} -> {:ok, nil}
      {:ok, cluster} -> {:ok, cluster.name}
      {:error, reason} -> {:error, reason}
    end
  end

  def resolve_cluster(nil), do: {:ok, nil}

  def resolve_cluster(cluster_name) when is_binary(cluster_name) do
    case get_cluster(cluster_name) do
      {:ok, _cluster} -> {:ok, cluster_name}
      {:error, :not_found} -> {:error, {:cluster_not_found, cluster_name}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Get list of node hostnames for a cluster.
  """
  def get_node_hosts(cluster_name) do
    case get_cluster(cluster_name) do
      {:ok, %{nodes: nodes}} ->
        hosts = Enum.map(nodes, & &1.host_name)
        {:ok, hosts}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private functions

  defp parse_cluster_rows(rows) do
    rows
    |> Enum.group_by(fn row -> Enum.at(row, 0) end)
    |> Enum.map(fn {cluster_name, cluster_rows} ->
      nodes = Enum.map(cluster_rows, &parse_node_row/1)
      {cluster_name, nodes}
    end)
  end

  defp parse_node_row(row) do
    %{
      host_name: Enum.at(row, 1),
      host_address: Enum.at(row, 2),
      port: Enum.at(row, 3)
    }
  end

  defp system_cluster?(name) do
    # Filter out system/internal clusters
    name in ["system", "_system", "default_cluster"] or
      String.starts_with?(name, "_")
  end
end
