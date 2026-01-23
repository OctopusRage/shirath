defmodule Mix.Tasks.Shirath.Mv do
  @moduledoc """
  Manages ClickHouse materialized views with two-phase backfill.

  ## Usage

      # Create a new materialized view
      mix shirath.mv create \\
        --name products_by_category \\
        --source products \\
        --engine "SummingMergeTree()" \\
        --order-by category \\
        --query "SELECT category, count() as cnt, sum(price) as total FROM {source_table} GROUP BY category"

      # List all MV jobs
      mix shirath.mv list

      # Show status of a specific MV
      mix shirath.mv status products_by_category

      # Pause a running MV job
      mix shirath.mv pause products_by_category

      # Resume a paused MV job
      mix shirath.mv resume products_by_category

      # Drop a materialized view (removes MV, target table, distributed table)
      mix shirath.mv drop products_by_category

  ## Create Options

      --name NAME        MV name (required)
      --source TABLE     Source ClickHouse table (required)
      --engine ENGINE    MergeTree engine, e.g. "SummingMergeTree()" (required)
      --order-by COLS    ORDER BY columns, comma-separated (required)
      --query SQL        SELECT query with {source_table} placeholder (required)
      --primary-key COL  Column to use for backfill pagination (default: "id")
      --cluster NAME     Cluster name, "auto" for auto-detect, omit for single-node

  ## Notes

  The two-phase backfill process:
  1. Records cutoff_id (max ID from source table)
  2. Creates MV (captures new data with id > cutoff_id)
  3. Backfills historical data (id <= cutoff_id) in batches
  4. Runs OPTIMIZE TABLE FINAL to merge all parts

  """

  use Mix.Task

  alias Shirath.MV.{MVManager, MVJob}

  @shortdoc "Manages ClickHouse materialized views"

  @switches [
    name: :string,
    source: :string,
    engine: :string,
    order_by: :string,
    query: :string,
    primary_key: :string,
    cluster: :string
  ]

  @impl Mix.Task
  def run(args) do
    # Start the application
    Mix.Task.run("app.start")

    case args do
      ["create" | rest] ->
        create_mv(rest)

      ["list"] ->
        list_jobs()

      ["status", name] ->
        show_status(name)

      ["pause", name] ->
        pause_job(name)

      ["resume", name] ->
        resume_job(name)

      ["drop", name] ->
        drop_mv(name)

      _ ->
        print_usage()
    end
  end

  defp create_mv(args) do
    {opts, _, _} = OptionParser.parse(args, switches: @switches)

    # Validate required options
    required = [:name, :source, :engine, :order_by, :query]
    missing = Enum.filter(required, fn key -> !opts[key] end)

    if length(missing) > 0 do
      Mix.shell().error(
        "Missing required options: #{Enum.map(missing, &"--#{&1}") |> Enum.join(", ")}"
      )

      exit({:shutdown, 1})
    end

    # Parse order_by into list
    order_by = opts[:order_by] |> String.split(",") |> Enum.map(&String.trim/1)

    # Primary key for backfill pagination (default: id)
    primary_key = opts[:primary_key] || "id"

    # Infer columns from query (basic parsing)
    columns = infer_columns_from_query(opts[:query])

    if length(columns) == 0 do
      Mix.shell().error("Could not infer columns from query. Please check your SELECT query.")
      exit({:shutdown, 1})
    end

    params = %{
      "name" => opts[:name],
      "source_table" => opts[:source],
      "engine" => opts[:engine],
      "order_by" => order_by,
      "columns" => columns,
      "select_query" => opts[:query],
      "primary_key" => primary_key,
      "cluster" => opts[:cluster]
    }

    Mix.shell().info("Creating materialized view '#{opts[:name]}'...")
    Mix.shell().info("  Source table: #{opts[:source]}")
    Mix.shell().info("  Engine: #{opts[:engine]}")
    Mix.shell().info("  Order by: #{Enum.join(order_by, ", ")}")
    Mix.shell().info("  Primary key: #{primary_key} (backfill pagination, descending)")
    Mix.shell().info("  Cluster: #{opts[:cluster] || "single-node"}")
    Mix.shell().info("")

    case MVManager.create(params) do
      {:ok, job} ->
        Mix.shell().info("Materialized view job created successfully!")
        Mix.shell().info("")
        print_job(job)
        Mix.shell().info("")
        Mix.shell().info("Job is now running in the background via Oban.")
        Mix.shell().info("Use `mix shirath.mv status #{job.name}` to check progress.")

      {:error, {:missing_fields, fields}} ->
        Mix.shell().error("Missing required fields: #{Enum.join(fields, ", ")}")
        exit({:shutdown, 1})

      {:error, reason} ->
        Mix.shell().error("Failed to create materialized view: #{inspect(reason)}")
        exit({:shutdown, 1})
    end
  end

  defp list_jobs do
    jobs = MVManager.list_jobs()

    if length(jobs) == 0 do
      Mix.shell().info("No materialized view jobs found.")
    else
      Mix.shell().info("Materialized View Jobs:\n")
      print_jobs_table(jobs)
    end
  end

  defp show_status(name) do
    case MVManager.get_by_name(name) do
      {:ok, job} ->
        print_job_details(job)

      {:error, :not_found} ->
        Mix.shell().error("Materialized view '#{name}' not found")
        exit({:shutdown, 1})
    end
  end

  defp pause_job(name) do
    with {:ok, job} <- MVManager.get_by_name(name),
         {:ok, job} <- MVManager.pause(job.id) do
      Mix.shell().info("Paused MV job '#{job.name}'")
    else
      {:error, :not_found} ->
        Mix.shell().error("Materialized view '#{name}' not found")
        exit({:shutdown, 1})

      {:error, :cannot_pause} ->
        Mix.shell().error("Cannot pause job in current status")
        exit({:shutdown, 1})

      {:error, reason} ->
        Mix.shell().error("Failed to pause: #{inspect(reason)}")
        exit({:shutdown, 1})
    end
  end

  defp resume_job(name) do
    with {:ok, job} <- MVManager.get_by_name(name),
         {:ok, job} <- MVManager.resume(job.id) do
      Mix.shell().info("Resumed MV job '#{job.name}'")
    else
      {:error, :not_found} ->
        Mix.shell().error("Materialized view '#{name}' not found")
        exit({:shutdown, 1})

      {:error, :not_paused} ->
        Mix.shell().error("Job is not paused")
        exit({:shutdown, 1})

      {:error, reason} ->
        Mix.shell().error("Failed to resume: #{inspect(reason)}")
        exit({:shutdown, 1})
    end
  end

  defp drop_mv(name) do
    Mix.shell().info("Dropping materialized view '#{name}'...")

    case MVManager.drop(name) do
      :ok ->
        Mix.shell().info("Materialized view '#{name}' dropped successfully")

      {:error, :not_found} ->
        Mix.shell().error("Materialized view '#{name}' not found")
        exit({:shutdown, 1})

      {:error, reason} ->
        Mix.shell().error("Failed to drop: #{inspect(reason)}")
        exit({:shutdown, 1})
    end
  end

  defp print_jobs_table(jobs) do
    header = "| Name | Source | Status | Progress | Cluster |"
    separator = "|" <> String.duplicate("-", String.length(header) - 2) <> "|"

    Mix.shell().info(separator)
    Mix.shell().info(header)
    Mix.shell().info(separator)

    Enum.each(jobs, fn job ->
      progress =
        if job.total_rows && job.total_rows > 0 do
          pct = MVJob.progress_percent(job)
          "#{job.processed_rows}/#{job.total_rows} (#{pct}%)"
        else
          "#{job.processed_rows || 0}"
        end

      status_display = format_status(job.status)
      cluster = job.cluster_name || "-"

      row =
        "| #{pad(job.name, 25)} | #{pad(job.source_table, 15)} | #{pad(status_display, 12)} | #{pad(progress, 20)} | #{pad(cluster, 12)} |"

      Mix.shell().info(row)
    end)

    Mix.shell().info(separator)
  end

  defp print_job(job) do
    Mix.shell().info("  ID: #{job.id}")
    Mix.shell().info("  Name: #{job.name}")
    Mix.shell().info("  Status: #{job.status}")
    Mix.shell().info("  Source: #{job.source_table}")
    Mix.shell().info("  Target: #{job.target_table}")

    if job.distributed_table do
      Mix.shell().info("  Distributed: #{job.distributed_table}")
    end

    if job.cluster_name do
      Mix.shell().info("  Cluster: #{job.cluster_name}")
    end
  end

  defp print_job_details(job) do
    Mix.shell().info("Materialized View: #{job.name}")
    Mix.shell().info(String.duplicate("=", 50))
    Mix.shell().info("")
    Mix.shell().info("  ID:           #{job.id}")
    Mix.shell().info("  Status:       #{format_status(job.status)}")
    Mix.shell().info("  Source Table: #{job.source_table}")
    Mix.shell().info("  Target Table: #{job.target_table}")

    if job.distributed_table do
      Mix.shell().info("  Distributed:  #{job.distributed_table}")
    end

    if job.cluster_name do
      Mix.shell().info("  Cluster:      #{job.cluster_name}")
    end

    Mix.shell().info("")
    Mix.shell().info("Backfill Progress:")
    Mix.shell().info("  Cutoff ID:    #{job.cutoff_id || "-"}")
    Mix.shell().info("  Total Rows:   #{job.total_rows || "-"}")
    Mix.shell().info("  Processed:    #{job.processed_rows || 0}")

    if job.total_rows && job.total_rows > 0 do
      pct = MVJob.progress_percent(job)
      Mix.shell().info("  Progress:     #{pct}%")
    end

    if job.last_processed_id do
      Mix.shell().info("  Last ID:      #{job.last_processed_id}")
    end

    if job.error_message do
      Mix.shell().info("")
      Mix.shell().info("Error: #{job.error_message}")
    end

    Mix.shell().info("")
    Mix.shell().info("  Created: #{job.inserted_at}")
    Mix.shell().info("  Updated: #{job.updated_at}")
  end

  defp format_status("completed"), do: "completed"
  defp format_status("creating"), do: "creating"
  defp format_status("backfilling"), do: "backfilling"
  defp format_status("optimizing"), do: "optimizing"
  defp format_status("pending"), do: "pending"
  defp format_status("paused"), do: "paused"
  defp format_status("failed"), do: "FAILED"
  defp format_status(other), do: other

  defp pad(value, width) do
    str = to_string(value)
    String.pad_trailing(str, width)
  end

  # Basic column inference from SELECT query
  # This is a simple parser that extracts column aliases
  defp infer_columns_from_query(query) do
    # Extract the SELECT ... FROM portion
    case Regex.run(~r/SELECT\s+(.+?)\s+FROM/is, query) do
      [_, select_part] ->
        select_part
        |> String.split(",")
        |> Enum.map(&String.trim/1)
        |> Enum.map(&extract_column_info/1)
        |> Enum.reject(&is_nil/1)

      _ ->
        []
    end
  end

  # Extract column name and infer type from common patterns
  defp extract_column_info(col_expr) do
    # Try to match "expr as alias" pattern
    case Regex.run(~r/(.+)\s+as\s+(\w+)$/i, col_expr) do
      [_, expr, alias_name] ->
        %{"name" => alias_name, "type" => infer_type(expr)}

      _ ->
        # If no alias, use the expression as name (for simple column refs)
        name = col_expr |> String.split(".") |> List.last() |> String.trim()

        if String.match?(name, ~r/^\w+$/) do
          %{"name" => name, "type" => "String"}
        else
          nil
        end
    end
  end

  # Infer ClickHouse type from common aggregate functions
  defp infer_type(expr) do
    expr = String.downcase(expr)

    cond do
      String.contains?(expr, "count(") -> "UInt64"
      String.contains?(expr, "sum(") -> "Float64"
      String.contains?(expr, "avg(") -> "Float64"
      String.contains?(expr, "min(") -> "Float64"
      String.contains?(expr, "max(") -> "Float64"
      String.contains?(expr, "uniq(") -> "UInt64"
      true -> "String"
    end
  end

  defp print_usage do
    Mix.shell().info(@moduledoc)
  end
end
