defmodule Shirath.MV.MVManager do
  @moduledoc """
  Manages materialized view lifecycle:
  - Creation (with ON CLUSTER support)
  - Two-phase backfill (always descending order)
  - Status tracking
  """

  import Ecto.Query
  alias Shirath.ObanRepo
  alias Shirath.MV.{MVJob, SQLBuilder, Cluster}
  alias Shirath.Workers.MVCreateWorker

  @doc """
  Create a new materialized view job.
  This validates the config and enqueues the creation worker.
  """
  def create(params) do
    with {:ok, config} <- validate_config(params),
         {:ok, cluster_name} <- Cluster.resolve_cluster(config["cluster"]),
         {:ok, job} <- create_job(config, cluster_name) do
      # Enqueue the creation worker
      %{mv_job_id: job.id}
      |> MVCreateWorker.new()
      |> Oban.insert()

      {:ok, job}
    end
  end

  @doc """
  Get a job by ID.
  """
  def get_job(id) do
    case ObanRepo.get(MVJob, id) do
      nil -> {:error, :not_found}
      job -> {:ok, job}
    end
  end

  def get_job!(id) do
    ObanRepo.get!(MVJob, id)
  end

  @doc """
  Get a job by name.
  """
  def get_by_name(name) do
    case ObanRepo.get_by(MVJob, name: name) do
      nil -> {:error, :not_found}
      job -> {:ok, job}
    end
  end

  @doc """
  List all MV jobs.
  """
  def list_jobs(opts \\ []) do
    query =
      from(j in MVJob, order_by: [desc: j.inserted_at])

    query =
      case Keyword.get(opts, :status) do
        nil -> query
        status -> from(j in query, where: j.status == ^status)
      end

    ObanRepo.all(query)
  end

  @doc """
  Create the target table in ClickHouse.
  """
  def create_target_table(%MVJob{} = job) do
    sql = SQLBuilder.create_target_table(job.config, job.cluster_name)

    case ClickhouseMaster.query(sql) do
      {:ok, _} -> {:ok, job}
      {:error, reason} -> {:error, {:create_target_table_failed, reason}}
    end
  end

  @doc """
  Create the materialized view in ClickHouse.
  """
  def create_materialized_view(%MVJob{} = job) do
    sql = SQLBuilder.create_materialized_view(job.config, job.cluster_name)

    case ClickhouseMaster.query(sql) do
      {:ok, _} -> {:ok, job}
      {:error, reason} -> {:error, {:create_mv_failed, reason}}
    end
  end

  @doc """
  Create the distributed table (for clustered setups).
  """
  def create_distributed_table(%MVJob{cluster_name: nil} = job), do: {:ok, job}

  def create_distributed_table(%MVJob{} = job) do
    sql = SQLBuilder.create_distributed_table(job.config, job.cluster_name)

    case ClickhouseMaster.query(sql) do
      {:ok, _} -> {:ok, job}
      {:error, reason} -> {:error, {:create_distributed_failed, reason}}
    end
  end

  @doc """
  Get cutoff value and total rows for backfill.
  This should be called BEFORE creating the MV.
  Uses the configured primary_key column.
  """
  def setup_backfill(%MVJob{} = job) do
    source_table = job.config["source_table"]
    primary_key = job.primary_key || "id"

    with {:ok, cutoff_value} <- get_max_pk(source_table, primary_key),
         {:ok, total_rows} <- get_row_count(source_table, primary_key, cutoff_value) do
      job
      |> MVJob.backfill_setup_changeset(%{
        cutoff_id: cutoff_value,
        total_rows: total_rows,
        status: "backfilling"
      })
      |> ObanRepo.update()
    end
  end

  @doc """
  Fetch a batch of data and insert into target table.
  Always processes in descending order on primary_key.
  Returns {:ok, :continue} if more data, {:ok, :complete} if done.
  """
  def process_backfill_batch(%MVJob{} = job, batch_size \\ 10_000) do
    config = job.config
    primary_key = job.primary_key || "id"
    cutoff_value = job.cutoff_id
    last_processed_value = job.last_processed_id

    # Get the min pk from this batch (for pagination in descending order)
    with {:ok, batch_min_pk} <-
           get_batch_min_pk(
             config["source_table"],
             primary_key,
             cutoff_value,
             last_processed_value,
             batch_size
           ),
         :ok <-
           execute_backfill_batch(
             config,
             primary_key,
             cutoff_value,
             last_processed_value,
             batch_size
           ) do
      if batch_min_pk == nil do
        # No more data
        {:ok, :complete}
      else
        # Update progress
        processed = (job.processed_rows || 0) + batch_size

        job
        |> MVJob.progress_changeset(%{
          processed_rows: min(processed, job.total_rows),
          last_processed_id: batch_min_pk
        })
        |> ObanRepo.update()

        {:ok, :continue}
      end
    end
  end

  @doc """
  Run OPTIMIZE TABLE FINAL on the target table.
  """
  def optimize(%MVJob{} = job) do
    sql = SQLBuilder.optimize_table(job.config["target_table"], job.cluster_name)

    case ClickhouseMaster.query(sql) do
      {:ok, _} -> {:ok, job}
      {:error, reason} -> {:error, {:optimize_failed, reason}}
    end
  end

  @doc """
  Mark job as completed.
  """
  def mark_completed(job_id) do
    job = get_job!(job_id)

    job
    |> MVJob.status_changeset("completed", %{processed_rows: job.total_rows})
    |> ObanRepo.update()
  end

  @doc """
  Mark job as failed with error message.
  """
  def mark_failed(job_id, reason) do
    job = get_job!(job_id)
    error_msg = inspect(reason)

    job
    |> MVJob.status_changeset("failed", %{error_message: error_msg})
    |> ObanRepo.update()
  end

  @doc """
  Update job status.
  """
  def update_status(job_id, status) do
    job = get_job!(job_id)

    job
    |> MVJob.status_changeset(status)
    |> ObanRepo.update()
  end

  @doc """
  Pause a running job.
  """
  def pause(job_id) do
    with {:ok, job} <- get_job(job_id),
         true <- job.status in ["backfilling", "creating"],
         {:ok, job} <- update_status(job_id, "paused") do
      {:ok, job}
    else
      false -> {:error, :cannot_pause}
      error -> error
    end
  end

  @doc """
  Resume a paused job.
  """
  def resume(job_id) do
    with {:ok, job} <- get_job(job_id),
         true <- job.status == "paused" do
      # Re-enqueue the appropriate worker based on where we left off
      if job.cutoff_id do
        # Was in backfill phase
        %{mv_job_id: job.id}
        |> Shirath.Workers.MVBackfillWorker.new()
        |> Oban.insert()
      else
        # Was in creation phase
        %{mv_job_id: job.id}
        |> MVCreateWorker.new()
        |> Oban.insert()
      end

      update_status(job_id, if(job.cutoff_id, do: "backfilling", else: "creating"))
    else
      false -> {:error, :not_paused}
      error -> error
    end
  end

  @doc """
  Drop a materialized view and its associated tables.
  """
  def drop(name) do
    with {:ok, job} <- get_by_name(name) do
      cluster = job.cluster_name

      # Drop in order: MV -> distributed -> target
      ClickhouseMaster.query(SQLBuilder.drop_materialized_view(job.name, cluster))

      if job.distributed_table,
        do: ClickhouseMaster.query(SQLBuilder.drop_table(job.distributed_table, cluster))

      ClickhouseMaster.query(SQLBuilder.drop_table(job.target_table, cluster))

      # Delete the job record
      ObanRepo.delete(job)

      :ok
    end
  end

  # Private functions

  defp validate_config(params) do
    required = ~w(name source_table engine order_by columns select_query)

    missing =
      required
      |> Enum.reject(fn key -> Map.has_key?(params, key) end)

    if Enum.empty?(missing) do
      {:ok, params}
    else
      {:error, {:missing_fields, missing}}
    end
  end

  defp create_job(config, cluster_name) do
    target_table = config["target_table"] || "#{config["name"]}_data"
    primary_key = config["primary_key"] || "id"

    distributed_table =
      if cluster_name, do: config["distributed_table"] || "#{config["name"]}_dist", else: nil

    attrs = %{
      name: config["name"],
      source_table: config["source_table"],
      target_table: target_table,
      distributed_table: distributed_table,
      primary_key: primary_key,
      cluster_name: cluster_name,
      status: "pending",
      config: Map.merge(config, %{"target_table" => target_table})
    }

    %MVJob{}
    |> MVJob.changeset(attrs)
    |> ObanRepo.insert()
  end

  defp get_max_pk(source_table, primary_key) do
    sql = SQLBuilder.get_max_pk_query(source_table, primary_key)

    case ClickhouseMaster.select(sql) do
      {:ok, %{rows: [[max_pk]]}} when not is_nil(max_pk) ->
        {:ok, max_pk}

      {:ok, %{rows: [[nil]]}} ->
        {:ok, 0}

      {:ok, %{rows: []}} ->
        {:ok, 0}

      {:error, reason} ->
        {:error, {:get_max_pk_failed, reason}}
    end
  end

  defp get_row_count(source_table, primary_key, cutoff_value) do
    sql = SQLBuilder.get_row_count_query(source_table, primary_key, cutoff_value)

    case ClickhouseMaster.select(sql) do
      {:ok, %{rows: [[count]]}} -> {:ok, count}
      {:ok, %{rows: []}} -> {:ok, 0}
      {:error, reason} -> {:error, {:get_row_count_failed, reason}}
    end
  end

  defp get_batch_min_pk(source_table, primary_key, cutoff_value, last_processed_value, batch_size) do
    sql =
      SQLBuilder.get_batch_min_pk_query(
        source_table,
        primary_key,
        cutoff_value,
        last_processed_value,
        batch_size
      )

    case ClickhouseMaster.select(sql) do
      {:ok, %{rows: [[min_pk]]}} -> {:ok, min_pk}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, reason} -> {:error, {:get_batch_min_pk_failed, reason}}
    end
  end

  defp execute_backfill_batch(config, primary_key, cutoff_value, last_processed_value, batch_size) do
    sql =
      SQLBuilder.backfill_batch_query(
        config,
        primary_key,
        cutoff_value,
        last_processed_value,
        batch_size
      )

    case ClickhouseMaster.query(sql) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, {:backfill_batch_failed, reason}}
    end
  end
end
