defmodule Shirath.Backfill do
  @moduledoc """
  Context module for managing backfill operations.

  Provides functions to:
  - Create and manage backfill jobs
  - Detect primary keys from PostgreSQL
  - Query data in batches for backfilling
  - Track progress persistently
  """

  import Ecto.Query
  alias Shirath.ObanRepo
  alias Shirath.Repo
  alias Shirath.Backfill.BackfillJob
  alias Shirath.MapConfig
  alias Workers.BackfillWorker

  require Logger

  @default_batch_size 5000

  # --------------------------------------------------------------------------
  # Public API
  # --------------------------------------------------------------------------

  @doc """
  Starts backfill for the given tables.
  Returns {:ok, jobs} or {:error, reason}.
  """
  def start(tables, opts \\ []) when is_list(tables) do
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    results =
      tables
      |> Enum.map(fn table ->
        case create_or_resume_job(table, batch_size) do
          {:ok, job} ->
            enqueue_worker(job)
            {:ok, job}

          {:error, reason} ->
            {:error, {table, reason}}
        end
      end)

    errors = Enum.filter(results, &match?({:error, _}, &1))

    if Enum.empty?(errors) do
      jobs = Enum.map(results, fn {:ok, job} -> job end)
      {:ok, jobs}
    else
      {:error, errors}
    end
  end

  @doc """
  Starts backfill for all tables defined in mapper.json.
  """
  def start_all(opts \\ []) do
    tables =
      MapConfig.load()
      |> Enum.map(& &1.source_table)
      |> Enum.uniq()

    start(tables, opts)
  end

  @doc """
  Gets a backfill job by ID.
  """
  def get_job(id), do: ObanRepo.get(BackfillJob, id)

  @doc """
  Gets a backfill job by source table that is not completed.
  """
  def get_active_job(source_table) do
    BackfillJob
    |> where(
      [j],
      j.source_table == ^source_table and j.status in ["pending", "running", "paused"]
    )
    |> order_by([j], desc: j.inserted_at)
    |> limit(1)
    |> ObanRepo.one()
  end

  @doc """
  Lists all backfill jobs, optionally filtered by status.
  """
  def list_jobs(opts \\ []) do
    status = Keyword.get(opts, :status)

    BackfillJob
    |> maybe_filter_status(status)
    |> order_by([j], desc: j.inserted_at)
    |> ObanRepo.all()
  end

  @doc """
  Pauses a running backfill job.
  """
  def pause_job(id) do
    case get_job(id) do
      nil -> {:error, :not_found}
      job -> update_job(job, %{status: "paused"})
    end
  end

  @doc """
  Resumes a paused backfill job.
  """
  def resume_job(id) do
    case get_job(id) do
      nil ->
        {:error, :not_found}

      %{status: "paused"} = job ->
        with {:ok, job} <- update_job(job, %{status: "running"}) do
          enqueue_worker(job)
          {:ok, job}
        end

      _ ->
        {:error, :not_paused}
    end
  end

  @doc """
  Detects the primary key column for a given table.
  Returns {:ok, column_name} or {:error, reason}.
  """
  def detect_primary_key(table) do
    # Sanitize table name to prevent SQL injection
    if valid_identifier?(table) do
      query = """
      SELECT a.attname
      FROM pg_index i
      JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
      WHERE i.indrelid = '#{table}'::regclass
      AND i.indisprimary
      ORDER BY array_position(i.indkey, a.attnum)
      LIMIT 1
      """

      case Repo.query(query, []) do
        {:ok, %{rows: [[column]]}} ->
          {:ok, column}

        {:ok, %{rows: []}} ->
          {:error, :no_primary_key}

        {:error, error} ->
          {:error, error}
      end
    else
      {:error, :invalid_table_name}
    end
  end

  @doc """
  Gets the total row count for a table (estimate for large tables).
  """
  def get_row_count(table) do
    if valid_identifier?(table) do
      # Use estimate for large tables, exact count for small ones
      query = """
      SELECT reltuples::bigint AS estimate
      FROM pg_class
      WHERE relname = $1
      """

      case Repo.query(query, [table]) do
        {:ok, %{rows: [[count]]}} when count > 0 ->
          {:ok, count}

        _ ->
          # Fallback to exact count
          case Repo.query("SELECT COUNT(*) FROM #{table}", []) do
            {:ok, %{rows: [[count]]}} -> {:ok, count}
            {:error, error} -> {:error, error}
          end
      end
    else
      {:error, :invalid_table_name}
    end
  end

  @doc """
  Gets the maximum primary key value for a table.
  """
  def get_max_id(table, primary_key) do
    if valid_identifier?(table) and valid_identifier?(primary_key) do
      case Repo.query("SELECT MAX(#{primary_key}) FROM #{table}", []) do
        {:ok, %{rows: [[nil]]}} -> {:ok, nil}
        {:ok, %{rows: [[max_id]]}} -> {:ok, max_id}
        {:error, error} -> {:error, error}
      end
    else
      {:error, :invalid_identifier}
    end
  end

  @doc """
  Fetches a batch of rows for backfilling.
  Returns rows in DESC order by primary key.
  """
  def fetch_batch(table, primary_key, last_id, batch_size) do
    if valid_identifier?(table) and valid_identifier?(primary_key) do
      query =
        if is_nil(last_id) do
          "SELECT * FROM #{table} ORDER BY #{primary_key} DESC LIMIT $1"
        else
          "SELECT * FROM #{table} WHERE #{primary_key} < $1 ORDER BY #{primary_key} DESC LIMIT $2"
        end

      params = if is_nil(last_id), do: [batch_size], else: [last_id, batch_size]

      case Repo.query(query, params) do
        {:ok, result} ->
          rows = Shirath.RawQueryMapper.get(result)
          {:ok, rows}

        {:error, error} ->
          {:error, error}
      end
    else
      {:error, :invalid_identifier}
    end
  end

  @doc """
  Updates a backfill job with new attributes.
  """
  def update_job(%BackfillJob{} = job, attrs) do
    job
    |> BackfillJob.changeset(attrs)
    |> ObanRepo.update()
  end

  @doc """
  Marks a job as completed.
  """
  def complete_job(%BackfillJob{} = job) do
    update_job(job, %{status: "completed"})
  end

  @doc """
  Marks a job as failed with an error message.
  """
  def fail_job(%BackfillJob{} = job, error_message) do
    update_job(job, %{status: "failed", error_message: error_message})
  end

  # --------------------------------------------------------------------------
  # Private Functions
  # --------------------------------------------------------------------------

  defp create_or_resume_job(table, batch_size) do
    # Check if there's an existing active job for this table
    case get_active_job(table) do
      %BackfillJob{status: "paused"} = job ->
        # Resume paused job
        update_job(job, %{status: "pending"})

      %BackfillJob{} = job ->
        # Already running or pending
        {:ok, job}

      nil ->
        # Create new job
        create_job(table, batch_size)
    end
  end

  defp create_job(table, batch_size) do
    # Get mapping config for this table
    case MapConfig.get_by_source_tbl(table) do
      [] ->
        {:error, :table_not_in_config}

      [config | _] ->
        dest_table = config.dest_table

        with {:ok, primary_key} <- detect_primary_key(table),
             {:ok, total_rows} <- get_row_count(table),
             {:ok, max_id} <- get_max_id(table, primary_key) do
          attrs = %{
            source_table: table,
            dest_table: dest_table,
            primary_key: primary_key,
            total_rows: total_rows,
            batch_size: batch_size,
            last_processed_id: if(max_id, do: max_id + 1, else: nil),
            status: "pending"
          }

          %BackfillJob{}
          |> BackfillJob.changeset(attrs)
          |> ObanRepo.insert()
        end
    end
  end

  defp enqueue_worker(%BackfillJob{id: id}) do
    %{backfill_job_id: id}
    |> BackfillWorker.new()
    |> Oban.insert()
  end

  defp maybe_filter_status(query, nil), do: query

  defp maybe_filter_status(query, status) when is_binary(status) do
    where(query, [j], j.status == ^status)
  end

  defp maybe_filter_status(query, statuses) when is_list(statuses) do
    where(query, [j], j.status in ^statuses)
  end

  # Validates that a string is a safe SQL identifier (table/column name)
  defp valid_identifier?(name) when is_binary(name) do
    Regex.match?(~r/^[a-zA-Z_][a-zA-Z0-9_]*$/, name)
  end

  defp valid_identifier?(_), do: false
end
