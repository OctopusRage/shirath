defmodule Shirath.Controllers.MVController do
  @moduledoc """
  HTTP controller for materialized view operations.
  """

  import Plug.Conn
  alias Shirath.MV.{MVManager, MVJob}

  @doc """
  POST /api/mv
  Body: {
    "name": "products_by_category",
    "source_table": "products",
    "engine": "SummingMergeTree()",
    "order_by": ["category"],
    "columns": [
      {"name": "category", "type": "String"},
      {"name": "cnt", "type": "UInt64"}
    ],
    "select_query": "SELECT category, count() as cnt FROM {source_table} GROUP BY category",
    "cluster": "auto"  // optional: "auto", null, or cluster name
  }

  Creates a new materialized view with two-phase backfill.
  """
  def create(conn) do
    case MVManager.create(conn.body_params) do
      {:ok, job} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          201,
          Jason.encode!(%{
            message: "Materialized view creation started",
            job: format_job(job)
          })
        )

      {:error, {:missing_fields, fields}} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          400,
          Jason.encode!(%{
            error: "Missing required fields",
            missing: fields
          })
        )

      {:error, {:cluster_not_found, cluster_name}} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          400,
          Jason.encode!(%{
            error: "Cluster not found",
            cluster: cluster_name
          })
        )

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          500,
          Jason.encode!(%{
            error: "Failed to create materialized view",
            reason: inspect(reason)
          })
        )
    end
  end

  @doc """
  GET /api/mv
  Lists all materialized view jobs.

  Query params:
    - status: Filter by status (pending, creating, backfilling, optimizing, completed, failed, paused)
  """
  def index(conn) do
    status = conn.query_params["status"]
    opts = if status, do: [status: status], else: []

    jobs = MVManager.list_jobs(opts)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(%{jobs: Enum.map(jobs, &format_job/1)}))
  end

  @doc """
  GET /api/mv/:name
  Gets a specific materialized view job by name.
  """
  def show(conn, name) do
    case MVManager.get_by_name(name) do
      {:ok, job} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{job: format_job(job)}))

      {:error, :not_found} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Materialized view not found"}))
    end
  end

  @doc """
  POST /api/mv/:name/pause
  Pauses a running MV job.
  """
  def pause(conn, name) do
    with {:ok, job} <- MVManager.get_by_name(name),
         {:ok, job} <- MVManager.pause(job.id) do
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, Jason.encode!(%{message: "Job paused", job: format_job(job)}))
    else
      {:error, :not_found} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Materialized view not found"}))

      {:error, :cannot_pause} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Cannot pause job in current status"}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(500, Jason.encode!(%{error: inspect(reason)}))
    end
  end

  @doc """
  POST /api/mv/:name/resume
  Resumes a paused MV job.
  """
  def resume(conn, name) do
    with {:ok, job} <- MVManager.get_by_name(name),
         {:ok, job} <- MVManager.resume(job.id) do
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, Jason.encode!(%{message: "Job resumed", job: format_job(job)}))
    else
      {:error, :not_found} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Materialized view not found"}))

      {:error, :not_paused} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Job is not paused"}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(500, Jason.encode!(%{error: inspect(reason)}))
    end
  end

  @doc """
  DELETE /api/mv/:name
  Drops a materialized view and its associated tables.
  """
  def delete(conn, name) do
    case MVManager.drop(name) do
      :ok ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{message: "Materialized view deleted", name: name}))

      {:error, :not_found} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Materialized view not found"}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(500, Jason.encode!(%{error: inspect(reason)}))
    end
  end

  # Private functions

  defp format_job(job) do
    %{
      id: job.id,
      name: job.name,
      source_table: job.source_table,
      target_table: job.target_table,
      distributed_table: job.distributed_table,
      status: job.status,
      cluster_name: job.cluster_name,
      cutoff_id: job.cutoff_id,
      total_rows: job.total_rows,
      processed_rows: job.processed_rows,
      progress_percent: MVJob.progress_percent(job),
      error_message: job.error_message,
      inserted_at: job.inserted_at,
      updated_at: job.updated_at
    }
  end
end
