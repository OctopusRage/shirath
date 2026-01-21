defmodule Shirath.Controllers.BackfillController do
  @moduledoc """
  HTTP controller for backfill operations.
  """

  import Plug.Conn
  alias Shirath.Backfill
  alias Shirath.MapConfig

  @doc """
  POST /api/backfill
  Body: { "tables": ["users", "orders"] }
  Or:   { "all": true }

  Starts backfill jobs for the specified tables.
  """
  def create(conn) do
    case conn.body_params do
      %{"all" => true} ->
        start_all_backfill(conn)

      %{"tables" => tables} when is_list(tables) ->
        start_backfill(conn, tables)

      _ ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          400,
          Jason.encode!(%{error: "Invalid request. Provide 'tables' array or 'all': true"})
        )
    end
  end

  @doc """
  GET /api/backfill
  Lists all backfill jobs.

  Query params:
    - status: Filter by status (pending, running, completed, failed, paused)
  """
  def index(conn) do
    status = conn.query_params["status"]
    opts = if status, do: [status: status], else: []

    jobs = Backfill.list_jobs(opts)

    response =
      Enum.map(jobs, fn job ->
        %{
          id: job.id,
          source_table: job.source_table,
          dest_table: job.dest_table,
          status: job.status,
          total_rows: job.total_rows,
          processed_rows: job.processed_rows,
          last_processed_id: job.last_processed_id,
          progress_percent: calculate_progress(job),
          error_message: job.error_message,
          inserted_at: job.inserted_at,
          updated_at: job.updated_at
        }
      end)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(%{jobs: response}))
  end

  @doc """
  GET /api/backfill/:id
  Gets a specific backfill job.
  """
  def show(conn, id) do
    case Backfill.get_job(id) do
      nil ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Job not found"}))

      job ->
        response = %{
          id: job.id,
          source_table: job.source_table,
          dest_table: job.dest_table,
          primary_key: job.primary_key,
          status: job.status,
          total_rows: job.total_rows,
          processed_rows: job.processed_rows,
          last_processed_id: job.last_processed_id,
          batch_size: job.batch_size,
          progress_percent: calculate_progress(job),
          error_message: job.error_message,
          inserted_at: job.inserted_at,
          updated_at: job.updated_at
        }

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{job: response}))
    end
  end

  @doc """
  POST /api/backfill/:id/pause
  Pauses a running backfill job.
  """
  def pause(conn, id) do
    case Backfill.pause_job(id) do
      {:ok, job} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{message: "Job paused", job_id: job.id}))

      {:error, :not_found} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Job not found"}))
    end
  end

  @doc """
  POST /api/backfill/:id/resume
  Resumes a paused backfill job.
  """
  def resume(conn, id) do
    case Backfill.resume_job(id) do
      {:ok, job} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{message: "Job resumed", job_id: job.id}))

      {:error, :not_found} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Job not found"}))

      {:error, :not_paused} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Job is not paused"}))
    end
  end

  # Private functions

  defp start_backfill(conn, tables) do
    # Validate tables exist in config
    config_tables = MapConfig.get_tables()
    invalid_tables = Enum.filter(tables, &(&1 not in config_tables))

    if length(invalid_tables) > 0 do
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(
        400,
        Jason.encode!(%{
          error: "Tables not found in mapper.json",
          invalid_tables: invalid_tables,
          available_tables: config_tables |> Enum.uniq()
        })
      )
    else
      case Backfill.start(tables) do
        {:ok, jobs} ->
          response =
            Enum.map(jobs, fn job ->
              %{id: job.id, source_table: job.source_table, status: job.status}
            end)

          conn
          |> put_resp_content_type("application/json")
          |> send_resp(201, Jason.encode!(%{message: "Backfill started", jobs: response}))

        {:error, errors} ->
          error_details =
            Enum.map(errors, fn {:error, {table, reason}} ->
              %{table: table, reason: inspect(reason)}
            end)

          conn
          |> put_resp_content_type("application/json")
          |> send_resp(
            500,
            Jason.encode!(%{error: "Failed to start backfill", details: error_details})
          )
      end
    end
  end

  defp start_all_backfill(conn) do
    tables = MapConfig.get_tables() |> Enum.uniq()
    start_backfill(conn, tables)
  end

  defp calculate_progress(%{total_rows: nil}), do: nil
  defp calculate_progress(%{total_rows: 0}), do: 100.0

  defp calculate_progress(%{total_rows: total, processed_rows: processed}) do
    Float.round(processed / total * 100, 2)
  end
end
