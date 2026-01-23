defmodule Shirath.Workers.MVCreateWorker do
  @moduledoc """
  Oban worker for creating materialized view objects in ClickHouse.

  This worker:
  1. Sets up backfill tracking (gets cutoff_id BEFORE creating MV)
  2. Creates target table
  3. Creates materialized view
  4. Creates distributed table (if clustered)
  5. Enqueues backfill worker
  """
  use Oban.Worker,
    queue: :mat_view,
    max_attempts: 3,
    priority: 1

  alias Shirath.MV.MVManager
  alias Shirath.Workers.MVBackfillWorker

  require Logger

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"mv_job_id" => job_id}}) do
    Logger.info("MVCreateWorker: Starting creation for job #{job_id}")

    with {:ok, job} <- MVManager.get_job(job_id),
         :ok <- check_status(job),
         {:ok, _} <- MVManager.update_status(job_id, "creating"),
         # IMPORTANT: Setup backfill (get cutoff_id) BEFORE creating MV
         {:ok, job} <- MVManager.setup_backfill(job),
         {:ok, job} <- MVManager.create_target_table(job),
         {:ok, job} <- MVManager.create_materialized_view(job),
         {:ok, _job} <- MVManager.create_distributed_table(job) do
      Logger.info("MVCreateWorker: Created MV objects for job #{job_id}, starting backfill")

      # Enqueue backfill worker
      %{mv_job_id: job_id}
      |> MVBackfillWorker.new()
      |> Oban.insert()

      :ok
    else
      {:error, :not_found} ->
        Logger.error("MVCreateWorker: Job #{job_id} not found")
        {:error, :not_found}

      {:error, :invalid_status} ->
        Logger.warning("MVCreateWorker: Job #{job_id} has invalid status, skipping")
        :ok

      {:error, reason} ->
        Logger.error("MVCreateWorker: Failed for job #{job_id}: #{inspect(reason)}")
        MVManager.mark_failed(job_id, reason)
        {:error, reason}
    end
  end

  defp check_status(job) do
    if job.status in ["pending", "creating", "paused"] do
      :ok
    else
      {:error, :invalid_status}
    end
  end
end
