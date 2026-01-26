defmodule Shirath.Workers.MVOptimizeWorker do
  @moduledoc """
  Oban worker for running OPTIMIZE TABLE FINAL on the MV target table.

  This is the final step after backfill completes, merging all data parts.
  """
  use Oban.Worker,
    queue: :mat_view,
    max_attempts: 3,
    priority: 2

  alias Shirath.MV.MVManager

  require Logger

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"mv_job_id" => job_id}}) do
    Logger.info("MVOptimizeWorker: Starting optimize for job #{job_id}")

    with {:ok, job} <- MVManager.get_job(job_id),
         {:ok, _} <- MVManager.update_status(job_id, "optimizing"),
         {:ok, _} <- MVManager.optimize(job),
         {:ok, _} <- MVManager.mark_completed(job_id) do
      Logger.info("MVOptimizeWorker: Job #{job_id} completed successfully")
      :ok
    else
      {:error, :not_found} ->
        Logger.error("MVOptimizeWorker: Job #{job_id} not found")
        {:error, :not_found}

      {:error, reason} ->
        Logger.error("MVOptimizeWorker: Failed for job #{job_id}: #{inspect(reason)}")
        MVManager.mark_failed(job_id, reason)
        {:error, reason}
    end
  end
end
