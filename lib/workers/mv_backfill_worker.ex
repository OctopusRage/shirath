defmodule Shirath.Workers.MVBackfillWorker do
  @moduledoc """
  Oban worker for backfilling materialized view data.

  This worker processes data in batches:
  1. Fetches batch from source table (WHERE id <= cutoff_id)
  2. Inserts aggregated data into target table
  3. Updates progress
  4. Re-enqueues itself for next batch or enqueues optimize worker when done
  """
  use Oban.Worker,
    queue: :mat_view,
    max_attempts: 5,
    priority: 3

  alias Shirath.MV.MVManager
  alias Shirath.Workers.MVOptimizeWorker

  require Logger

  @batch_size 10_000

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"mv_job_id" => job_id}}) do
    Logger.info("MVBackfillWorker: Processing batch for job #{job_id}")

    with {:ok, job} <- MVManager.get_job(job_id),
         :ok <- check_status(job) do
      case MVManager.process_backfill_batch(job, @batch_size) do
        {:ok, :continue} ->
          Logger.info(
            "MVBackfillWorker: Batch complete for job #{job_id}, progress: #{job.processed_rows}/#{job.total_rows}"
          )

          # Re-enqueue for next batch
          %{mv_job_id: job_id}
          |> __MODULE__.new()
          |> Oban.insert()

          :ok

        {:ok, :complete} ->
          Logger.info("MVBackfillWorker: Backfill complete for job #{job_id}, starting optimize")

          # Enqueue optimize worker
          %{mv_job_id: job_id}
          |> MVOptimizeWorker.new()
          |> Oban.insert()

          :ok

        {:error, reason} ->
          Logger.error("MVBackfillWorker: Batch failed for job #{job_id}: #{inspect(reason)}")
          MVManager.mark_failed(job_id, reason)
          {:error, reason}
      end
    else
      {:error, :not_found} ->
        Logger.error("MVBackfillWorker: Job #{job_id} not found")
        {:error, :not_found}

      {:error, :invalid_status} ->
        Logger.warning("MVBackfillWorker: Job #{job_id} has invalid status, skipping")
        :ok

      {:error, reason} ->
        Logger.error("MVBackfillWorker: Failed for job #{job_id}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp check_status(job) do
    if job.status == "backfilling" do
      :ok
    else
      {:error, :invalid_status}
    end
  end
end
