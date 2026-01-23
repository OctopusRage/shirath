defmodule Workers.BackfillWorker do
  @moduledoc """
  Oban worker for processing backfill jobs.

  Each job processes one batch of rows and re-enqueues itself
  if there are more rows to process. This approach:
  - Allows Oban to handle retries on failure
  - Provides natural backpressure
  - Makes progress visible in Oban dashboard
  """

  use Oban.Worker,
    queue: :backfill,
    max_attempts: 5,
    priority: 3

  alias Shirath.Backfill
  alias Shirath.Backfill.BackfillJob
  alias Shirath.Ingestor

  require Logger

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"backfill_job_id" => job_id}}) do
    case Backfill.get_job(job_id) do
      nil ->
        Logger.error("[Backfill] Job #{job_id} not found")
        {:error, :job_not_found}

      %BackfillJob{status: "paused"} ->
        Logger.info("[Backfill] Job #{job_id} is paused, skipping")
        :ok

      %BackfillJob{status: "completed"} ->
        Logger.info("[Backfill] Job #{job_id} already completed")
        :ok

      %BackfillJob{status: "failed"} ->
        Logger.info("[Backfill] Job #{job_id} is marked as failed, skipping")
        :ok

      %BackfillJob{} = job ->
        process_batch(job)
    end
  end

  defp process_batch(%BackfillJob{} = job) do
    %{
      id: job_id,
      source_table: table,
      ordering_column: ordering_col,
      last_processed_id: last_value,
      batch_size: batch_size,
      processed_rows: processed_rows
    } = job

    # Use ordering_column for pagination, fallback to primary_key for backwards compatibility
    order_col = ordering_col || job.primary_key || "id"

    # Mark as running
    {:ok, job} = Backfill.update_job(job, %{status: "running"})

    Logger.info(
      "[Backfill] Processing #{table} | order_by: #{order_col} DESC | last_value: #{last_value} | processed: #{processed_rows}"
    )

    case Backfill.fetch_batch(table, order_col, last_value, batch_size) do
      {:ok, []} ->
        # No more rows, mark as completed
        Logger.info("[Backfill] Completed #{table} | total processed: #{processed_rows}")
        Backfill.complete_job(job)
        :ok

      {:ok, rows} ->
        # Push to ingestor
        Ingestor.push_messages(table, rows)

        # Calculate new last_processed_id (minimum value in this batch since DESC order)
        new_last_value =
          rows
          |> Enum.map(&Map.get(&1, String.to_atom(order_col)))
          |> Enum.min()

        new_processed = processed_rows + length(rows)

        # Update job progress
        {:ok, updated_job} =
          Backfill.update_job(job, %{
            last_processed_id: new_last_value,
            processed_rows: new_processed
          })

        Logger.info(
          "[Backfill] #{table} | batch: #{length(rows)} | new_last_value: #{new_last_value} | total: #{new_processed}"
        )

        # Enqueue next batch
        if length(rows) == batch_size do
          # There might be more rows
          %{backfill_job_id: job_id}
          |> __MODULE__.new()
          |> Oban.insert()
        else
          # This was the last batch
          Logger.info("[Backfill] Completed #{table} | total processed: #{new_processed}")
          Backfill.complete_job(updated_job)
        end

        :ok

      {:error, reason} ->
        error_msg = inspect(reason)
        Logger.error("[Backfill] Error fetching batch for #{table}: #{error_msg}")
        Backfill.fail_job(job, error_msg)
        {:error, reason}
    end
  end
end
