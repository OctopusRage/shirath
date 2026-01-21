defmodule Mix.Tasks.Shirath.Backfill do
  @moduledoc """
  Backfills existing PostgreSQL data to ClickHouse.

  ## Usage

      # Backfill specific tables
      mix shirath.backfill users orders products

      # Backfill all tables defined in mapper.json
      mix shirath.backfill --all

      # List current backfill jobs
      mix shirath.backfill --list

      # Resume a paused job by ID
      mix shirath.backfill --resume 123

      # Pause a running job by ID
      mix shirath.backfill --pause 123

  ## Options

      --all           Backfill all tables in mapper.json
      --list          List all backfill jobs and their status
      --resume ID     Resume a paused backfill job
      --pause ID      Pause a running backfill job
      --batch-size N  Batch size for processing (default: 5000)

  """

  use Mix.Task

  alias Shirath.Backfill
  alias Shirath.MapConfig

  @shortdoc "Backfills existing PostgreSQL data to ClickHouse"

  @switches [
    all: :boolean,
    list: :boolean,
    resume: :integer,
    pause: :integer,
    batch_size: :integer
  ]

  @impl Mix.Task
  def run(args) do
    # Start the application
    Mix.Task.run("app.start")

    {opts, tables, _} = OptionParser.parse(args, switches: @switches)

    cond do
      opts[:list] ->
        list_jobs()

      opts[:resume] ->
        resume_job(opts[:resume])

      opts[:pause] ->
        pause_job(opts[:pause])

      opts[:all] ->
        backfill_all(opts)

      length(tables) > 0 ->
        backfill_tables(tables, opts)

      true ->
        print_usage()
    end
  end

  defp backfill_tables(tables, opts) do
    batch_size = opts[:batch_size] || 5000

    # Validate tables exist in config
    config_tables = MapConfig.get_tables()

    invalid_tables = Enum.filter(tables, &(&1 not in config_tables))

    if length(invalid_tables) > 0 do
      Mix.shell().error(
        "Error: Tables not found in mapper.json: #{Enum.join(invalid_tables, ", ")}"
      )

      Mix.shell().info("\nAvailable tables:")
      Enum.each(config_tables, &Mix.shell().info("  - #{&1}"))
      exit({:shutdown, 1})
    end

    Mix.shell().info("Starting backfill for #{length(tables)} table(s)...")
    Mix.shell().info("Batch size: #{batch_size}")
    Mix.shell().info("")

    case Backfill.start(tables, batch_size: batch_size) do
      {:ok, jobs} ->
        Mix.shell().info("Backfill jobs created successfully!\n")
        print_jobs_table(jobs)
        Mix.shell().info("\nJobs are now running in the background via Oban.")
        Mix.shell().info("Use `mix shirath.backfill --list` to check progress.")

      {:error, errors} ->
        Mix.shell().error("Failed to create some backfill jobs:")

        Enum.each(errors, fn {:error, {table, reason}} ->
          Mix.shell().error("  - #{table}: #{inspect(reason)}")
        end)

        exit({:shutdown, 1})
    end
  end

  defp backfill_all(opts) do
    tables = MapConfig.get_tables() |> Enum.uniq()
    Mix.shell().info("Backfilling all #{length(tables)} table(s) from mapper.json...")
    backfill_tables(tables, opts)
  end

  defp list_jobs do
    jobs = Backfill.list_jobs()

    if length(jobs) == 0 do
      Mix.shell().info("No backfill jobs found.")
    else
      Mix.shell().info("Backfill Jobs:\n")
      print_jobs_table(jobs)
    end
  end

  defp resume_job(id) do
    case Backfill.resume_job(id) do
      {:ok, job} ->
        Mix.shell().info("Resumed backfill job #{id} for table '#{job.source_table}'")

      {:error, :not_found} ->
        Mix.shell().error("Job #{id} not found")
        exit({:shutdown, 1})

      {:error, :not_paused} ->
        Mix.shell().error("Job #{id} is not paused")
        exit({:shutdown, 1})
    end
  end

  defp pause_job(id) do
    case Backfill.pause_job(id) do
      {:ok, job} ->
        Mix.shell().info("Paused backfill job #{id} for table '#{job.source_table}'")

      {:error, :not_found} ->
        Mix.shell().error("Job #{id} not found")
        exit({:shutdown, 1})
    end
  end

  defp print_jobs_table(jobs) do
    header = "| ID | Source Table | Dest Table | Status | Progress | Last ID |"
    separator = "|" <> String.duplicate("-", String.length(header) - 2) <> "|"

    Mix.shell().info(separator)
    Mix.shell().info(header)
    Mix.shell().info(separator)

    Enum.each(jobs, fn job ->
      progress =
        if job.total_rows && job.total_rows > 0 do
          pct = Float.round(job.processed_rows / job.total_rows * 100, 1)
          "#{job.processed_rows}/#{job.total_rows} (#{pct}%)"
        else
          "#{job.processed_rows || 0}"
        end

      status_display = format_status(job.status)
      last_id = job.last_processed_id || "-"

      row =
        "| #{pad(job.id, 4)} | #{pad(job.source_table, 20)} | #{pad(job.dest_table, 15)} | #{pad(status_display, 10)} | #{pad(progress, 20)} | #{pad(last_id, 12)} |"

      Mix.shell().info(row)
    end)

    Mix.shell().info(separator)
  end

  defp format_status("completed"), do: "completed"
  defp format_status("running"), do: "running"
  defp format_status("pending"), do: "pending"
  defp format_status("paused"), do: "paused"
  defp format_status("failed"), do: "FAILED"
  defp format_status(other), do: other

  defp pad(value, width) do
    str = to_string(value)
    String.pad_trailing(str, width)
  end

  defp print_usage do
    Mix.shell().info(@moduledoc)
  end
end
