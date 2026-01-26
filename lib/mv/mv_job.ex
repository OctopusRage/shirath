defmodule Shirath.MV.MVJob do
  @moduledoc """
  Schema for tracking materialized view creation and backfill jobs.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @statuses ~w(pending creating backfilling optimizing completed failed paused)

  schema "mv_jobs" do
    field(:name, :string)
    field(:source_table, :string)
    field(:target_table, :string)
    field(:distributed_table, :string)
    field(:primary_key, :string, default: "id")

    field(:status, :string, default: "pending")

    # Cluster info
    field(:cluster_name, :string)
    field(:nodes, {:array, :string}, default: [])

    # Backfill tracking
    field(:cutoff_id, :integer)
    field(:total_rows, :integer, default: 0)
    field(:processed_rows, :integer, default: 0)
    field(:last_processed_id, :integer)

    # Full config stored as JSON
    field(:config, :map)

    field(:error_message, :string)

    timestamps()
  end

  @required_fields ~w(name source_table target_table config)a
  @optional_fields ~w(distributed_table primary_key status cluster_name nodes cutoff_id total_rows processed_rows last_processed_id error_message)a

  def changeset(mv_job, attrs) do
    mv_job
    |> cast(attrs, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> validate_inclusion(:status, @statuses)
    |> unique_constraint(:name)
  end

  def status_changeset(mv_job, status, attrs \\ %{}) do
    mv_job
    |> cast(Map.put(attrs, :status, status), [
      :status,
      :error_message,
      :processed_rows,
      :last_processed_id
    ])
    |> validate_inclusion(:status, @statuses)
  end

  def progress_changeset(mv_job, attrs) do
    mv_job
    |> cast(attrs, [:processed_rows, :last_processed_id])
  end

  def backfill_setup_changeset(mv_job, attrs) do
    mv_job
    |> cast(attrs, [:cutoff_id, :total_rows, :status])
  end

  @doc """
  Calculate progress percentage.
  """
  def progress_percent(%__MODULE__{total_rows: 0}), do: 0.0
  def progress_percent(%__MODULE__{total_rows: nil}), do: 0.0

  def progress_percent(%__MODULE__{total_rows: total, processed_rows: processed}) do
    Float.round(processed / total * 100, 2)
  end
end
