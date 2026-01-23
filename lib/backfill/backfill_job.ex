defmodule Shirath.Backfill.BackfillJob do
  use Ecto.Schema
  import Ecto.Changeset

  schema "backfill_jobs" do
    field(:source_table, :string)
    field(:dest_table, :string)
    field(:primary_key, :string, default: "id")
    field(:ordering_column, :string, default: "id")
    field(:total_rows, :integer)
    field(:processed_rows, :integer, default: 0)
    field(:last_processed_id, :integer)
    field(:batch_size, :integer, default: 5000)
    field(:status, :string, default: "pending")
    field(:error_message, :string)

    timestamps()
  end

  @required_fields [:source_table, :dest_table]
  @optional_fields [
    :primary_key,
    :ordering_column,
    :total_rows,
    :processed_rows,
    :last_processed_id,
    :batch_size,
    :status,
    :error_message
  ]

  def changeset(backfill_job, attrs) do
    backfill_job
    |> cast(attrs, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> validate_inclusion(:status, ["pending", "running", "completed", "failed", "paused"])
  end
end
