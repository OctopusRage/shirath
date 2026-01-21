defmodule Shirath.ObanRepo.Migrations.CreateBackfillJobs do
  use Ecto.Migration

  def change do
    create table(:backfill_jobs) do
      add :source_table, :string, null: false
      add :dest_table, :string, null: false
      add :primary_key, :string, null: false, default: "id"
      add :total_rows, :bigint
      add :processed_rows, :bigint, default: 0
      add :last_processed_id, :bigint
      add :batch_size, :integer, default: 5000
      add :status, :string, default: "pending"
      add :error_message, :text

      timestamps()
    end

    create index(:backfill_jobs, [:source_table])
    create index(:backfill_jobs, [:status])
  end
end
