defmodule Shirath.ObanRepo.Migrations.CreateMvJobs do
  use Ecto.Migration

  def change do
    create table(:mv_jobs) do
      add :name, :string, null: false
      add :source_table, :string, null: false
      add :target_table, :string, null: false
      add :distributed_table, :string
      add :primary_key, :string, null: false, default: "id"

      add :status, :string, null: false, default: "pending"
      # Status values: pending, creating, backfilling, optimizing, completed, failed, paused

      # Cluster info
      add :cluster_name, :string
      add :nodes, {:array, :string}, default: []

      # Backfill tracking
      add :cutoff_id, :bigint
      add :total_rows, :bigint, default: 0
      add :processed_rows, :bigint, default: 0
      add :last_processed_id, :bigint

      # Full config stored as JSON
      add :config, :map, null: false

      add :error_message, :text

      timestamps()
    end

    create unique_index(:mv_jobs, [:name])
    create index(:mv_jobs, [:status])
    create index(:mv_jobs, [:source_table])
  end
end
