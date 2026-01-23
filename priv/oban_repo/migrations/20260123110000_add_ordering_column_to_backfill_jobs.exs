defmodule Shirath.ObanRepo.Migrations.AddOrderingColumnToBackfillJobs do
  use Ecto.Migration

  def change do
    alter table(:backfill_jobs) do
      add :ordering_column, :string, null: false, default: "id"
    end
  end
end
