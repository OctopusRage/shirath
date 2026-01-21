defmodule Shirath.ObanRepo do
  use Ecto.Repo,
    otp_app: :shirath,
    adapter: Ecto.Adapters.Postgres
end
