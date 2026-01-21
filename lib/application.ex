defmodule Shirath.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    # List all child processes to be supervised
    children = [
      {Bandit, plug: Shirath.Router, scheme: :http, port: 4000},
      {Shirath.KV, %{}},
      Shirath.Repo,
      Shirath.ObanRepo,
      Shirath.MapConfig,
      {Oban, Application.fetch_env!(:shirath, Oban)},
      ClickhouseMaster,
      ClickhouseSlave,
      {Shirath.Ingestor, []},
      Shirath.Scheduler,
      {Shirath.Seeder, []},
      cainophile(),
      Shirath.Events
    ]

    {:ok, _} = Logger.add_backend(Sentry.LoggerBackend)

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    # Shirath.State.start_link(false)
    opts = [strategy: :one_for_one, name: Shirath.Supervisor]
    r = Supervisor.start_link(children, opts)
    # Shirath.start()
    r
  end

  def cainophile do
    db_conf = Application.get_env(:shirath, Shirath.Repo) |> Enum.into(%{})
    cdc_conf = Application.get_env(:shirath, :cdc) |> Enum.into(%{})

    IO.inspect(db_conf)

    {
      Cainophile.Adapters.Postgres,

      # name this process will be registered globally as, for usage with Cainophile.Adapters.Postgres.subscribe/2
      # All epgsql options are supported here
      # :temporary is also supported if you don't want Postgres keeping track of what you've acknowledged
      # You can provide a different WAL position if desired, or default to allowing Postgres to send you what it thinks you need
      register: Shirath.Events,
      epgsql: %{
        host: ~c"#{db_conf.hostname}",
        username: db_conf.username,
        database: db_conf.database,
        password: db_conf.password
      },
      slot: cdc_conf.slot,
      wal_position: {"0", "0"},
      publications: cdc_conf.publications
    }
  end
end
