# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

config :shirath, Oban,
  engine: Oban.Engines.Basic,
  queues: [default: 1, mat_view: 1, file_flusher: 1, backfill: 1],
  plugins: [{Oban.Plugins.Pruner, max_age: 86400}],
  repo: Shirath.ObanRepo

config :shirath,
  env: Mix.env(),
  ecto_repos: [Shirath.Repo]

config :shirath, Shirath.Scheduler, jobs: []

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

config :sentry,
  dsn: "https://public:secret@app.getsentry.com/1",
  included_environments: [:prod],
  environment_name: config_env()

config :shirath, Shirath.Scheduler,
  jobs: [
    # Every minute
    {"* * * * *", {Shirath.Heartbeat, :send, []}}
  ]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"
