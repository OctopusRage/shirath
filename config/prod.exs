import Config

config :logger,
  level: :error,
  handle_otp_reports: true,
  handle_sasl_reports: true

config :shirath, ClickhouseMaster, connection_strings: System.get_env("CH_CONNECTION")
config :shirath, ClickhouseSlave, connection_strings: System.get_env("CH_CONNECTION_SLAVE") || System.get_env("CH_CONNECTION")

config :shirath, Shirath.Repo,
  username: System.get_env("DB_USERNAME"),
  password: System.get_env("DB_PASSWORD"),
  database: System.get_env("DB_NAME"),
  hostname: System.get_env("DB_HOST"),
  pool_size: 10,
  show_sensitive_data_on_connection_error: true,
  log: false

config :shirath, Shirath.ObanRepo,
  username: System.get_env("DB_JOB_USERNAME"),
  password: System.get_env("DB_JOB_PASSWORD"),
  database: System.get_env("DB_JOB_NAME"),
  hostname: System.get_env("DB_JOB_HOST"),
  pool_size: 10,
  timeout: 86400 * 1000,
  show_sensitive_data_on_connection_error: true,
  log: false

config :shirath, :cdc,
  slot: System.get_env("CDC_SLOT"),
  publications: [System.get_env("CDC_PUBLICATION")]

config :sentry,
  dsn: System.get_env("SENTRY_DSN"),
  included_environments: [:prod],
  environment_name: :prod,
  validate_compile_env: false,
  tags: %{
    env: "production"
  }
