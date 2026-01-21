import Config

config :logger,
  level: :error,
  handle_otp_reports: true,
  handle_sasl_reports: true

config :shirath, ClickhouseMaster,
  connection_strings: "http://username:password@localhost:8123/analytics"

config :shirath, ClickhouseSlave,
  connection_strings: "http://username:password@localhost:8123/analytics"

config :shirath, Shirath.Repo,
  username: "postgres",
  password: "postgres",
  # your source database name
  database: "eshop",
  hostname: "localhost",
  port: 5432,
  pool_size: 10,
  timeout: 10_000_000

config :shirath, Shirath.ObanRepo,
  username: "postgres",
  password: "postgres",
  database: "shirath",
  hostname: "localhost",
  pool_size: 10,
  timeout: 10_000_000

config :shirath, :cdc,
  slot: "eshop_slot",
  publications: ["eshop_pub"]
