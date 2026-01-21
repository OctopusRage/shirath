defmodule Shirath.Ingestor do
  use Broadway
  alias Shirath.KV
  alias Shirath.Clickhouse
  alias Workers.FileBuffer
  alias Broadway.Message
  alias Shirath.MapConfig
  # alias Shirath.MapConfig
  require Logger

  defp get_batchers() do
    MapConfig.get_tables()
    |> Enum.uniq()
    |> Enum.map(fn e ->
      [
        "#{e}": [
          batch_size: 10000,
          batch_timeout: 5000,
          concurrency: 1
        ]
      ]
    end)
    |> List.flatten()
  end

  @moduledoc """
  This module is responsible for ingesting data into the system.
  It forces concurrency to 1 to ensure FIFO order.
  The processors concurrency is set to 10 because it will be partitioned by table name.
  """
  def start_link(_opts) do
    batchers = get_batchers()

    r =
      Broadway.start_link(__MODULE__,
        name: __MODULE__,
        producer: [
          module: {__MODULE__.Producer, []},
          transformer: {__MODULE__, :transform, []},
          concurrency: 1
        ],
        processors: [
          default: [
            concurrency: 10
          ]
        ],
        batchers: batchers,
        partition_by: &partition/1
      )

    IO.inspect(batchers)
    r
  end

  def transform(event, _opts) do
    %Message{
      data: event,
      acknowledger: {__MODULE__, :ack_id, :ack_data}
    }
  end

  defp partition(msg) do
    :erlang.phash2(msg.data.table)
  end

  def push_messages(table, records) when is_list(records) do
    try do
      messages =
        records |> Enum.map(fn record -> transform_e(table, record) end) |> List.flatten()

      IO.inspect({:push_messages, table, messages |> Enum.count()})
      Broadway.push_messages(__MODULE__, messages)
    rescue
      e ->
        Sentry.capture_exception(e, stacktrace: __STACKTRACE__)
        true
    end
  end

  def push_message(table, record) do
    try do
      messages = transform_e(table, record)
      Broadway.push_messages(__MODULE__, messages)
    rescue
      e ->
        Sentry.capture_exception(e, stacktrace: __STACKTRACE__)
        true
    end
  end

  def push_message(event) do
    try do
      messages = transform_e(event)
      if messages, do: Broadway.push_messages(__MODULE__, messages)
    rescue
      e ->
        Sentry.capture_exception(e, stacktrace: __STACKTRACE__)
        true
    end
  end

  def push_message_force(table, record) do
    try do
      messages = [
        %Message{
          data: %{dest_table: table, table: table, record: record},
          acknowledger: {__MODULE__, :ack_id, :ack_data}
        }
      ]

      Broadway.push_messages(__MODULE__, messages)
    rescue
      e ->
        Sentry.capture_exception(e, stacktrace: __STACKTRACE__)
        true
    end
  end

  def transform_e(%{operations: [%{table: table, record: record} | _]}) do
    MapConfig.get_by_source_tbl(table)
    |> Enum.map(fn map ->
      record = MapConfig.remap(map, record)

      %Message{
        data: %{dest_table: map.dest_table, table: table, record: record},
        acknowledger: {__MODULE__, :ack_id, :ack_data}
      }
    end)
  end

  def transform_e(_), do: nil

  def transform_e(table, record) do
    MapConfig.get_by_source_tbl(table)
    |> Enum.map(fn map ->
      record = MapConfig.remap(map, record)

      %Message{
        data: %{dest_table: map.dest_table, table: table, record: record},
        acknowledger: {__MODULE__, :ack_id, :ack_data}
      }
    end)
  end

  @impl true
  def handle_message(_, message, _) do
    {dest_table, batcher} =
      case message do
        %{data: %{dest_table: dest_table, table: table}} ->
          batcher =
            try do
              String.to_existing_atom(table)
            rescue
              _ ->
                String.to_atom(table)
            end

          {dest_table, batcher}

        _ ->
          {nil, nil}
      end

    message |> Message.put_batcher(batcher) |> Message.put_batch_key(dest_table)
  end

  @impl true
  def handle_batch(_, messages, %{batch_key: dest_table}, _) do
    dest_table = if is_atom(dest_table), do: Atom.to_string(dest_table), else: dest_table

    try do
      messages |> do_bulk_insert(dest_table)
      # messages
    rescue
      e ->
        Sentry.capture_exception(e, stacktrace: __STACKTRACE__)
        true
    end

    messages
  end

  def ack(:ack_id, _successful, _failed) do
    true
  end

  def ack(:ack_data, _successful, _failed) do
    true
  end

  defp write_local(tbl, contents) do
    IO.inspect("write local")
    file = "#{System.os_time()}_#{tbl}.json"
    path = "mc_data/#{file}"

    with :ok <- File.mkdir_p(Path.dirname(path)) do
      File.write(path, contents)
    end

    if !KV.get("mat_view_lock:#{tbl}") do
      queue = :default

      %{"file" => path, "table" => tbl}
      |> FileBuffer.new(queue: queue)
      |> Oban.insert()
    end
  end

  def do_bulk_insert(messages, tbl) do
    if tbl == "channels", do: IO.inspect({:bulk_tbl, tbl})

    records =
      messages
      |> Enum.map(fn msg ->
        msg.data.record |> convert_values_to_clickhouse_for_json_insert()
      end)

    write_local(tbl, records |> Jason.encode!())
  end

  defp convert_values_to_clickhouse_for_json_insert(map) do
    map
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Enum.map(fn {key, value} ->
      {key, Clickhouse.convert(value)}
    end)
    |> Map.new()
  end
end
