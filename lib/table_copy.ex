defmodule Shirath.TableCopy do
  alias Shirath.{Repo, RawQueryMapper, MapConfig, KV}
  # @truncated_max_id 100_000_000

  def run_async() do
    Task.start(fn ->
      run()
    end)
  end

  def run() do
    now = NaiveDateTime.utc_now()
    start_time = DateTime.utc_now() |> DateTime.to_unix()
    KV.put("initial_load", now)
    IO.inspect({:catching_up, {DateTime.utc_now()}})

    MapConfig.load()
    |> Enum.map(fn e ->
      table = e.source_table
      copy_table(table)
    end)

    end_time = DateTime.utc_now() |> DateTime.to_unix()
    IO.inspect({:finished_catching_up, end_time - start_time, {DateTime.utc_now()}})
  end

  def copy_table_async(table, batch \\ 10000, max_id \\ nil) do
    Task.start(fn ->
      copy_table(table, batch, max_id)
    end)
  end

  def copy_table(table, batch \\ 10000, max_id \\ nil) do
    try do
      # if is_nil(max_id), do: sync_status(table)
      KV.put("sync:#{table}", true)
      {max_id, low_id} = get_data_limit(table, max_id, batch)

      IO.inspect("copying table : #{table} | #{max_id} - #{low_id}")

      if max_id > 0 do
        process_bulk(table, low_id, max_id)
        copy_table(table, batch, low_id - 1)
      end
      KV.flush()
    rescue
      e ->
        KV.flush()
        Sentry.capture_exception(e, stacktrace: __STACKTRACE__)
        reraise "error", __STACKTRACE__
    end
  end

  defp process_bulk(table, min_id, max_id) do
    try do
      results =
        Repo.query!("select * from #{table} where id between #{min_id} and #{max_id}")
        |> RawQueryMapper.get()
        |> Enum.filter(fn e ->
          case KV.get("#{table}:#{e.id}") do
            nil -> true
            _ -> false
          end
        end)

      Task.start(fn ->
        Shirath.Ingestor.push_messages(table, results)
      end)
    rescue
      e ->
        Sentry.capture_exception(e, stacktrace: __STACKTRACE__)
        nil
    end
  end

  def get_data_limit(table, max_id \\ nil, batch \\ 10000) do
    max_id =
      if is_nil(max_id) do
        case Repo.query!("select max(id) from #{table}") do
          %{rows: [[id_max]]} ->
            id_max

          _ ->
            max_id
        end
      else
        max_id
      end

    low_id = if batch > max_id, do: 0, else: max_id - batch
    {max_id, low_id}
  end

  def test_updatefast do
    Enum.map(1..1_000_000, fn _ ->

      Repo.query!("UPDATE room_logs SET notes = 'ubah' where id = #{:rand.uniform(200_000_000)}")
    end)
  end

  def sync_table(table, ids) do
    ids = "(" <> Enum.join(ids, ",") <> ")"
    results = Repo.query!("select * from #{table} where id in #{ids}") |> RawQueryMapper.get() |> Enum.filter(fn e ->
      case KV.get("#{table}:#{e.id}") do
        nil -> true
        _ -> false
      end
    end)

    Task.start(fn ->
      Shirath.Ingestor.push_messages(table, results)
    end)
  end

  def test() do
    ids = 1..20000
    ids |> Enum.map(fn id ->
      id_string = "#{id}"
      q = "update room_logs set name = 'name||#{id_string}' where id = #{id}"
      Repo.query!(q)
    end)
  end
end
