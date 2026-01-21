defmodule Workers.FileBuffer do
  use Oban.Worker, queue: :default

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"table" => table, "file" => file} = _args}) do
    records = file |> File.read!() |> Jason.decode!(keys: :atoms)
    result = do_bulk_insert(records, table)
    with :ok <- result do
      File.rm!(file)
      :ok
    end
  end

  def do_bulk_insert(records, tbl) do
    sql = insert_to_table(tbl, records)

    case ClickhouseMaster.query(sql) do
      {:ok, _} ->
        rec_id =
          case records do
            [%{id: id} | _] -> id
            _ -> nil
          end

        if tbl == "user_channels", do: IO.inspect({:test_deleted_user_channels, sql})
        res = {:ok, tbl, rec_id, Enum.count(records), DateTime.now!("Etc/UTC")}
        IO.inspect(res)

        :ok

      {:error, %{reason: err}} ->
        id_range = get_id_ranges(records)

        Sentry.capture_message("Failed Inserting batch",
          extra: %{reason: inspect(err), table: tbl, id_range: id_range}
        )

        err = {:err, tbl, inspect(err)}
        IO.inspect({:error, err})
        {:error, "failed insert batch: #{inspect(err)}"}

      err ->
        id_range = get_id_ranges(records)

        Sentry.capture_message("Failed Inserting batch",
          extra: %{reason: inspect(err), table: tbl, id_range: id_range}
        )

        err = {:err, tbl, inspect(err)}
        IO.inspect({:error, err})
        {:error, "failed insert batch: #{inspect(err)}"}
    end
  end

  defp get_id_ranges(records) do
    case records do
      [%{id: id} | _x] ->
        res = Enum.map(records, fn e -> e.id end) |> Enum.join(",")
        "#{id}-#{List.last(records) |> Map.get(:id)}"
        res

      _ ->
        "-"
    end
  end

  def insert_to_table(table_name, record) when is_map(record) do
    generate_json_insert_query(table_name, List.wrap(record))
  end

  def insert_to_table(table_name, records) when is_list(records) do
    generate_json_insert_query(table_name, records)
  end

  defp generate_json_insert_query(table_name, records) do
    sql_strings = [
      "INSERT INTO",
      table_name,
      "SETTINGS async_insert=1, wait_for_async_insert=1",
      "FORMAT JSONEachRow",
      Enum.join(Enum.map(records, &Jason.encode!/1), " ")
    ]

    Enum.join(sql_strings, "\n")
  end
end
