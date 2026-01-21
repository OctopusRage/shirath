defmodule Shirath.Clickhouse do
  def insert_to_table(table_name, record) when is_map(record) do
    converted_value = convert_values_to_clickhouse_for_json_insert(record)

    generate_json_insert_query(table_name, List.wrap(converted_value))
  end

  def insert_to_table(table_name, records) when is_list(records) do
    converted_values = Enum.map(records, &convert_values_to_clickhouse_for_json_insert/1)

    generate_json_insert_query(table_name, converted_values)
  end

  defp generate_json_insert_query(table_name, records) do
    sql_strings = [
      "INSERT INTO",
      table_name,
      "FORMAT JSONEachRow",
      Enum.join(Enum.map(records, &Jason.encode!/1), " ")
    ]

    Enum.join(sql_strings, "\n")
  end

  defp convert_values_to_clickhouse_for_json_insert(map) do
    map
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Enum.map(fn {key, value} ->
      {key, convert(value)}
    end)
    |> Map.new()
  end

  def convert(param) when is_list(param) do
    Enum.map(param, &convert/1)
  end

  def convert(param) when is_integer(param) do
    Integer.to_string(param)
  end

  def convert(true), do: 1
  def convert(false), do: 0

  def convert(nil), do: nil

  def convert(param) when is_atom(param) do
    Atom.to_string(param)
  end

  def convert(param) when is_float(param) do
    Float.to_string(param)
  end

  def convert(%DateTime{} = datetime) do
    datetime
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
    |> String.replace("Z", "")
  end

  def convert(%Date{} = date) do
    date
    |> Date.to_iso8601()
  end

  def convert(%Decimal{} = decimal) do
    Decimal.to_float(decimal)
  end

  def convert(param) when is_map(param) do
    json = Jason.encode!(param)
    convert(json)
  end

  def convert(param) when is_binary(param) do
    param
  end

  def convert(param) do
    param
  end
end
