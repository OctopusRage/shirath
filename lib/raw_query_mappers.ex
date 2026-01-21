defmodule Shirath.RawQueryMapper do
  alias Shirath.Helper
  def get({:ok, result}), do: get(result)
  # def get([] = results), do: Enum.map(results, fn result -> get(result) end)
  def get(result) do
    cond do
      result.num_rows > 0 ->
        r =
          Enum.map(result.rows, fn row ->
            Enum.reduce(row, %{}, fn col, acc ->
              case Map.get(acc, :col_count) do
                nil ->
                  col_count = 0
                  col = if is_list(col) || is_map(col), do: Helper.atomize_keys(col), else: col

                  col =
                    if is_naive_date(col) do
                      col |> NaiveDateTime.truncate(:second) |> NaiveDateTime.to_string()
                    else
                      col
                    end

                  %{col_count: col_count, "#{Enum.at(result.columns, col_count)}": col}

                _ ->
                  col_count = Map.get(acc, :col_count) + 1
                  col = if is_list(col) || is_map(col), do: Helper.atomize_keys(col), else: col

                  col =
                    if is_naive_date(col) do
                      col |> NaiveDateTime.truncate(:second) |> NaiveDateTime.to_string()
                    else
                      col
                    end

                  Map.merge(acc, %{
                    col_count: col_count,
                    "#{Enum.at(result.columns, col_count)}": col
                  })
              end
            end)
            |> Map.delete(:col_count)
          end)

        r

      true ->
        []
    end
  end

  def is_naive_date(%NaiveDateTime{}), do: true
  def is_naive_date(_), do: false
end
