defmodule Shirath.Helper do
  def generate_psql_ch_func(db, table, select_q \\ "SELECT *", clause \\ "") do
    q = """
    #{select_q}
    FROM postgresql('#{db.host}:#{db.port}', '#{db.database}', '#{table}', '#{db.username}', '#{db.password}')
    #{clause}
    """

    ClickhouseMaster.query(q)
  end

  @doc """
  Convert map string keys to :atom keys
  """
  def atomize_keys(nil), do: nil

  # Structs don't do enumerable and anyway the keys are already
  # atoms
  def atomize_keys(struct = %{__struct__: _}) do
    struct
  end

  def atomize_keys(map = %{}) do
    map
    |> Enum.map(fn
      {k, v} when is_atom(k) -> {k, atomize_keys(v)}
      {k, v} -> {String.to_atom(k), atomize_keys(v)}
    end)
    |> Enum.into(%{})
  end

  # Walk the list and atomize the keys of
  # of any map members
  def atomize_keys([head | rest]) do
    [atomize_keys(head) | atomize_keys(rest)]
  end

  def atomize_keys(not_a_map) do
    not_a_map
  end

  def truncate_ch_data() do
    {:ok, tables} = ClickhouseMaster.query("show tables")
    tables = tables |> String.split("\n")

    tables
    |> Enum.map(fn e ->
      ClickhouseMaster.query("truncate table #{e}")
    end)
  end


end
