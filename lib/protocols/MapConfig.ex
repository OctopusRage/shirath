defmodule Shirath.MapConfig do
  use GenServer

  def init(_state) do
    state = load_from_file()
    {:ok, state}
  end

  def start_link(_ \\ nil) do
    GenServer.start_link(__MODULE__, fn -> [] end, name: __MODULE__)
  end

  def handle_call(:load_conf, _from, state) do
    {:reply, state, state}
  end

  def load_from_cache do
    GenServer.call(__MODULE__, :load_conf)
  end

  def load_from_file do
    if System.get_env("MAPPER_FILE") do
      config_file = System.get_env("MAPPER_FILE") |> File.read!() |> Jason.decode!(keys: :atoms)
      config_file.data
    else
      config_file = (File.cwd!() <> "/mapper.json") |> File.read!() |> Jason.decode!(keys: :atoms)
      config_file.data
    end
  end

  def load do
    cached_conf = load_from_cache()

    if cached_conf do
      cached_conf
    else
      load_from_file()
    end
  end

  def get_topics(use_cache \\ true) do
    data = if use_cache, do: load(), else: load_from_file()
    data |> Enum.map(fn e -> e.topic end)
  end

  def get_tables(use_cache \\ true) do
    data = if use_cache, do: load(), else: load_from_file()
    data |> Enum.map(fn e -> e.source_table end)
  end

  def get_by_topic(topic) do
    load() |> Enum.find(fn e -> e.topic == topic end)
  end

  def get_by_source_tbl(source) do
    load() |> Enum.filter(fn e -> e.source_table == source end)
  end

  # def remap(%{remap: %{}}, data), do: data

  def remap(%{remap: %{}} = map, data) do
    map.remap
    |> Enum.map(fn {k, v} ->
      v =
        if is_binary(v) do
          case Regex.scan(~r/{([^}]*)}/, v) do
            [[k1, k2]] ->
              rplced_val = Map.get(data, String.to_atom(k2))

              if v == k1 do
                rplced_val
              else
                String.replace(v, k1, rplced_val)
              end

            _ ->
              v
          end
        else
          v
        end

      {k, v}
    end)
    |> Map.new()
    |> Map.merge(%{ver: Map.get(data, :ver, 0)})
  end

  def remap(_, data), do: data
end
