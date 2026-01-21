defmodule Shirath.Events do
  use GenServer

  alias Shirath.KV
  alias Shirath.MapConfig
  alias Shirath.Helper
  alias Shirath.Ingestor
  alias Cainophile.{Adapters, Changes}

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{})
  end

  @impl true
  def init(state) do
    # create publication if not exists
    create_publication_if_not_exists()

    # subscribe to the Cainophile process in order to receive "todos" events (see handle_info bellow)
    Adapters.Postgres.subscribe(__MODULE__, self())

    {:ok, state}
  end

  defp create_publication_if_not_exists do
    publication = System.get_env("CDC_PUBLICATION") || "eshop_pub"
    Shirath.Repo.query("CREATE PUBLICATION IF NOT EXISTS #{publication} FOR ALL TABLES")
  end

  @impl true
  def handle_info(
        %Changes.Transaction{
          changes: changes
        },
        state
      ) do
    try do
      Task.start(fn ->
        Shirath.KV.incr("event_rate")
        process_changes(changes)
      end)
    rescue
      _e ->
        nil
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(_, state), do: {:noreply, state}

  defp process_changes(changes) do
    Enum.map(changes, fn e ->
      process_data(e)
    end)
  end

  defp process_data(%Cainophile.Changes.NewRecord{relation: {_, t}, record: r}) do
    send_data(t, r)
  end

  defp process_data(%Cainophile.Changes.UpdatedRecord{relation: {_, t}, record: r}) do
    send_data(t, r)
  end

  defp process_data(_) do
    nil
  end

  defp send_data(t, r) do
    version = System.os_time()

    if t in MapConfig.get_tables() do
      r = r |> Helper.atomize_keys() |> Map.merge(%{ver: version})

      if KV.get("sync:#{t}") do
        KV.put("#{t}:#{r.id}", true)
      end

      map = MapConfig.get_by_source_tbl(t)

      case map do
        [%{script: %{module: mod, function: fun}}] ->
          mod = "Elixir." <> mod

          mod =
            try do
              String.to_existing_atom(mod)
            rescue
              _ ->
                String.to_atom(mod)
            end

          # mod = String.to_existing_atom(mod)
          fun = String.to_atom(fun)

          apply(mod, fun, [%{table: t, record: r}])

        _ ->
          Ingestor.push_message(t, r)
      end
    end
  end

  def process_deleted(%{table: t, record: %{val: val, tbl: destined_tbl}}) do
    map = MapConfig.get_by_source_tbl(t) |> List.first()

    if map do
      version = System.os_time()
      val = val |> Map.merge(%{deleted_at: DateTime.utc_now(), ver: version})
      Ingestor.push_message_force(destined_tbl, val)
    end
  end
end
