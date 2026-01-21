defmodule Shirath.Workers.FileFlusher do
  alias Shirath.MapConfig
  alias Shirath.KV
  alias Workers.FileBuffer
  use Oban.Worker, queue: :file_flusher

  @impl Oban.Worker
  def perform(%Oban.Job{}) do
    Path.wildcard("mc_data/*")
    |> Enum.group_by(fn e ->
      e = Path.basename(e)
      prefix = e |> String.split("_") |> List.first()
      String.replace(e, [prefix <> "_", ".json"], "")
    end)
    |> Enum.map(fn {k, v} ->
      v
      |> Enum.sort()
      |> Enum.map(fn x ->
        %{"file" => x, "table" => k}
        |> FileBuffer.new()
        |> Oban.insert()
      end)
    end)

    MapConfig.get_tables()
    |> Enum.uniq()
    |> Enum.map(fn e ->
      KV.delete("mat_view_lock:#{e}")
    end)
    :ok
  end
end
