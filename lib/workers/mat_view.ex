defmodule Shirath.Workers.MatView do
  alias Shirath.Workers.FileFlusher
  alias Shirath.KV
  use Oban.Worker, queue: :mat_view, max_attempts: 2

  @impl Oban.Worker
  def perform(%Oban.Job{
        args: %{
          "table" => tbl,
          "query" => query
        }
      }) do
    KV.put("mat_view_lock:#{tbl}", true)
    res = ClickhouseMaster.query(query)
    res2 = if apply_to_nodes?() do
      ClickhouseSlave.query(query)
    else
      {:ok, true}
    end

    case [res, res2] do
      [{:ok, _}, {:ok, _}] ->
        %{}
        |> FileFlusher.new()
        |> Oban.insert()

        :ok

      [{:error, err}, _] ->
        IO.inspect(err)
        KV.delete("mat_view_lock:#{tbl}")
        {:cancel, inspect(err)}

      [_, {:error, err}] ->
        IO.inspect(err)
        KV.delete("mat_view_lock:#{tbl}")
        {:cancel, inspect(err)}
    end
  end

  defp apply_to_nodes?() do
    cmaster = Application.get_env(:shirath, ClickhouseMaster)[:connection_strings]
    cslave = Application.get_env(:shirath, ClickhouseSlave)[:connection_strings]
    cmaster != cslave
  end
end
