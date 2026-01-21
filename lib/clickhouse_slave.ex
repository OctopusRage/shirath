defmodule ClickhouseSlave do
  use Pillar,
    connection_strings: conn_string(),
    name: __MODULE__,
    pool_size: 15,
    timeout: 60 * 1000

  defp conn_string do
    [connection_strings: c_s] = Application.get_env(:shirath, ClickhouseSlave)
    c_s |> String.split("|")
  end
end
