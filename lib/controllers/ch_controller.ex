defmodule Shirath.Controllers.ChController do
  import Plug.Conn
  alias Shirath.Workers.MatView

  def create_mv(conn) do
    params = conn.params
    table = params["table"]

    {:ok, query, conn} = Plug.Conn.read_body(conn)

    if table && query do
      {:ok, data} =
        %{
          "table" => table,
          "query" => query
        }
        |> MatView.new()
        |> Oban.insert()

      conn
      |> put_resp_header("content-type", "application/json")
      |> send_resp(200, %{id: data.id} |> Jason.encode!())
    else
      send_resp(conn, 400, "params not completed")
    end
  end

  def show_mv_job(conn) do
    params = conn.params

    id = params["id"]

    if id do
      data = Oban.Job |> Shirath.ObanRepo.get(id)

      if data do
        conn
        |> put_resp_header("content-type", "application/json")
        |> send_resp(200, %{state: data.state, errors: data.errors} |> Jason.encode!())
      else
        send_resp(conn, 404, "not found")
      end
    else
      send_resp(conn, 400, "params not completed")
    end
  end
end
