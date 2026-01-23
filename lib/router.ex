defmodule Shirath.Router do
  alias Shirath.Controllers.ChController
  alias Shirath.Controllers.BackfillController
  alias Shirath.Controllers.MVController
  use Plug.Router

  plug(:match)

  plug(Plug.Parsers,
    parsers: [:urlencoded, :json],
    pass: ["*/*"],
    body_reader: {Shirath.CacheBodyReader, :read_body, []},
    json_decoder: Jason
  )

  plug(:dispatch)

  get "/" do
    send_resp(conn, 200, "ok")
  end

  post "/api/mat_view" do
    ChController.create_mv(conn)
  end

  get "/api/mat_view" do
    ChController.show_mv_job(conn)
  end

  # Backfill endpoints
  get "/api/backfill" do
    BackfillController.index(conn)
  end

  post "/api/backfill" do
    BackfillController.create(conn)
  end

  get "/api/backfill/:id" do
    BackfillController.show(conn, String.to_integer(id))
  end

  post "/api/backfill/:id/pause" do
    BackfillController.pause(conn, String.to_integer(id))
  end

  post "/api/backfill/:id/resume" do
    BackfillController.resume(conn, String.to_integer(id))
  end

  # Materialized view endpoints
  get "/api/mv" do
    MVController.index(conn)
  end

  post "/api/mv" do
    MVController.create(conn)
  end

  get "/api/mv/:name" do
    MVController.show(conn, name)
  end

  post "/api/mv/:name/pause" do
    MVController.pause(conn, name)
  end

  post "/api/mv/:name/resume" do
    MVController.resume(conn, name)
  end

  delete "/api/mv/:name" do
    MVController.delete(conn, name)
  end
end
