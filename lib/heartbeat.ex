defmodule Shirath.Heartbeat do
  alias Shirath.KV
  alias Shirath.Repo
  def send do
    IO.inspect(:tick)
    n = :rand.uniform(100)
    q = """
    WITH upsert AS
          (UPDATE _heartbeat SET heartbeat=#{n} WHERE id=1 RETURNING *)
          INSERT INTO _heartbeat (id, heartbeat) SELECT 1, 1234
          WHERE NOT EXISTS (SELECT * FROM upsert);
    """
    Repo.query(q)
    now = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
    val = KV.get("event_rate")
    if val do
      q = "insert into event_rate (total, created_at) values (#{val}, '#{now}')"
      IO.inspect(q)
      ClickhouseMaster.query(q)
      KV.delete("event_rate")
    end
  end
end
