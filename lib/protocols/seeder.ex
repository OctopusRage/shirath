defmodule Shirath.Seeder do
  use GenServer

  def init(_state) do
    {:ok, nil}
  end

  def start_link(_ \\ nil) do
    GenServer.start_link(__MODULE__, fn -> [] end, name: __MODULE__)
  end

  def handle_cast({:run}, state) do
    Shirath.TableCopy.run_async()
    {:noreply, state}
  end

  def handle_cast({:copy_table, args}, state) do
    apply(Shirath.TableCopy, :copy_table_async, args)
    {:noreply, state}
  end

  def run do
    GenServer.cast(__MODULE__, {:run})
  end

  def copy_table(table, batch \\ 10000, max_id \\ nil) do
    args = [table, batch, max_id]
    GenServer.cast(__MODULE__, {:copy_table, args})
  end

end
