defmodule Shirath.Ingestor.Producer do
  use GenStage
  @behaviour Broadway.Producer

  def start_link(_opts) do
    GenStage.start_link(__MODULE__, [])
  end

  @impl true
  def init(_opts) do
    state = %{demand: 0}
    {:producer, state}
  end

  @impl true
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    state = %{state | demand: demand + incoming_demand}
    {:noreply, [], state}
  end

end
