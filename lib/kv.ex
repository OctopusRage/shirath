defmodule Shirath.KV do
  use Agent

  @doc """
  Starts a new bucket.
  """
  def start_link(initial_value \\ %{}) do
    Agent.start_link(fn -> initial_value end, name: __MODULE__)
  end
  @doc """
  Gets a value from the `bucket` by `key`.
  """
  def get(key) do
    Agent.get(__MODULE__, &Map.get(&1, key))
  end

  @doc """
  Puts the `value` for the given `key` in the `bucket`.
  """
  def put(key, value) do
    Agent.update(__MODULE__, &Map.put(&1, key, value))
  end

  def delete(key) do
    Agent.update(__MODULE__, &Map.delete(&1, key))
  end

  def incr(key, value \\ 1) when is_integer(value) do
    value = case get(key) do
      nil -> value
      v -> v + value
    end
    Agent.update(__MODULE__, &Map.put(&1, key, value))
  end

  def show() do
    Agent.get(__MODULE__, fn state -> state end)
  end

  def flush() do
    Agent.cast(__MODULE__, fn _ -> %{} end )
  end
end
