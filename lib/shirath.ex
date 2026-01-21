defmodule Shirath do
  @moduledoc """
  Documentation for `Shirath`.
  """
  # alias Shirath.MapConfig

  @doc """
  Hello world.

  ## Examples

      iex> Shirath.hello()
      :world

  """
  def hello do
    :world
  end

  def start() do
    # WalEx.Config.replace_config(Shirath, :subscriptions, MapConfig.get_tables())
    # Task.start fn -> CDC.Replication.start_link(cdc_conf()) end
  end

  # defp cdc_conf do
  #   cdc_opts = Application.get_env(:shirath, :cdc)
  #   conn_opts = Application.get_env(:shirath, Shirath.Repo)
  #   conn_opts ++ cdc_opts
  # end

end
