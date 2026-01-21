defmodule ShirathTest do
  use ExUnit.Case
  doctest Shirath

  test "greets the world" do
    assert Shirath.hello() == :world
  end
end
