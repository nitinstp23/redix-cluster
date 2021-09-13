defmodule RedixClusterTest do
  use ExUnit.Case
  doctest RedixCluster

  test "greets the world" do
    assert RedixCluster.hello() == :world
  end
end
