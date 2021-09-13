defmodule RedixClusterTest do
  use ExUnit.Case

  setup _ do
    nodes = [
      "redis://localhost:7000",
      "redis://localhost:7001",
      "redis://localhost:7002",
      "redis://localhost:7003",
      "redis://localhost:7004",
      "redis://localhost:7005"
    ]

    {:ok, %{nodes: nodes}}
  end

  describe "start_link/2" do
    test "connects to cluster with a list of nodes", %{nodes: nodes} do
      {:ok, pid} = RedixCluster.start_link(name: :test_cluster, nodes: nodes)

      assert is_pid(pid)

      assert RedixCluster.command(pid, ["PING"]) == {:ok, "PONG"}
    end
  end
end
