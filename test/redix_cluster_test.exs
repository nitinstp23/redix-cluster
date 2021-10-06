defmodule RedixClusterTest do
  use ExUnit.Case, async: false

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
      cluster_name = :test_cluster

      {:ok, pid} = RedixCluster.start_link(name: cluster_name, nodes: nodes)

      assert is_pid(pid)
      assert RedixCluster.command(cluster_name, ["PING"]) == {:ok, "PONG"}

      # verify cluster process state
      {:connected, %RedixCluster.Connection{name: ^cluster_name, nodes: nodes_info}} =
        :sys.get_state(pid)

      assert Map.keys(nodes_info) == nodes
    end

    @tag :skip
    test "caches cluster slots info", %{nodes: nodes} do
      cluster_name = :test_cluster

      {:ok, pid} = RedixCluster.start_link(name: cluster_name, nodes: nodes)

      {:connected,
       %RedixCluster.Connection{name: ^cluster_name, nodes: _nodes, slot_loader: slot_loader_pid}} =
        :sys.get_state(pid, 20_000)

      assert is_pid(slot_loader_pid)
      refute :ets.info(cluster_name) == :undefined

      assert :ets.tab2list(cluster_name) == [
               slot_info: [
                 %{
                   end_slot: 5460,
                   master: %{ip: "127.0.0.1", port: 7000},
                   replicas: [%{ip: "127.0.0.1", port: 7003}],
                   start_slot: 0
                 },
                 %{
                   end_slot: 10922,
                   master: %{ip: "127.0.0.1", port: 7001},
                   replicas: [%{ip: "127.0.0.1", port: 7004}],
                   start_slot: 5461
                 },
                 %{
                   end_slot: 16383,
                   master: %{ip: "127.0.0.1", port: 7002},
                   replicas: [%{ip: "127.0.0.1", port: 7005}],
                   start_slot: 10923
                 }
               ]
             ]
    end
  end

  describe "hash slot calculation" do
    test "hash slot" do
      nodes = [
        "redis://localhost:7000",
        "redis://localhost:7001",
        "redis://localhost:7002",
        "redis://localhost:7003",
        "redis://localhost:7004",
        "redis://localhost:7005"
      ]

      {:ok, _pid} = RedixCluster.start_link(name: :test_cluster, nodes: nodes)
      {:ok, conn} = Redix.start_link(host: "localhost", port: 7000)

      for key <- [
            "k",
            "k1",
            "hello world",
            "this is a redix cluster repo",
            "unicode !&*^@*#&漢字^"
            # "a{bcd}" this is not working because of deterministic hashing, @todo https://redis.com/blog/redis-clustering-best-practices-with-keys/
          ] do
        {:ok, expected_hash} = Redix.command(conn, ["CLUSTER", "KEYSLOT", key])
        assert expected_hash == RedixCluster.SlotFinder.hash_slot(key)
      end
    end
  end
end
