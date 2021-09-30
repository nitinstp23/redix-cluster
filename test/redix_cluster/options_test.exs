defmodule RedixCluster.OptionsTest do
  use ExUnit.Case, async: true

  alias RedixCluster.Options

  describe "validate/2" do
    test "validate cluster name" do
      valid_opts = [name: :test_cluster, nodes: []]

      assert NimbleOptions.validate(valid_opts, Options.definition()) ==
               {:ok, [{:name, :test_cluster}, {:nodes, []}]}

      invalid_opts = [name: "test_cluster", nodes: []]

      assert NimbleOptions.validate(invalid_opts, Options.definition()) ==
               {:error,
                %NimbleOptions.ValidationError{
                  key: :name,
                  keys_path: [],
                  message: "expected :name to be an atom, got: \"test_cluster\"",
                  value: "test_cluster"
                }}
    end

    test "validate nodes" do
      missing_nodes_opts = [name: :test_cluster]

      assert NimbleOptions.validate(missing_nodes_opts, Options.definition()) ==
               {:error,
                %NimbleOptions.ValidationError{
                  key: :nodes,
                  keys_path: [],
                  message: "required option :nodes not found, received options: [:name]",
                  value: nil
                }}

      invalid_nodes_opts = [
        name: :test_cluster,
        nodes: ["redis://localhost:6379", "invalid://localhost:6379"]
      ]

      assert NimbleOptions.validate(invalid_nodes_opts, Options.definition()) ==
               {
                 :error,
                 %NimbleOptions.ValidationError{
                   __exception__: true,
                   key: :nodes,
                   keys_path: [],
                   message: "invalid scheme for the node url, got: invalid://localhost:6379",
                   value: ["redis://localhost:6379", "invalid://localhost:6379"]
                 }
               }

      valid_nodes_opts = [
        name: :test_cluster,
        nodes: ["redis://localhost:8001", "rediss://localhost:8002"]
      ]

      assert NimbleOptions.validate(valid_nodes_opts, Options.definition()) ==
               {:ok,
                [
                  name: :test_cluster,
                  nodes: ["redis://localhost:8001", "rediss://localhost:8002"]
                ]}
    end
  end
end
