defmodule RedixCluster.Options do
  @moduledoc false

  definition = [
    name: [
      required: true,
      type: :atom,
      doc: "Used for cluster process registration"
    ],
    nodes: [
      required: true,
      type: {:custom, __MODULE__, :validate_nodes, []},
      doc: "A list of cluster node URLs"
    ]
  ]

  @definition NimbleOptions.new!(definition)

  @valid_schemes ["redis", "rediss"]

  def definition, do: @definition

  def validate_nodes(nodes) when is_list(nodes) do
    Enum.each(nodes, &parse_node_url/1)

    {:ok, nodes}
  rescue
    ex in ArgumentError ->
      {:error, ex.message}
  end

  def validate_nodes(nodes) do
    {:error, "expected :nodes to be list, got: #{inspect(nodes)}"}
  end

  defp parse_node_url(node_url) when is_binary(node_url) do
    URI.parse(node_url)
    |> case do
      %URI{scheme: scheme} = uri when scheme in @valid_schemes ->
        uri

      _uri ->
        raise ArgumentError, "invalid scheme for the node url, got: #{node_url}"
    end
  end

  defp parse_node_url(node_url) do
    {:error, "expected node url to be a string, got: #{inspect(node_url)}"}
  end
end
