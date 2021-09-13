defmodule RedixCluster.Connection do
  @moduledoc false

  @behaviour :gen_statem

  defstruct [:nodes, :name]

  ## Public API

  @spec start_link(keyword()) :: list()
  def start_link(opts) when is_list(opts) do
    # TODO: add options validator
    nodes = Keyword.fetch!(opts, :nodes)
    name = Keyword.fetch!(opts, :name)

    :gen_statem.start_link({:local, name}, __MODULE__, {nodes, name}, [])
  end

  @spec node_pid(atom()) :: {:ok, %__MODULE__{}}
  def node_pid(conn) do
    conn_pid = GenServer.whereis(conn)

    :gen_statem.call(conn_pid, :get_node)
    |> case do
      {:ok, node} ->
        {:ok, node}

      {:error, error} ->
        {:error, error}
    end
  end

  ## Callbacks

  ## Init callbacks

  @impl true
  def callback_mode(), do: :state_functions

  @impl true
  def init({nodes, name} = _opts) do
    data = %__MODULE__{
      nodes: nodes,
      name: name
    }

    actions = [{:next_event, :internal, :connect}]
    {:ok, :disconnected, data, actions}
  end

  ## State functions

  def disconnected(:internal, :connect, %__MODULE__{nodes: nodes} = data) do
    node_pids =
      Enum.map(nodes, fn node_url ->
        {:ok, pid} = Redix.start_link(node_url)

        {node_url, pid}
      end)
      |> Map.new()

    {:next_state, :connected, %{data | nodes: node_pids}}
  end

  def disconnected({:call, {from, ref}}, :get_node, data) do
    send(from, {ref, {:error, %Redix.ConnectionError{reason: :closed}}})

    actions = [{:next_event, :internal, :connect}]
    {:ok, :disconnected, data, actions}
  end

  def connected({:call, {from, ref}}, :get_node, %__MODULE__{nodes: nodes} = _data) do
    node_pid = Enum.random(nodes) |> elem(1)
    send(from, {ref, {:ok, node_pid}})

    :keep_state_and_data
  end
end
