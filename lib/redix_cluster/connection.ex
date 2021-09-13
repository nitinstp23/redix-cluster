defmodule RedixCluster.Connection do
  @moduledoc false

  alias RedixCluster.SlotLoader

  @behaviour :gen_statem

  # 10 seconds
  @default_slot_refresh_interval 10

  defstruct [:name, :nodes, :slot_refresh_interval, :slot_loader]

  ## Public API

  @spec start_link(keyword()) :: list()
  def start_link(opts) when is_list(opts) do
    # TODO: add options validator
    nodes = Keyword.fetch!(opts, :nodes)
    name = Keyword.fetch!(opts, :name)

    slot_refresh_interval =
      Keyword.get(opts, :slot_refresh_interval, @default_slot_refresh_interval)

    :gen_statem.start_link({:local, name}, __MODULE__, {nodes, name, slot_refresh_interval}, [])
  end

  @spec node_pid(atom()) :: {:ok, %__MODULE__{}}
  def node_pid(name) when is_atom(name) do
    conn_pid = GenServer.whereis(name)

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
  def init({nodes, name, slot_refresh_interval} = _opts) do
    data = %__MODULE__{
      name: name,
      nodes: nodes,
      slot_refresh_interval: slot_refresh_interval
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

    actions = [{:next_event, :internal, :fetch_slot_info}]
    {:next_state, :connected, %{data | nodes: node_pids}, actions}
  end

  def disconnected({:call, {from, ref}}, :get_node, data) do
    send(from, {ref, {:error, %Redix.ConnectionError{reason: :closed}}})

    actions = [{:next_event, :internal, :connect}]
    {:ok, :disconnected, data, actions}
  end

  def connected(
        :internal,
        :fetch_slot_info,
        %__MODULE__{name: name, nodes: nodes, slot_refresh_interval: slot_refresh_interval} = data
      ) do
    # TODO: Monitor slot_loader proc and handle crashes
    {:ok, slot_loader} = SlotLoader.start_link(name, nodes, slot_refresh_interval)
    data = %{data | slot_loader: slot_loader}

    SlotLoader.slot_info(name)
    |> case do
      [] ->
        # TODO: Add retry with backoff
        SlotLoader.refresh_slot_info(name)

        {:next_state, :connected, data}

      _ ->
        {:next_state, :connected, data}
    end
  end

  def connected({:call, {from, ref}}, :get_node, %__MODULE__{nodes: nodes} = _data) do
    node_pid = Enum.random(nodes) |> elem(1)
    send(from, {ref, {:ok, node_pid}})

    :keep_state_and_data
  end
end
