defmodule RedixCluster.SlotLoader do
  @moduledoc false

  use GenServer
  require Logger

  defstruct [:name, :nodes, :refresh_interval_ms]

  @default_refresh_timeout :timer.seconds(10)

  ## Public API

  def start_link(name, nodes, refresh_interval) do
    refresh_interval_ms = :timer.seconds(refresh_interval)

    GenServer.start_link(
      __MODULE__,
      %__MODULE__{
        name: name,
        nodes: nodes,
        refresh_interval_ms: refresh_interval_ms
      },
      name: proc_name(name)
    )
  end

  def slot_info(name) when is_atom(name) do
    :ets.tab2list(name)
  end

  # Sync call to refresh slot_info
  def refresh_slot_info(name, timeout \\ @default_refresh_timeout) when is_atom(name) do
    proc_name(name)
    |> GenServer.call(:fetch_slot_info, timeout)
  end

  defp proc_name(name) do
    :"#{name}_slot_loader"
  end

  ## Callbacks

  @impl true
  def init(%__MODULE__{name: name} = state) do
    if :ets.info(name) == :undefined do
      :ets.new(name, [:set, :protected, :named_table, read_concurrency: true])
    end

    {:ok, state}
  end

  @impl true
  def handle_call(:fetch_slot_info, from, state) do
    state = handle_fetch_slot_info(state)

    {:reply, from, state}
  end

  @impl true
  def handle_info(:fetch_slot_info, state) do
    state = handle_fetch_slot_info(state)

    {:noreply, state}
  end

  defp handle_fetch_slot_info(
         %__MODULE__{
           name: name,
           nodes: nodes,
           refresh_interval_ms: refresh_interval_ms
         } = state
       ) do
    with {:ok, slot_info_raw} <- fetch_slot_info(nodes),
         slot_info <- parse_slot_info(slot_info_raw) do
      :ets.insert(name, {:slot_info, slot_info})
      Process.send_after(self(), :fetch_slot_info, refresh_interval_ms)

      state
    else
      {:error, _error} ->
        state
    end
  end

  defp fetch_slot_info(nodes) do
    node_pid = Enum.random(nodes) |> elem(1)

    Redix.command(node_pid, ["CLUSTER", "SLOTS"])
    |> case do
      {:ok, slot_info} ->
        {:ok, slot_info}

      {:error, error} ->
        Logger.error("Error while fetching slot info inspect#{error}")

        {:error, error}
    end
  end

  # Response Format:
  # - Start slot range
  # - End slot range
  # - Master for slot range represented as nested IP/Port array
  # - First replica of master for slot range
  # - Second replica
  # ...continues until all replicas for this master are returned.
  # Ref - https://redis.io/commands/cluster-slots#nested-result-array
  defp parse_slot_info(slot_info) do
    Enum.map(slot_info, fn [start_slot, end_slot, master | replicas] ->
      %{
        start_slot: start_slot,
        end_slot: end_slot,
        master: parse_node_info(master),
        replicas: Enum.map(replicas, &parse_node_info/1)
      }
    end)
  end

  defp parse_node_info([node_ip, node_port, _node_id | _] = _node) do
    %{
      ip: node_ip,
      port: node_port
    }
  end
end
