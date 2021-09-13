defmodule RedixCluster do
  @moduledoc """
  Documentation for `RedixCluster`
  """

  alias RedixCluster.Connection

  @type command() :: [String.Chars.t()]

  @spec start_link(keyword()) :: :gen_statem.start_ret()
  def start_link(opts \\ [])

  def start_link(opts) when is_list(opts), do: Connection.start_link(opts)

  @spec command(atom(), command(), keyword()) ::
          {:ok, Redix.Protocol.redis_value()}
          | {:error, atom() | Redix.Error.t() | Redix.ConnectionError.t()}
  def command(conn, command, opts \\ []) do
    Connection.node_pid(conn)
    |> case do
      {:ok, pid} ->
        Redix.command(pid, command, opts)

      {:error, error} ->
        {:error, error}
    end
  end
end
