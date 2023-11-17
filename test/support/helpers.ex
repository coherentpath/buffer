defmodule Buffer.Helpers do
  @moduledoc false

  import ExUnit.Callbacks, only: [start_supervised: 1]

  ################################
  # Public API
  ################################

  @doc false
  @spec seed_buffer(GenServer.server()) :: :ok
  def seed_buffer(buffer) do
    Buffer.insert(buffer, "foo")
    Buffer.insert(buffer, "bar")
    Buffer.insert(buffer, "baz")
  end

  @doc false
  @spec start_ex_buffer(keyword()) :: {:ok, GenServer.name()} | {:error, atom()}
  def start_ex_buffer(opts \\ []) do
    name = Keyword.get(opts, :name, Buffer)
    opts = Keyword.put_new(opts, :flush_callback, flush_callback(name))

    case start_supervised({Buffer, opts}) do
      {:ok, pid} -> {:ok, process_name(pid)}
      {:error, {{_, {_, _, reason}}, _}} -> {:error, reason}
      {:error, {reason, _}} -> {:error, reason}
    end
  end

  ################################
  # Private API
  ################################

  defp flush_callback(name) do
    destination = self()
    fn data, opts -> send(destination, {name, data, opts}) end
  end

  defp process_name(pid) do
    pid
    |> Process.info()
    |> Keyword.get(:registered_name)
  end
end
