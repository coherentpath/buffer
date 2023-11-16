defmodule Buffer.Buffer.Server do
  @moduledoc false

  use GenServer

  alias Buffer.Buffer

  @server_fields [
    :buffer_timeout,
    :flush_callback,
    :flush_meta,
    :jitter_rate,
    :max_length,
    :max_size,
    :partition,
    :size_callback
  ]

  ################################
  # Public API
  ################################

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {opts, server_opts} = Keyword.split(opts, @server_fields)
    GenServer.start_link(__MODULE__, opts, server_opts)
  end

  @doc false
  @spec dump(GenServer.server()) :: list()
  def dump(buffer), do: GenServer.call(buffer, :dump)

  @doc false
  @spec flush(GenServer.server(), keyword()) :: :ok
  def flush(buffer, opts \\ []) do
    if Keyword.get(opts, :async, true) do
      GenServer.call(buffer, :async_flush)
    else
      GenServer.call(buffer, :sync_flush)
    end
  end

  @doc false
  @spec info(GenServer.server()) :: map()
  def info(buffer), do: GenServer.call(buffer, :info)

  @doc false
  @spec insert(GenServer.server(), term()) :: :ok
  def insert(buffer, item), do: GenServer.call(buffer, {:insert, item})

  @doc false
  @spec insert_batch(GenServer.server(), Enumerable.t(), keyword()) :: :ok
  def insert_batch(buffer, items, opts \\ []) do
    if Keyword.get(opts, :safe_flush, true) do
      GenServer.call(buffer, {:safe_insert_batch, items})
    else
      GenServer.call(buffer, {:unsafe_insert_batch, items})
    end
  end

  ################################
  # GenServer Callbacks
  ################################

  @doc false
  @impl GenServer
  @spec init(keyword()) :: {:ok, Buffer.t(), {:continue, :refresh}} | {:stop, atom()}
  def init(opts) do
    Process.flag(:trap_exit, true)

    case init_buffer(opts) do
      {:ok, buffer} -> {:ok, buffer, {:continue, :refresh}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @doc false
  @impl GenServer
  @spec handle_call(term(), GenServer.from(), Buffer.t()) ::
          {:reply, term(), Buffer.t()}
          | {:reply, term(), Buffer.t(), {:continue, :flush | :refresh}}
  def handle_call(:async_flush, _from, buffer) do
    {:reply, :ok, buffer, {:continue, :flush}}
  end

  def handle_call(:dump, _from, buffer) do
    {:reply, Buffer.items(buffer), buffer, {:continue, :refresh}}
  end

  def handle_call(:info, _from, buffer), do: {:reply, build_info(buffer), buffer}

  def handle_call({:insert, item}, _from, buffer) do
    case Buffer.insert(buffer, item) do
      {:flush, buffer} -> {:reply, :ok, buffer, {:continue, :flush}}
      {:cont, buffer} -> {:reply, :ok, buffer}
    end
  end

  def handle_call({:safe_insert_batch, items}, _from, buffer) do
    {:reply, :ok, do_safe_insert_batch(buffer, items)}
  end

  def handle_call({:unsafe_insert_batch, items}, _from, buffer) do
    case do_unsafe_insert_batch(buffer, items) do
      {:flush, buffer} -> {:reply, :ok, buffer, {:continue, :flush}}
      {:cont, buffer} -> {:reply, :ok, buffer}
    end
  end

  def handle_call(:sync_flush, _from, buffer) do
    do_flush(buffer)
    {:reply, :ok, buffer, {:continue, :refresh}}
  end

  @doc false
  @impl GenServer
  @spec handle_continue(term(), Buffer.t()) ::
          {:noreply, Buffer.t()} | {:noreply, Buffer.t(), {:continue, :refresh}}
  def handle_continue(:flush, buffer) do
    do_flush(buffer)
    {:noreply, buffer, {:continue, :refresh}}
  end

  def handle_continue(:refresh, buffer), do: {:noreply, refresh(buffer)}

  @doc false
  @impl GenServer
  @spec handle_info(term(), Buffer.t()) ::
          {:noreply, Buffer.t()} | {:noreply, Buffer.t(), {:continue, :flush}}
  def handle_info({:timeout, timer, :flush}, buffer) when timer == buffer.timer do
    {:noreply, buffer, {:continue, :flush}}
  end

  def handle_info(_, buffer), do: {:noreply, buffer}

  @doc false
  @impl GenServer
  @spec terminate(term(), Buffer.t()) :: term()
  def terminate(_, buffer), do: do_flush(buffer)

  ################################
  # Private API
  ################################

  defp init_buffer(opts) do
    case Keyword.get(opts, :flush_callback) do
      nil -> {:error, :invalid_callback}
      _ -> Buffer.new(opts)
    end
  end

  defp build_info(buffer) do
    %{
      length: buffer.length,
      max_length: buffer.max_length,
      max_size: buffer.max_size,
      next_flush: get_next_flush(buffer),
      partition: buffer.partition,
      size: buffer.size,
      timeout: buffer.timeout
    }
  end

  defp do_safe_insert_batch(buffer, items) do
    Enum.reduce(items, buffer, fn item, acc ->
      case Buffer.insert(acc, item) do
        {:flush, acc} ->
          do_flush(acc)
          refresh(acc)

        {:cont, acc} ->
          acc
      end
    end)
  end

  defp do_unsafe_insert_batch(buffer, items) do
    Enum.reduce(items, {:cont, buffer}, fn item, acc ->
      {_, buffer} = acc
      Buffer.insert(buffer, item)
    end)
  end

  defp refresh(%Buffer{timeout: :infinity} = buffer), do: Buffer.refresh(buffer)

  defp refresh(buffer) do
    cancel_upcoming_flush(buffer)
    timer = schedule_next_flush(buffer)
    Buffer.refresh(buffer, timer)
  end

  defp cancel_upcoming_flush(%Buffer{timer: nil}), do: :ok
  defp cancel_upcoming_flush(buffer), do: Process.cancel_timer(buffer.timer)

  defp schedule_next_flush(buffer) do
    # We use `:erlang.start_timer/3` to include the timer ref in the message. This is necessary
    # for handling race conditions resulting from multiple simultaneous flush conditions.
    :erlang.start_timer(buffer.timeout, self(), :flush)
  end

  defp get_next_flush(%Buffer{timer: nil}), do: nil

  defp get_next_flush(buffer) do
    with false <- Process.read_timer(buffer.timer), do: nil
  end

  defp do_flush(buffer) do
    opts = [length: buffer.length, meta: buffer.flush_meta, size: buffer.size]

    buffer
    |> Buffer.items()
    |> buffer.flush_callback.(opts)
  end
end
