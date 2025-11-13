defmodule Buffer.Server do
  @moduledoc false

  use GenServer

  alias Buffer.State

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
  @spec dump(GenServer.server(), timeout()) :: list()
  def dump(buffer, timeout \\ 5000), do: GenServer.call(buffer, :dump, timeout)

  @doc false
  @spec flush(GenServer.server(), keyword()) :: :ok
  def flush(buffer, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5000)

    if Keyword.get(opts, :async, true) do
      GenServer.call(buffer, :async_flush, timeout)
    else
      GenServer.call(buffer, :sync_flush, timeout)
    end
  end

  @doc false
  @spec info(GenServer.server(), timeout()) :: map()
  def info(buffer, timeout \\ 5000), do: GenServer.call(buffer, :info, timeout)

  @doc false
  @spec insert(GenServer.server(), term(), timeout()) :: :ok
  def insert(buffer, item, timeout \\ 5000), do: GenServer.call(buffer, {:insert, item}, timeout)

  @doc false
  @spec insert_batch(GenServer.server(), Enumerable.t(), keyword()) :: :ok
  def insert_batch(buffer, items, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5000)

    if Keyword.get(opts, :safe_flush, true) do
      GenServer.call(buffer, {:safe_insert_batch, items}, timeout)
    else
      GenServer.call(buffer, {:unsafe_insert_batch, items}, timeout)
    end
  end

  ################################
  # GenServer Callbacks
  ################################

  @doc false
  @impl GenServer
  @spec init(keyword()) :: {:ok, State.t(), {:continue, :refresh}} | {:stop, atom()}
  def init(opts) do
    Process.flag(:trap_exit, true)

    case init_buffer(opts) do
      {:ok, buffer} -> {:ok, buffer, {:continue, :refresh}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @doc false
  @impl GenServer
  @spec handle_call(term(), GenServer.from(), State.t()) ::
          {:reply, term(), State.t()}
          | {:reply, term(), State.t(), {:continue, :flush | :refresh}}
  def handle_call(:async_flush, _from, buffer) do
    {:reply, :ok, buffer, {:continue, :flush}}
  end

  def handle_call(:dump, _from, buffer) do
    {:reply, State.items(buffer), buffer, {:continue, :refresh}}
  end

  def handle_call(:info, _from, buffer), do: {:reply, build_info(buffer), buffer}

  def handle_call({:insert, item}, _from, buffer) do
    case State.insert(buffer, item) do
      {:flush, buffer} -> {:reply, :ok, buffer, {:continue, :flush}}
      {:cont, buffer} -> {:reply, :ok, buffer}
    end
  end

  def handle_call({:safe_insert_batch, items}, _from, buffer) do
    {buffer, count} = do_safe_insert_batch(buffer, items)
    {:reply, count, buffer}
  end

  def handle_call({:unsafe_insert_batch, items}, _from, buffer) do
    case do_unsafe_insert_batch(buffer, items) do
      {{:flush, buffer}, count} -> {:reply, count, buffer, {:continue, :flush}}
      {{:cont, buffer}, count} -> {:reply, count, buffer}
    end
  end

  def handle_call(:sync_flush, _from, buffer) do
    do_flush(buffer)
    {:reply, :ok, buffer, {:continue, :refresh}}
  end

  @doc false
  @impl GenServer
  @spec handle_continue(term(), State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :refresh}}
  def handle_continue(:flush, buffer) do
    do_flush(buffer)
    {:noreply, buffer, {:continue, :refresh}}
  end

  def handle_continue(:refresh, buffer), do: {:noreply, refresh(buffer)}

  @doc false
  @impl GenServer
  @spec handle_info(term(), State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :flush}}
  def handle_info({:timeout, timer, :flush}, buffer) when timer == buffer.timer do
    {:noreply, buffer, {:continue, :flush}}
  end

  def handle_info(_, buffer), do: {:noreply, buffer}

  @doc false
  @impl GenServer
  @spec terminate(term(), State.t()) :: term()
  def terminate(_, buffer), do: do_flush(buffer)

  ################################
  # Private API
  ################################

  defp init_buffer(opts) do
    case Keyword.get(opts, :flush_callback) do
      nil -> {:error, :invalid_callback}
      _ -> State.new(opts)
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
    Enum.reduce(items, {buffer, 0}, fn item, {buffer, count} ->
      case State.insert(buffer, item) do
        {:flush, buffer} ->
          do_flush(buffer)
          {refresh(buffer), count + 1}

        {:cont, buffer} ->
          {buffer, count + 1}
      end
    end)
  end

  defp do_unsafe_insert_batch(buffer, items) do
    Enum.reduce(items, {{:cont, buffer}, 0}, fn item, acc ->
      {{_, buffer}, count} = acc
      {State.insert(buffer, item), count + 1}
    end)
  end

  defp refresh(%State{timeout: :infinity} = buffer), do: State.refresh(buffer)

  defp refresh(buffer) do
    cancel_upcoming_flush(buffer)
    timer = schedule_next_flush(buffer)
    State.refresh(buffer, timer)
  end

  defp cancel_upcoming_flush(%State{timer: nil}), do: :ok
  defp cancel_upcoming_flush(buffer), do: Process.cancel_timer(buffer.timer)

  defp schedule_next_flush(buffer) do
    # We use `:erlang.start_timer/3` to include the timer ref in the message. This is necessary
    # for handling race conditions resulting from multiple simultaneous flush conditions.
    :erlang.start_timer(buffer.timeout, self(), :flush)
  end

  defp get_next_flush(%State{timer: nil}), do: nil

  defp get_next_flush(buffer) do
    with false <- Process.read_timer(buffer.timer), do: nil
  end

  defp do_flush(buffer) do
    opts = [length: buffer.length, meta: buffer.flush_meta, size: buffer.size]

    buffer
    |> State.items()
    |> buffer.flush_callback.(opts)
  end
end
