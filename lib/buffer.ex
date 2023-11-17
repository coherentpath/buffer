defmodule Buffer do
  @moduledoc """
  A simple data buffer that can be added directly to a supervision tree.
  """

  alias Buffer.{Server, Stream}

  @supervisor_fields [:name, :partitioner, :partitions]

  ################################
  # Public API
  ################################

  @doc false
  @spec child_spec(keyword()) :: map()
  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Starts a `Buffer` process linked to the current process.

  ## Options

    * `:flush_callback` - The function invoked to flush a buffer. Must have an arity of 2
      where the first arg is a list of items and the second arg is a keyword list of flush
      opts. This option is required.

    * `:name` - The name of the buffer. Must be an atom or a `:via` tuple. This option is
      required.

    * `:buffer_timeout` - The maximum time (in ms) allowed between flushes of the buffer.
      Defaults to `:infinity`

    * `:flush_meta` - Any term to be included in the flush opts under the `:meta` key.

    * `:jitter_rate` - The rate at which limits are jittered between partitions. Limits are not
      jittered by default.

    * `:max_length` - The maximum number of items allowed in the buffer before being flushed.
      By default, this limit is `:infinity`.

    * `:max_size` - The maximum size (in bytes) of the buffer before being flushed. By default,
      this limit is `:infinity`.

    * `:partitioner` - The method by which items are inserted into different partitions. The
      options are `:rotating` and `:random` and the former is the default.

    * `:partitions` - The number of buffer partitions.

    * `:size_callback` - The function invoked to determine item size. Must have an arity of 1
      where the only arg is an inserted item and must return a non-negative integer representing
      the size of the item. Default size callback is `byte_size` (predicated by `term_to_binary`
      if applicable).
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    opts = Keyword.put_new(opts, :name, __MODULE__)

    with {:ok, partitions} <- validate_partitions(opts),
         {:ok, partitioner} <- validate_partitioner(opts),
         {:ok, _} = result <- do_start_link(opts) do
      partitioner = build_partitioner(partitions, partitioner)
      name = Keyword.get(opts, :name)
      put_buffer(name, partitioner, partitions)
      result
    end
  end

  @doc """
  Lazily chunks an enumerable based on `Buffer` flush conditions.

  ## Options

    * `:max_length` - The maximum number of items in a chunk. By default, this limit is `:infinity`.

    * `:max_size` - The maximum size (in bytes) of the items in a chunk. By default, this limit is
      `:infinity`.

    * `:size_callback` - The function invoked to determine the size of an item. Default size callback
      is `byte_size` (predicated by `term_to_binary` if applicable).
  """
  @spec chunk(Enumerable.t(), keyword()) :: {:ok, Enumerable.t()} | {:error, atom()}
  defdelegate chunk(enum, opts \\ []), to: Stream

  @doc """
  Lazily chunks an enumerable based on `Buffer` flush conditions and raises an `ArgumentError`
  with invalid options.

  For information on options, see `chunk/2`.
  """
  @spec chunk!(Enumerable.t(), keyword()) :: Enumerable.t()
  defdelegate chunk!(enum, opts \\ []), to: Stream

  @doc """
  Dumps the contents of the given `Buffer` to a list, bypassing a flush
  callback and resetting the buffer.

  ## Options

    * `:partition` - The specific partition to dump. Defaults to `:all`.
  """
  @spec dump(GenServer.server(), keyword()) :: {:ok, list()} | {:error, atom()}
  def dump(buffer, opts \\ []) do
    with {:ok, {_, parts}} <- fetch_buffer(buffer),
         {:ok, part} <- validate_partition(opts, parts) do
      case part do
        :all -> {:ok, Enum.reduce(1..parts, [], &(&2 ++ do_dump_part(buffer, &1 - 1)))}
        part -> {:ok, do_dump_part(buffer, part)}
      end
    end
  end

  @doc """
  Flushes the given `Buffer`, regardless of whether or not the flush conditions
  have been met.

  ## Options

    * `:async` - Whether or not the flush will be async. Defaults to `true`.

    * `:partition` - The specific partition to flush. Defaults to `:all`.
  """
  @spec flush(GenServer.server(), keyword()) :: :ok | {:error, atom()}
  def flush(buffer, opts \\ []) do
    with {:ok, {_, parts}} <- fetch_buffer(buffer),
         {:ok, part} <- validate_partition(opts, parts) do
      case part do
        :all -> Enum.each(1..parts, &do_flush_part(buffer, &1 - 1, opts))
        part -> do_flush_part(buffer, part, opts)
      end
    end
  end

  @doc """
  Returns information about the given `Buffer`.

  ## Options

    * `:partition` - The specific partition to return info for. Defaults to `:all`.
  """
  @spec info(GenServer.server(), keyword()) :: {:ok, list()} | {:error, atom()}
  def info(buffer, opts \\ []) do
    with {:ok, {_, parts}} <- fetch_buffer(buffer),
         {:ok, part} <- validate_partition(opts, parts) do
      case part do
        :all -> {:ok, Enum.map(1..parts, &do_info_part(buffer, &1 - 1))}
        part -> {:ok, [do_info_part(buffer, part)]}
      end
    end
  end

  @doc """
  Inserts the given item into the given `Buffer`.
  """
  @spec insert(GenServer.server(), term()) :: :ok | {:error, atom()}
  def insert(buffer, item) do
    with {:ok, {partitioner, _}} <- fetch_buffer(buffer) do
      do_insert(buffer, partitioner, item)
    end
  end

  @doc """
  Inserts a batch of items into the given `Buffer`.

  ## Options

    * `:safe_flush` - Whether or not to flush immediately after exceeding a buffer limit.
      Defaults to `true`. If set to `false`, all items in the batch will be inserted
      regardless of flush conditions being met. Afterwards, if a limit has been exceeded,
      the buffer will be flushed async.
  """
  @spec insert_batch(GenServer.server(), Enumerable.t(), keyword()) :: :ok | {:error, atom()}
  def insert_batch(buffer, items, opts \\ []) do
    with {:ok, {partitioner, _}} <- fetch_buffer(buffer) do
      do_insert_batch(buffer, partitioner, items, opts)
    end
  end

  ################################
  # Private API
  ################################

  defguardp is_valid_part(part, parts) when part == :all or (part >= 0 and part < parts)

  defp validate_partitions(opts) do
    case Keyword.get(opts, :partitions, 1) do
      parts when is_integer(parts) and parts > 0 -> {:ok, parts}
      _ -> {:error, :invalid_partitions}
    end
  end

  defp validate_partitioner(opts) do
    case Keyword.get(opts, :partitioner, :rotating) do
      partitioner when partitioner in [:random, :rotating] -> {:ok, partitioner}
      _ -> {:error, :invalid_partitioner}
    end
  end

  defp validate_partition(opts, partitions) do
    case Keyword.get(opts, :partition, :all) do
      part when is_valid_part(part, partitions) -> {:ok, part}
      _ -> {:error, :invalid_partition}
    end
  end

  defp do_start_link(opts) do
    {sup_opts, buffer_opts} = Keyword.split(opts, @supervisor_fields)
    with_args = fn [opts], part -> [Keyword.put(opts, :partition, part)] end
    child_spec = {Server, buffer_opts}

    sup_opts
    |> Keyword.merge(with_arguments: with_args, child_spec: child_spec)
    |> PartitionSupervisor.start_link()
  end

  defp build_partitioner(1, _), do: fn -> 0 end

  defp build_partitioner(partitions, :random) do
    fn -> :rand.uniform(partitions) - 1 end
  end

  defp build_partitioner(partitions, :rotating) do
    atomics_ref = :atomics.new(1, [])

    fn ->
      case :atomics.add_get(atomics_ref, 1, 1) do
        part when part > partitions ->
          :atomics.put(atomics_ref, 1, 0)
          0

        part ->
          part - 1
      end
    end
  end

  defp put_buffer(buffer, partitioner, partitions) do
    buffer
    |> build_key()
    |> :persistent_term.put({partitioner, partitions})
  end

  defp fetch_buffer(buffer) do
    buffer
    |> build_key()
    |> :persistent_term.get(nil)
    |> case do
      nil -> {:error, :not_found}
      buffer -> {:ok, buffer}
    end
  end

  defp build_key(buffer), do: {__MODULE__, buffer}

  defp do_dump_part(buffer, partition) do
    buffer
    |> buffer_partition_name(partition)
    |> Server.dump()
  end

  defp do_flush_part(buffer, partition, opts) do
    buffer
    |> buffer_partition_name(partition)
    |> Server.flush(opts)
  end

  defp do_info_part(buffer, partition) do
    buffer
    |> buffer_partition_name(partition)
    |> Server.info()
  end

  defp do_insert(buffer, partitioner, item) do
    buffer
    |> buffer_partition_name(partitioner.())
    |> Server.insert(item)
  end

  defp do_insert_batch(buffer, partitioner, items, opts) do
    buffer
    |> buffer_partition_name(partitioner.())
    |> Server.insert_batch(items, opts)
  end

  defp buffer_partition_name(buffer, partition) do
    {:via, PartitionSupervisor, {buffer, partition}}
  end
end
