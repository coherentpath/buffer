defmodule Buffer.State do
  @moduledoc false

  defstruct [
    :flush_callback,
    :flush_meta,
    :max_length,
    :max_size,
    :partition,
    :size_callback,
    :timeout,
    buffer: [],
    length: 0,
    size: 0,
    timer: nil
  ]

  @type t :: %__MODULE__{}

  ################################
  # Public API
  ################################

  @doc false
  @spec insert(t(), term()) :: {:flush, t()} | {:cont, t()}
  def insert(buffer, item) do
    buffer = %{
      buffer
      | buffer: [item | buffer.buffer],
        length: buffer.length + 1,
        size: buffer.size + buffer.size_callback.(item)
    }

    if flush?(buffer), do: {:flush, buffer}, else: {:cont, buffer}
  end

  @doc false
  @spec items(t()) :: list()
  def items(buffer), do: Enum.reverse(buffer.buffer)

  @doc false
  @spec new(keyword()) :: {:ok, t()} | {:error, atom()}
  def new(opts) do
    with {:ok, jitter} <- get_jitter(opts),
         {:ok, flush_callback} <- get_flush_callback(opts),
         {:ok, size_callback} <- get_size_callback(opts),
         {:ok, max_length} <- get_max_length(opts, jitter),
         {:ok, max_size} <- get_max_size(opts, jitter),
         {:ok, timeout} <- get_timeout(opts, jitter) do
      buffer = %__MODULE__{
        flush_callback: flush_callback,
        flush_meta: Keyword.get(opts, :flush_meta),
        max_length: max_length,
        max_size: max_size,
        partition: Keyword.get(opts, :partition, 0),
        size_callback: size_callback,
        timeout: timeout
      }

      {:ok, buffer}
    end
  end

  @doc false
  @spec refresh(t(), reference() | nil) :: t()
  def refresh(buffer, timer \\ nil) do
    %{buffer | buffer: [], length: 0, size: 0, timer: timer}
  end

  ################################
  # Private API
  ################################

  defp get_jitter(opts) do
    case Keyword.get(opts, :jitter_rate, 0.0) do
      jitter when jitter < 0 or jitter > 1 -> {:error, :invalid_jitter}
      jitter -> {:ok, jitter}
    end
  end

  defp get_flush_callback(opts) do
    case Keyword.get(opts, :flush_callback) do
      nil -> {:ok, nil}
      callback -> validate_callback(callback, 2)
    end
  end

  defp get_size_callback(opts) do
    opts
    |> Keyword.get(:size_callback, &item_size/1)
    |> validate_callback(1)
  end

  defp get_max_length(opts, jitter) do
    validate_limit(Keyword.get(opts, :max_length, :infinity), jitter)
  end

  defp get_max_size(opts, jitter) do
    validate_limit(Keyword.get(opts, :max_size, :infinity), jitter)
  end

  defp get_timeout(opts, jitter) do
    validate_limit(Keyword.get(opts, :buffer_timeout, :infinity), jitter)
  end

  defp validate_callback(fun, arity) when is_function(fun, arity), do: {:ok, fun}
  defp validate_callback(_, _), do: {:error, :invalid_callback}

  defp validate_limit(:infinity, _), do: {:ok, :infinity}
  defp validate_limit(_, jitter) when jitter < 0 or jitter > 1, do: {:error, :invalid_jitter}

  defp validate_limit(limit, jitter) when is_integer(limit) and limit >= 0 do
    {:ok, round(limit * (1 - jitter * :rand.uniform()))}
  end

  defp validate_limit(_, _), do: {:error, :invalid_limit}

  defp item_size(item) when is_bitstring(item), do: byte_size(item)

  defp item_size(item) do
    item
    |> :erlang.term_to_binary()
    |> byte_size()
  end

  defp flush?(buffer) do
    exceeds?(buffer.length, buffer.max_length) or exceeds?(buffer.size, buffer.max_size)
  end

  defp exceeds?(_, :infinity), do: false
  defp exceeds?(num, max), do: num >= max
end
