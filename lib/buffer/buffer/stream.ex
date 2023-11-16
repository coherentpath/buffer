defmodule Buffer.Buffer.Stream do
  @moduledoc false

  alias Buffer.Buffer

  @fields [:max_length, :max_size, :size_callback]

  ################################
  # Public API
  ################################

  @doc false
  @spec chunk(Enumerable.t(), keyword()) :: {:ok, Enumerable.t()} | {:error, atom()}
  def chunk(enum, opts \\ []) do
    opts = Keyword.take(opts, @fields)

    with {:ok, buffer} <- Buffer.new(opts) do
      {:ok, Stream.chunk_while(enum, buffer, &chunk_fun(&2, &1), &after_fun/1)}
    end
  end

  @doc false
  @spec chunk!(Enumerable.t(), keyword()) :: Enumerable.t()
  def chunk!(enum, opts \\ []) do
    case chunk(enum, opts) do
      {:ok, stream} -> stream
      {:error, reason} -> raise(ArgumentError, to_message(reason))
    end
  end

  ################################
  # Private API
  ################################

  defp chunk_fun(buffer, item) do
    with {:flush, buffer} <- Buffer.insert(buffer, item) do
      {:cont, Buffer.items(buffer), Buffer.refresh(buffer)}
    end
  end

  defp after_fun(%Buffer{buffer: []} = buffer), do: {:cont, buffer}
  defp after_fun(buffer), do: {:cont, Buffer.items(buffer), Buffer.refresh(buffer)}

  defp to_message(reason), do: String.replace(to_string(reason), "_", " ")
end
