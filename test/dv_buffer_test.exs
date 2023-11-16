defmodule DVBufferTest do
  use ExUnit.Case, async: true

  import DVBuffer.Helpers

  describe "start_link/1" do
    test "will start an unpartitioned DVBuffer" do
      assert start_ex_buffer() == {:ok, DVBuffer}
    end

    test "will correctly name an unpartitioned DVBuffer" do
      opts = [name: :ex_buffer]

      assert start_ex_buffer(opts) == {:ok, :ex_buffer}
    end

    test "will start a partitioned DVBuffer" do
      opts = [partitions: 2]

      assert start_ex_buffer(opts) == {:ok, DVBuffer}
    end

    test "will correctly name a partitioned DVBuffer" do
      opts = [name: :ex_buffer, partitions: 2]

      assert start_ex_buffer(opts) == {:ok, :ex_buffer}
    end

    test "will jitter the limits of an DVBuffer" do
      opts = [jitter_rate: 0.05, max_size: 10_000, partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert {:ok, [%{max_size: limit_1}, %{max_size: limit_2}]} = DVBuffer.info(buffer)
      assert limit_1 != limit_2
    end

    test "will not start with an invalid flush callback" do
      opts = [flush_callback: nil]

      assert start_ex_buffer(opts) == {:error, :invalid_callback}
    end

    test "will not start with an invalid size callback" do
      opts = [size_callback: fn _, _ -> :ok end]

      assert start_ex_buffer(opts) == {:error, :invalid_callback}
    end

    test "will not start with an invalid limit" do
      opts = [buffer_timeout: -5]

      assert start_ex_buffer(opts) == {:error, :invalid_limit}
    end

    test "will not start with an invalid partition count" do
      opts = [partitions: -2]

      assert start_ex_buffer(opts) == {:error, :invalid_partitions}
    end

    test "will not start with an invalid partitioner" do
      opts = [partitioner: :fake_partitioner]

      assert start_ex_buffer(opts) == {:error, :invalid_partitioner}
    end

    test "will not start with an invalid jitter rate" do
      opts = [jitter_rate: 3.14]

      assert start_ex_buffer(opts) == {:error, :invalid_jitter}
    end

    test "will flush an DVBuffer on termination" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert PartitionSupervisor.stop(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end
  end

  describe "chunk/2" do
    test "will correctly chunk an enumerable" do
      opts = [max_length: 3, max_size: 10]
      enum = ["foo", "bar", "baz", "foobar", "barbaz", "foobarbaz"]

      assert {:ok, enum} = DVBuffer.chunk(enum, opts)
      assert Enum.into(enum, []) == [["foo", "bar", "baz"], ["foobar", "barbaz"], ["foobarbaz"]]
    end

    test "will correctly chunk an enumerable with a size callback" do
      opts = [max_size: 8, size_callback: &(byte_size(&1) + 1)]
      enum = ["foo", "bar", "baz"]

      assert {:ok, enum} = DVBuffer.chunk(enum, opts)
      assert Enum.into(enum, []) == [["foo", "bar"], ["baz"]]
    end

    test "will return an error with an invalid callback" do
      opts = [size_callback: fn -> :ok end]
      enum = ["foo", "bar", "baz"]

      assert DVBuffer.chunk(enum, opts) == {:error, :invalid_callback}
    end

    test "will return an error with an invalid limit" do
      opts = [max_length: -5]

      assert DVBuffer.chunk(["foo", "bar", "baz"], opts) == {:error, :invalid_limit}
    end
  end

  describe "chunk!/2" do
    test "will correctly chunk an enumerable" do
      opts = [max_length: 3, max_size: 10]
      enum = ["foo", "bar", "baz", "foobar", "barbaz", "foobarbaz"]
      enum = DVBuffer.chunk!(enum, opts)

      assert Enum.into(enum, []) == [["foo", "bar", "baz"], ["foobar", "barbaz"], ["foobarbaz"]]
    end

    test "will correctly chunk an enumerable with a size callback" do
      opts = [max_size: 8, size_callback: &(byte_size(&1) + 1)]
      enum = ["foo", "bar", "baz"]
      enum = DVBuffer.chunk!(enum, opts)

      assert Enum.into(enum, []) == [["foo", "bar"], ["baz"]]
    end

    test "will raise an error with an invalid callback" do
      opts = [size_callback: fn -> :ok end]
      enum = ["foo", "bar", "baz"]
      fun = fn -> DVBuffer.chunk!(enum, opts) end

      assert_raise ArgumentError, "invalid callback", fun
    end

    test "will raise an error with an invalid limit" do
      opts = [max_length: -5]
      enum = ["foo", "bar", "baz"]
      fun = fn -> DVBuffer.chunk!(enum, opts) end

      assert_raise ArgumentError, "invalid limit", fun
    end
  end

  describe "dump/2" do
    test "will dump an unpartitioned DVBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert DVBuffer.dump(buffer) == {:ok, ["foo", "bar", "baz"]}
      assert {:ok, [%{length: 0}]} = DVBuffer.info(buffer)
    end

    test "will dump a partitioned DVBuffer" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert DVBuffer.dump(buffer) == {:ok, ["foo", "baz", "bar"]}
      assert {:ok, [%{length: 0}, %{length: 0}]} = DVBuffer.info(buffer)
    end

    test "will dump a specific DVBuffer partition" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert DVBuffer.dump(buffer, partition: 0) == {:ok, ["foo", "baz"]}
      assert {:ok, [%{length: 0}]} = DVBuffer.info(buffer, partition: 0)
    end

    test "will return an error with an invalid buffer" do
      assert DVBuffer.dump(:fake_buffer) == {:error, :not_found}
    end

    test "will return an error with an invalid partition" do
      assert {:ok, buffer} = start_ex_buffer()
      assert DVBuffer.dump(buffer, partition: -1) == {:error, :invalid_partition}
    end
  end

  describe "flush/2" do
    test "will flush an unpartitioned DVBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert DVBuffer.flush(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush a partitioned DVBuffer" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert DVBuffer.flush(buffer) == :ok
      assert_receive {^buffer, ["foo", "baz"], _}
      assert_receive {^buffer, ["bar"], _}
    end

    test "will flush a specific DVBuffer partition" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert DVBuffer.flush(buffer, partition: 0) == :ok
      assert_receive {^buffer, ["foo", "baz"], _}
      refute_receive _
    end

    test "will synchronously flush an DVBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert DVBuffer.flush(buffer, async: false) == :ok
      assert_received {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will include flush meta" do
      opts = [flush_meta: "meta"]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert DVBuffer.flush(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], flush_opts}
      assert Keyword.get(flush_opts, :meta) == "meta"
    end

    test "will return an error with an invalid buffer" do
      assert DVBuffer.flush(:fake_buffer) == {:error, :not_found}
    end

    test "will return an error with an invalid partition" do
      assert {:ok, buffer} = start_ex_buffer()
      assert DVBuffer.flush(buffer, partition: -1) == {:error, :invalid_partition}
    end
  end

  describe "info/2" do
    test "will return info for an unpartitioned DVBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert {:ok, [%{length: 3}]} = DVBuffer.info(buffer)
    end

    test "will return info for a partitioned DVBuffer" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert {:ok, [%{length: 2}, %{length: 1}]} = DVBuffer.info(buffer)
    end

    test "will return info for a specific DVBuffer partition" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert {:ok, [%{length: 2}]} = DVBuffer.info(buffer, partition: 0)
    end

    test "will return info for an DVBuffer with a size callback" do
      opts = [size_callback: &(byte_size(&1) + 1)]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert {:ok, [%{size: 12}]} = DVBuffer.info(buffer)
    end

    test "will include next flush when applicable" do
      opts = [buffer_timeout: 1_000]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert {:ok, [%{next_flush: next_flush}]} = DVBuffer.info(buffer)
      refute is_nil(next_flush)
    end

    test "will return an error with an invalid buffer" do
      assert DVBuffer.info(:fake_buffer) == {:error, :not_found}
    end

    test "will return an error with an invalid partition" do
      assert {:ok, buffer} = start_ex_buffer()
      assert DVBuffer.info(buffer, partition: -1) == {:error, :invalid_partition}
    end
  end

  describe "insert/2" do
    test "will insert items into an unpartitioned DVBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert DVBuffer.insert(buffer, "foo") == :ok
      assert DVBuffer.dump(buffer) == {:ok, ["foo"]}
    end

    test "will insert items into a partitioned DVBuffer" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert DVBuffer.insert(buffer, "foo") == :ok
      assert DVBuffer.insert(buffer, "bar") == :ok
      assert DVBuffer.dump(buffer, partition: 0) == {:ok, ["foo"]}
      assert DVBuffer.dump(buffer, partition: 1) == {:ok, ["bar"]}
    end

    test "will insert items into a partitioned DVBuffer with a random partitioner" do
      opts = [partitioner: :random, partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
    end

    test "will flush an DVBuffer based on a length condition" do
      opts = [max_length: 3]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush an DVBuffer based on a size condition" do
      opts = [max_size: 9]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush an DVBuffer with a size callback based on a size condition" do
      opts = [max_size: 12, size_callback: &(byte_size(&1) + 1)]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush an DVBuffer based on a time condition" do
      opts = [buffer_timeout: 50]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok

      :timer.sleep(50)

      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush an DVBuffer once the first condition is met" do
      opts = [max_length: 5, max_size: 9]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush a DVBuffer partitions independently" do
      opts = [max_length: 2, partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "baz"], _}
      assert {:ok, [%{length: 1}]} = DVBuffer.info(buffer, partition: 1)
    end

    test "will include flush meta when flushed" do
      opts = [flush_meta: "meta", max_length: 3]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], flush_opts}
      assert Keyword.get(flush_opts, :meta) == "meta"
    end

    test "will return an error with an invalid buffer" do
      assert DVBuffer.insert(:fake_buffer, "foo") == {:error, :not_found}
    end
  end

  describe "insert_batch/3" do
    test "will insert a batch of items into an unpartitioned DVBuffer" do
      items = ["foo", "bar", "baz"]

      assert {:ok, buffer} = start_ex_buffer()
      assert DVBuffer.insert_batch(buffer, items) == :ok
      assert DVBuffer.dump(buffer) == {:ok, ["foo", "bar", "baz"]}
    end

    test "will insert a batch of items into a partitioned DVBuffer" do
      opts = [partitions: 2]
      items = ["foo", "bar", "baz"]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert DVBuffer.insert_batch(buffer, items) == :ok
      assert DVBuffer.dump(buffer, partition: 0) == {:ok, ["foo", "bar", "baz"]}
    end

    test "will flush an DVBuffer while inserting a batch of items" do
      opts = [max_length: 2]
      items = ["foo", "bar", "baz"]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert DVBuffer.insert_batch(buffer, items) == :ok
      assert_receive {^buffer, ["foo", "bar"], _}
      assert DVBuffer.dump(buffer) == {:ok, ["baz"]}
    end

    test "will flush an DVBuffer unsafely" do
      opts = [max_length: 2]
      items = ["foo", "bar", "baz"]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert DVBuffer.insert_batch(buffer, items, safe_flush: false) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end
  end
end
