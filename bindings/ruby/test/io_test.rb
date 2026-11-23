# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# frozen_string_literal: true

require "test_helper"
require "tmpdir"

class IOTest < ActiveSupport::TestCase
  setup do
    @root = Dir.mktmpdir
    File.write(
      "#{@root}/sample",
      <<~EOF
        Sample data for testing
        A line after title
      EOF
    )
    @op = OpenDal::Operator.new("fs", {"root" => @root})
    @io_read = @op.open("sample", "r")
    @io_write = @op.open("sample_write", "w")
  end

  teardown do
    @io_read.close
    @io_write.close
    FileUtils.remove_entry(@root) if File.exist?(@root)
  end

  test "#binmode? returns" do
    assert_not @io_read.binmode?
  end

  test "#binmode returns" do
    assert_nothing_raised do
      @io_read.binmode
    end
  end

  test "#close closes IO" do
    assert_nothing_raised do
      @io_read.close
    end
  end

  test "#close_read closes reader" do
    assert_nothing_raised do
      @io_read.close_read
    end
  end

  test "#close_write closes writer" do
    assert_nothing_raised do
      @io_read.close_write
    end
  end

  test "#closed? returns" do
    assert_not @io_read.closed?
  end

  test "#closed_read? returns" do
    assert_not @io_read.closed_read?
  end

  test "#closed_write? returns" do
    assert @io_read.closed_write?
  end

  test "#read reads" do
    result = @io_read.read

    assert_equal "Sample data for testing\nA line after title\n", result
    # should be `assert_equal Encoding::UTF_8, result.encoding`
  end

  test "#read reads with nil" do
    result = @io_read.read(nil)

    assert_equal "Sample data for testing\nA line after title\n", result
  end

  test "#read reads with size" do
    result = @io_read.read(6)

    assert_equal "Sample", result
  end

  test "#read with a huge size returns remaining bytes without over-allocating" do
    result = @io_read.read(2**62)

    assert_equal "Sample data for testing\nA line after title\n", result
    assert_nil @io_read.read(2**62)
  end

  test "#read raises ArgumentError with -1" do
    err = assert_raise(ArgumentError) { @io_read.read(-1) }

    assert_equal "negative length -1 given", err.message
  end

  test "#read with zero size returns an empty string without moving the stream" do
    buffer = +"héllo"

    assert_equal "", @io_read.read(0)
    result = @io_read.read(0, buffer)
    assert_equal "", result
    assert_same buffer, result
    assert_equal Encoding::UTF_8, result.encoding
    assert_equal Encoding::UTF_8, buffer.encoding
    assert_equal 0, @io_read.pos
    assert_equal "Sample", @io_read.read(6)

    @io_read.read
    assert @io_read.eof?

    buffer = +"héllo"
    result = @io_read.read(0, buffer)
    assert_equal "", result
    assert_same buffer, result
    assert_equal Encoding::UTF_8, result.encoding
    assert_equal 43, @io_read.pos
  end

  test "#read with zero size raises on a closed stream" do
    @io_read.close

    assert_raise(RuntimeError) { @io_read.read(0) }
  end

  test "#read with zero size raises on a write-only stream" do
    assert_raise(RuntimeError) { @io_write.read(0) }
  end

  test "#read reads to a buffer" do
    buffer = +"hi " # unfreezes string literal
    result = @io_read.read(6, buffer)

    assert_equal "Sample", result
    assert_equal "hi Sample", buffer
  end

  test "#read with a buffer clears the buffer at EOF" do
    @io_read.read

    buffer = +"stale"
    result = @io_read.read(10, buffer)

    assert_nil result
    assert_equal "", buffer
  end

  test "#write writes" do
    @io_write.write("This is a sentence.")
    @io_write.close
    assert_equal "This is a sentence.", File.read("#{@root}/sample_write")
  end

  test "#write and #read round-trip binary data" do
    payload = "\xff\xfe\x00\x01".b

    written = @io_write.write(payload)
    @io_write.close
    assert_equal payload.bytesize, written

    reader = @op.open("sample_write", "r")
    data = reader.read
    reader.close
    assert_equal payload, data
    assert_equal Encoding::ASCII_8BIT, data.encoding

    dst = @op.open("sample_write_copy", "w")
    dst.write(data)
    dst.close
    assert_equal payload, @op.read("sample_write_copy")
  end

  test "open in append mode appends to existing file" do
    @op.write("log", "previous")

    io = @op.open("log", "a")
    io.write("new")
    io.close

    assert_equal "previousnew", @op.read("log")
  end

  test "open in append mode accepts integer flags" do
    @op.write("log", "previous")

    io = @op.open("log", File::WRONLY | File::APPEND)
    io.write("new")
    io.close

    assert_equal "previousnew", @op.read("log")
  end

  test "open in append mode creates missing file" do
    io = @op.open("created", "a")
    io.write("data")
    io.close

    assert_equal "data", @op.read("created")
  end

  test "open in append mode raises Unsupported without append capability" do
    op = OpenDal::Operator.new("memory", {})
    assert_not op.capability.write_can_append

    op.write("log", "previous")

    err = assert_raise(RuntimeError) { op.open("log", "a") }
    assert_match(/Unsupported/, err.message)
    assert_equal "previous", op.read("log")
  end

  test "open in exclusive mode fails when file exists" do
    @op.write("log", "previous")

    err = assert_raise(RuntimeError) do
      io = @op.open("log", "wx")
      io.write("new")
      io.close
    end

    assert_match(/ConditionNotMatch/, err.message)
    assert_equal "previous", @op.read("log")
  end

  test "open in exclusive mode creates missing file" do
    io = @op.open("created", "wx")
    io.write("data")
    io.close

    assert_equal "data", @op.read("created")
  end

  test "open in read-write mode raises ArgumentError" do
    assert_raise(ArgumentError) { @op.open("sample", "r+") }
    assert_raise(ArgumentError) { @op.open("sample", "w+") }
    assert_raise(ArgumentError) { @op.open("sample", "a+") }
  end

  test "open in append mode is not supported for reading" do
    @op.write("log", "previous")

    io = @op.open("log", "a")
    err = assert_raise(RuntimeError) { io.read }
    io.close

    assert_match(/write-only/, err.message)
  end

  test "#readline reads a line" do
    line = @io_read.readline

    assert_equal "Sample data for testing\n", line
    # should be `assert_equal Encoding::UTF_8, line.encoding`
  end

  test "#readline reads binary lines and raises EOFError at EOF" do
    File.binwrite("#{@root}/binary_lines", "ab\xff\xfe\ncd\n".b)
    io = @op.open("binary_lines", "r")

    line = io.readline
    assert_equal "ab\xff\xfe\n".b, line
    assert_equal Encoding::ASCII_8BIT, line.encoding
    assert_equal "cd\n".b, io.readline
    assert_raise(EOFError) { io.readline }
    io.close
  end

  test "#readlines reads all lines" do
    lines = @io_read.readlines

    assert_equal ["Sample data for testing\n", "A line after title\n"], lines
    # should be `assert_equal Encoding::UTF_8, lines.first.encoding`
  end

  test "#tell returns position" do
    assert_equal 0, @io_read.tell
  end

  test "#seek raises on underflow and overflow" do
    i64_min = -(1 << 63)
    i64_max = (1 << 63) - 1

    err = assert_raise(RuntimeError) { @io_read.seek(i64_min, IO::SEEK_CUR) }
    assert_match(/before start/, err.message)
    assert_equal 0, @io_read.tell

    @io_read.seek(5, IO::SEEK_SET)
    err = assert_raise(RuntimeError) { @io_read.seek(i64_min, IO::SEEK_END) }
    assert_match(/before start/, err.message)
    assert_equal 5, @io_read.tell

    err = assert_raise(RuntimeError) { @io_read.seek(i64_max, IO::SEEK_END) }
    assert_match(/overflow/i, err.message)
    assert_equal 5, @io_read.tell

    @io_read.seek(2**62, IO::SEEK_SET)
    assert_equal 2**62, @io_read.tell
    err = assert_raise(RuntimeError) { @io_read.seek(2**62, IO::SEEK_CUR) }
    assert_match(/overflow/i, err.message)
    assert_equal 2**62, @io_read.tell
  end

  test "#pos= moves position" do
    @io_read.pos = 5

    assert_equal 5, @io_read.pos
  end

  test "#rewind moves position to start" do
    @io_read.pos = 5
    @io_read.rewind

    assert_equal 0, @io_read.pos
  end

  test "#eof returns" do
    assert_not @io_read.eof
  end

  test "#eof? returns" do
    @io_read.read
    assert @io_read.eof?
  end

  test "#eof? preserves the stream position" do
    @io_read.read(1)

    assert_not @io_read.eof?
    assert_equal 1, @io_read.pos
    assert_equal "ample data for testing\nA line after title\n", @io_read.read
    assert @io_read.eof?
  end

  test "#length returns" do
    assert_equal 43, @io_read.length
  end

  test "#size returns" do
    assert_equal 43, @io_read.size
  end
end
