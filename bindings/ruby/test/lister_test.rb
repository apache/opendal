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

class ListerTest < ActiveSupport::TestCase
  setup do
    @root = Dir.mktmpdir
    File.write("#{@root}/sample", "Sample data for testing")
    Dir.mkdir("#{@root}/sub")
    File.write("#{@root}/sub/sample", "Sample data for testing")
    @op = OpenDal::Operator.new("fs", {"root" => @root})
  end

  test "lists the directory" do
    lister = @op.list("")

    lists = lister.map(&:to_h).map { |e| e[:path] }.sort

    assert_equal ["/", "sample", "sub/"], lists
  end

  test "list returns the entry" do
    entry = @op.list("/").first

    assert entry.is_a?(OpenDal::Entry)
    assert entry.name.length > 0
  end

  test "entry has the metadata" do
    metadata = @op.list("sample").first.metadata

    assert metadata.file?
    assert !metadata.dir?
  end

  test "lists the directory recursively" do
    lister = @op.list("", recursive: true)

    lists = lister.map(&:to_h).map { |e| e[:path] }.sort

    assert_equal ["/", "sample", "sub/", "sub/sample"], lists
  end

  test "lists the directory with limit" do
    lister = @op.list("", limit: 1)

    lists = lister.map(&:to_h).map { |e| e[:path] }.sort

    assert_equal ["/", "sample", "sub/"], lists
  end

  test "lists the directory with start_after" do
    lister = @op.list("", start_after: "sample")

    lists = lister.map(&:to_h).map { |e| e[:path] }

    assert_equal ["sub/"], lists
  end

  test "#each without a block returns an Enumerator" do
    enum = @op.list("").each

    assert_kind_of Enumerator, enum

    lists = enum.map { |e| e.to_h[:path] }.sort

    assert_equal ["/", "sample", "sub/"], lists
  end

  test "#each returns the break value and then the lister" do
    lister = @op.list("")

    result = lister.each { |e| break e }
    assert_kind_of OpenDal::Entry, result

    assert_same lister, lister.each { |e| e }
  end

  test "#each raises instead of truncating the listing on storage errors" do
    skip "chmod does not restrict root" if Process.euid.zero?

    locked = "#{@root}/locked"
    Dir.mkdir(locked)
    File.write("#{locked}/sample", "unreadable")
    File.chmod(0o000, locked)

    begin
      lister = @op.list("locked/")
      err = assert_raise(RuntimeError) { lister.to_a }
      assert_match(/PermissionDenied/, err.message)

      # The same failure surfaces through the enumerator returned by #each.
      lister = @op.list("locked/")
      assert_raise(RuntimeError) { lister.each.next }
    ensure
      File.chmod(0o755, locked) if File.exist?(locked)
    end
  end
end
