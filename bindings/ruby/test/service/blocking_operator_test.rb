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

SERVICE = ENV["OPENDAL_TEST"]

module Service
  class BlockingOperatorTest < ActiveSupport::TestCase
    setup do
      config = config_from_env(SERVICE)

      @op = OpenDal::Operator.new(SERVICE, config)
    end

    test "read and write text" do
      @op.write("content", "OpenDAL Ruby is ready.")

      content = @op.read("content")

      assert_equal "OpenDAL Ruby is ready.", content
    end

    test "read and write binary" do
      # writes 32-bit signed integers
      @op.write("binary", [67305985, -50462977].pack("l*"))

      bin = @op.read("binary")

      assert_equal [67305985, -50462977], bin.unpack("l*")
    end

    test "stat returns file metadata" do
      @op.write("test_stat", "test")

      stat = @op.stat("test_stat")

      assert stat.is_a?(OpenDal::Metadata)
      assert_equal 4, stat.content_length
      assert stat.file?
    end

    test "create_dir creates directory" do
      @op.create_dir("new/directory/")

      assert_equal @op.stat("new/directory/").mode, OpenDal::Metadata::DIRECTORY
    end

    test "exists returns existence" do
      @op.write("exist_file", "test")
      @op.create_dir("exist/directory/")

      assert @op.exist?("exist_file")
      assert @op.exist?("exist/directory/")
    end

    test "delete removes file" do
      @op.write("deletion_test", "test")

      assert @op.exist?("deletion_test"), "expect file to exist before deletion"

      @op.delete("deletion_test")
      assert !@op.exist?("/deletion_test"), "expect file not to exist after deletion"
    end

    test "rename renames file" do
      @op.write("rename_test", "test")

      @op.rename("rename_test", "new_name")
      assert !@op.exist?("rename_test"), "expect file not to exist after renaming"
      assert @op.exist?("new_name"), "expect file exist after renaming"
    end

    test "remove_all removes files" do
      @op.create_dir("nested/directory/")
      @op.write("nested/directory/text", "content")

      @op.remove_all("nested")
      assert !@op.exist?("nested/directory/"), "expect file not to exist after removal"
    end

    test "copy copies file" do
      @op.write("copy_test", "test")
      @op.copy("copy_test", "copy_destination")

      assert @op.exist?("copy_test"), "expect source file to exist"
      assert @op.exist?("copy_destination"), "expect destination file to exist"
    end

    test "opens an IO" do
      @op.write("io_test", "test")

      io = @op.open("/io_test", "rb")
      content = io.read

      assert_not io.closed?
      assert_equal "test", content

      io.close
    end

    test "middleware applies a middleware" do
      @op.middleware(OpenDal::Middleware::Retry.new)

      assert @op.is_a?(OpenDal::Operator)
    end

    private
    
    def config_from_env(service)
      prefix = "OPENDAL_#{service.upcase}_"
      config = {}
      ENV.each do |key, value|
        if key.start_with?(prefix)
          config[key[prefix.length..-1].downcase] = value
        end
      end

      disable_random_root = ENV["OPENDAL_DISABLE_RANDOM_ROOT"] == "true"
      unless disable_random_root
        root = config.fetch('root', '/')
        uuid = SecureRandom.uuid
        config["root"] = "#{root}/#{uuid}/"
      end

      config
    end
  end
end
