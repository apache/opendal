# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# frozen_string_literal: true

require "open3"
require "rbconfig"
require "tmpdir"
require "opendal"

def assert(condition, message)
  raise message unless condition
end

def loaded?(basename)
  File.read("/proc/self/maps").include?(basename)
end

assert(OpenDal::Runtime.minimum_runtime_protocol == 1, "unexpected minimum protocol")
assert(OpenDal::Runtime.runtime_protocol == 1, "unexpected current protocol")
assert(!loaded?("libfs_extension.so"), "FS loaded before package registration")

begin
  OpenDal::Operator.new("fs", {})
  raise "unregistered FS construction succeeded"
rescue RuntimeError => error
  assert(error.message.include?("not registered"), "unexpected registration error")
end

require "opendal/services/fs"
assert(!loaded?("libfs_extension.so"), "FS loaded during package registration")

Dir.mktmpdir("opendal-ruby-runtime-poc-") do |root|
  operator = OpenDal::Operator.new("fs", {"root" => root})
  assert(loaded?("libfs_extension.so"), "FS did not load during construction")
  payload = "ruby-runtime-poc-" * 512
  operator.write("hello.txt", payload)
  assert(operator.read("hello.txt") == payload, "FS round trip failed")
  assert(operator.info.fetch("scheme") == "fs", "unexpected operator scheme")
  operator.close

  begin
    operator.read("hello.txt")
    raise "closed operator remained usable"
  rescue RuntimeError => error
    assert(error.message == "operator is closed", "unexpected closed-handle error")
  end
end

main = ENV.fetch("OPENDAL_RUBY_POC_MAIN")
env = {"OPENDAL_POC_REQUIRED_RUNTIME_PROTOCOL" => "2"}
_, stderr, status = Open3.capture3(
  env,
  RbConfig.ruby,
  "-I#{main}",
  "-ropendal",
  "-e",
  "abort 'incompatible runtime unexpectedly loaded'"
)
assert(!status.success?, "newer binding protocol unexpectedly loaded")
assert(stderr.include?("runtime protocol negotiation failed"), "missing protocol diagnostic")

puts({
  adapter: "Magnus",
  runtime_protocol: OpenDal::Runtime.runtime_protocol,
  fs_lazy_load: true,
  fs_round_trip: true,
  closed_handle_rejected: true,
  newer_protocol_rejected: true
}.inspect)
