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

require "rbconfig"
require "rubygems/package"
require "tmpdir"

package = Gem::Package.new(ARGV.fetch(0))
expected = package.spec
require "opendal"
installed = Gem.loaded_specs.fetch(expected.name)
unless [installed.version, installed.platform, installed.content_address] ==
    [expected.version, expected.platform, package.content_address]
  abort "The loaded gem does not match the selected artifact"
end
unless installed.full_gem_path.start_with?(File.join(ENV.fetch("GEM_HOME"), "gems") + File::SEPARATOR)
  abort "The loaded gem is outside the isolated installation"
end

ruby_abi = RUBY_VERSION.split(".").first(2).join(".")
extension_path = File.join(installed.full_gem_path, "lib", "opendal_ruby", ruby_abi,
  "opendal_ruby.#{RbConfig::CONFIG.fetch("DLEXT")}")
abort "The installed gem did not load the Ruby #{ruby_abi} extension" unless $LOADED_FEATURES.include?(extension_path)

Dir.mktmpdir do |root|
  op = OpenDal::Operator.new("fs", {"root" => root})
  op.write("hello.txt", "hello")
  raise "bad read on Ruby #{RUBY_VERSION}" unless op.read("hello.txt") == "hello"
  expected_time = Time.utc(2024, 1, 2, 3, 4, 5, 125_000)
  File.utime(expected_time, expected_time, File.join(root, "hello.txt"))
  raise "bad last_modified on Ruby #{RUBY_VERSION}" unless op.stat("hello.txt").last_modified == expected_time
end

puts "#{File.basename(ARGV.fetch(0))} passed on #{installed.platform} / Ruby #{RUBY_VERSION}"
