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

ruby_abi = RUBY_VERSION.split(".").first(2).join(".")
gem_format = ENV.fetch("OPENDAL_RUBY_GEM_FORMAT")
requirement = case gem_format
when "conventional"
  Gem::Requirement.new(">= 3.3", "< 4.1.dev")
when "content-addressable"
  Gem::Requirement.new("~> #{ruby_abi}.0")
else
  abort "Unknown native gem format: #{gem_format}"
end
platform = Gem::Platform.new(ENV.fetch("OPENDAL_RUBY_PLATFORM", Gem::Platform.local.to_s))
binding_directory = File.expand_path("../..", __dir__)
source_spec = Gem::Specification.load(File.join(binding_directory, "opendal.gemspec"))
gem_files = Dir.glob(File.join(binding_directory, "pkg", "#{source_spec.name}-#{source_spec.version}-*.gem")).select do |path|
  spec = Gem::Package.new(path).spec
  spec.platform == platform && spec.required_ruby_version == requirement
end
abort "Expected one #{gem_format} gem for #{platform} / Ruby #{ruby_abi}, found #{gem_files.size}" unless gem_files.one?

Dir.mktmpdir("opendal-gem-smoke-") do |gem_home|
  environment = {
    "GEM_HOME" => gem_home,
    "GEM_PATH" => gem_home,
    "RUBYOPT" => nil,
    "RUBYLIB" => nil,
    "BUNDLE_GEMFILE" => nil
  }
  system(environment, Gem.ruby, File.join(RbConfig::CONFIG.fetch("bindir"), "gem"),
    "install", "--local", gem_files.first, "--no-document", exception: true)
  system(environment, Gem.ruby, "-e", <<~'RUBY', gem_files.first, exception: true)
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

    puts "#{ENV.fetch("OPENDAL_RUBY_GEM_FORMAT")} gem passed on #{installed.platform} / Ruby #{RUBY_VERSION}"
  RUBY
end
