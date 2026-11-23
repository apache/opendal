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

require "digest"
require "minitest/autorun"
require "rbconfig"
require "rubygems/package"
require "tmpdir"

class NativeGemTest < Minitest::Test
  def setup
    @source_spec = Gem::Specification.load("opendal.gemspec")
    @platform = Gem::Platform.new(ENV.fetch("OPENDAL_RUBY_PLATFORM", Gem::Platform.local.to_s))
    @packages = Dir.glob("pkg/#{@source_spec.name}-#{@source_spec.version}-*.gem").map do |path|
      Gem::Package.new(path)
    end.select { |package| package.spec.platform == @platform }
    @requirements = {
      ">= 3.3, < 4.1.dev" => %w[3.3 3.4 4.0],
      "~> 3.3.0" => %w[3.3],
      "~> 3.4.0" => %w[3.4],
      "~> 4.0.0" => %w[4.0]
    }

    assert_equal 4, @packages.size, "Build the native gems with rake native:opendal first"
  end

  def test_compatibility_metadata
    assert_equal @requirements.keys.sort, @packages.map { |package| package.spec.required_ruby_version.to_s }.sort

    @packages.each do |package|
      spec = package.spec
      assert_equal @source_spec.name, spec.name
      assert_equal @source_spec.version, spec.version
      assert_empty spec.extensions
      assert_empty spec.requirements
      assert spec.required_rubygems_version.satisfied_by?(Gem::Version.new("4.1.0"))
      assert_equal spec.required_ruby_version == Gem::Requirement.new(">= 3.3", "< 4.1.dev"),
        spec.required_rubygems_version.satisfied_by?(Gem::Version.new("4.0.0"))
    end
  end

  def test_runtime_and_legal_files_without_rust_build_inputs
    required_files = @source_spec.files.grep(%r{\Alib/.*\.rb\z})
    required_files.concat(Dir.glob("{LICENSE*,NOTICE*,DEPENDENCIES*}").select { |path| File.file?(path) })
    refute_empty required_files.grep(/LICENSE/)
    refute_empty required_files.grep(/NOTICE/)

    @packages.each do |package|
      assert_empty required_files - package.contents
      assert_empty package.contents.grep(%r{\A(?:src/|core/|Cargo\.toml\z|Cargo\.lock\z|build\.rs\z)})
    end
  end

  def test_each_gem_contains_its_compiled_extensions
    extension_file = "opendal_ruby.#{RbConfig::CONFIG.fetch("DLEXT")}"

    @packages.each do |package|
      versions = @requirements.fetch(package.spec.required_ruby_version.to_s)
      expected_paths = versions.map { |version| "lib/opendal_ruby/#{version}/#{extension_file}" }
      assert_equal expected_paths.sort, package.contents.grep(/\.(?:so|bundle)\z/).sort

      Dir.mktmpdir do |directory|
        package.extract_files(directory)
        versions.zip(expected_paths).each do |version, path|
          assert_equal Digest::SHA256.file("tmp/native/#{version}/#{extension_file}").hexdigest,
            Digest::SHA256.file(File.join(directory, path)).hexdigest
        end
      end
    end
  end
end
