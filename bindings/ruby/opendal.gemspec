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

require "json"

Gem::Specification.new do |spec|
  spec.name = "opendal"
  # RubyGems integrates and expects `cargo`.
  # Read more about [Gem::Ext::CargoBuilder](https://github.com/rubygems/rubygems/blob/v3.5.23/lib/rubygems/ext/cargo_builder.rb)
  #
  # OpenDAL relies on "version" in `Cargo.toml` for the release process. You can read this gem spec with:
  # `bundle exec ruby -e 'puts Gem::Specification.load("opendal.gemspec")'`
  #
  # keep in sync the key "opendal-ruby" with `Rakefile`.
  #
  # uses `cargo` to extract the version.
  spec.version = JSON.parse(`cargo metadata --format-version 1`.strip)
    .fetch("packages")
    .find { |p| p["name"] == "opendal-ruby" }
    .fetch("version")
  spec.authors = ["OpenDAL Contributors"]
  spec.email = ["dev@opendal.apache.org"]

  spec.summary = "OpenDAL Ruby Binding"
  spec.homepage = "https://opendal.apache.org/"
  spec.license = "Apache-2.0"

  spec.metadata = {
    "bug_tracker_uri" => "https://github.com/apache/opendal/issues",
    "changelog_uri" => "https://github.com/apache/opendal/releases",
    "documentation_uri" => "https://opendal.apache.org/docs/ruby/",
    "homepage_uri" => spec.homepage,
    "source_code_uri" => "https://github.com/apache/opendal",
    "rubygems_mfa_required" => "true"
  }

  # Specify which files should be added to a source release gem when we release OpenDAL Ruby gem.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(__dir__) do
    git_files = `git ls-files -z`.split("\x0").reject do |f|
      (File.expand_path(f) == __FILE__) || f.start_with?(*%w[gems/ pkg/ target/ tmp/ .git]) || f == "core"
    end

    # Copy core directory
    src = '../../core'
    dst = './core'
    `cp -RL #{src} #{dst}`

    # Include core directory files, excluding symlinks
    core_files = Dir.chdir("./core") do
      `git ls-files -z`.split("\x0").reject do |f|
        File.symlink?(File.join("./core", f))
      end.map { |f| "core/#{f}" }
    end

    # Resolve symlinks: copy actual files from their target locations
    # This handles recursive symbol link cases. e.g., core/CHANGELOG.md -> ../CHANGELOG.md
    symlink_targets = Dir.chdir("./core") do
      `git ls-files -z`.split("\x0").select do |f|
        File.symlink?(File.join("./core", f))
      end.filter_map do |f|
        link_target = File.readlink(File.join("./core", f))
        resolved_path = File.expand_path(link_target, File.join(__dir__, "core"))
        File.exist?(resolved_path) ? "core/#{f}" : nil
      end
    end

    git_files + core_files + symlink_targets
  end

  spec.require_paths = ["lib"]

  spec.extensions = ["./extconf.rb"]

  spec.requirements = ["Rust >= 1.82"]
  # use a Ruby version which:
  # - supports Rubygems with the ability of compilation of Rust gem
  # - not end of life
  #
  # keep in sync with `Rakefile`.
  spec.required_ruby_version = ">= 3.2"

  # intentionally skipping rb_sys gem because newer Rubygems will be present
end
