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
  spec.version = JSON.parse(`cargo metadata --format-version 1`.strip)["packages"].find { |p| p["name"] == "opendal-ruby" }["version"]
  spec.authors = ["OpenDAL Contributors"]
  spec.email = ["dev@opendal.apache.org"]

  spec.summary = "OpenDAL Ruby Binding"
  spec.homepage = "https://opendal.apache.org/"

  spec.metadata["allowed_push_host"] = "TODO: Set to your gem server 'https://example.com'"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/apache/opendal"
  spec.metadata["changelog_uri"] = "https://github.com/apache/opendal/releases"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").reject do |f|
      (File.expand_path(f) == __FILE__) || f.start_with?(*%w[bin/ test/ spec/ features/ .git .circleci appveyor])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.extensions = ["./extconf.rb"]

  # Rubygems is a default gem that is a part of Ruby core.
  # Rubygems 3.3.11 supports building gem with Cargo.
  # Read more https://github.com/rubygems/rubygems/blob/master/CHANGELOG.md#3311--2022-04-07
  #
  # Ruby 3.1.3 includes Rubygems 3.3.26
  # Read more https://stdgems.org/3.1.3/
  #
  # use a Ruby version which:
  # - supports new Rubygems with the ability of compilation of Rust gem
  # - not end of life
  spec.required_ruby_version = ">= 3.1.3"

  # intentionally skipping rb_sys gem because newer Rubygems will be present
end
