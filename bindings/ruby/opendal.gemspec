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

tracked_files_or_glob = lambda do |dir|
  git_files = begin
    IO.popen(["git", "-C", dir, "ls-files", "-z"], err: File::NULL, &:read).split("\x0")
  rescue Errno::ENOENT
    []
  end

  if git_files.empty?
    Dir.chdir(dir) do
      Dir.glob("**/*", File::FNM_DOTMATCH).reject do |f|
        File.directory?(f)
      end
    end
  else
    git_files
  end
end

Gem::Specification.new do |spec|
  spec.name = "opendal"
  # RubyGems integrates and expects `cargo`.
  # Read more about
  # [Gem::Ext::CargoBuilder](https://github.com/rubygems/rubygems/blob/v3.5.23/lib/rubygems/ext/cargo_builder.rb)
  #
  # OpenDAL relies on "version" in `Cargo.toml` for the release process. You can read this gem spec with:
  # `bundle exec ruby -e 'puts Gem::Specification.load("opendal.gemspec")'`
  #
  # Read from Cargo.toml directly so Bundler can evaluate this gemspec from an
  # unpacked native gem without requiring the Rust workspace or network access.
  cargo_toml = File.read(File.join(__dir__, "Cargo.toml"))
  spec.version = cargo_toml.match(/^\s*version\s*=\s*"([^"]+)"/)[1]
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
  # Prefer git-tracked files while building from a checkout, and fall back to
  # the unpacked files when Bundler loads this gemspec from an installed gem.
  spec.files = Dir.chdir(__dir__) do
    git_files = tracked_files_or_glob.call(__dir__).reject do |f|
      f.start_with?(*%w[.bundle/ gems/ pkg/ target/ tmp/ .git/ vendor/]) ||
        f.end_with?(*%w[.log .tmp .bak]) ||
        (f.end_with?(".lock") && f != "Cargo.lock") ||
        f == "."
    end
    git_files |= ["Cargo.lock"] if File.file?("Cargo.lock")

    # When building release package, include core directory files for rake build
    distributed_core_dir = "core"

    if Dir.exist?(distributed_core_dir)
      # Core files should already be copied by the Rakefile's copy_core task
      core_files = tracked_files_or_glob.call(File.expand_path(distributed_core_dir, __dir__)).map do |f|
        "#{distributed_core_dir}/#{f}"
      end

      git_files + core_files
    else
      git_files
    end
  end

  spec.require_paths = ["lib"]

  # A development build writes an unversioned library into lib/. The native
  # packaging task adds versioned libraries to the platform gem instead.
  spec.files -= %w[lib/opendal_ruby.so lib/opendal_ruby.bundle]
  native_extension = spec.files.any? { |path| path.match?(%r{\Alib/opendal_ruby/\d+\.\d+/opendal_ruby\.(so|bundle)\z}) }
  spec.extensions = native_extension ? [] : ["./Cargo.toml"]

  # Exclude non-Ruby files from RDoc to prevent parsing errors
  spec.rdoc_options = ["--exclude", "Cargo\\..*", "--exclude", "core/", "--exclude", "\\.rs$"]

  spec.requirements = ["Rust >= 1.91"]
  # use a Ruby version which:
  # - supports Rubygems with the ability of compilation of Rust gem
  # - not end of life
  spec.required_ruby_version = ">= 3.3"

  # intentionally skipping rb_sys gem because newer Rubygems will be present
end
