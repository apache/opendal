#!/usr/bin/env sh
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

set -eu

prototype_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
shared_dir="$prototype_dir/../../python/dynamic-extensions"
target_root="$prototype_dir/target"
runtime_target="$target_root/shared-runtime"
fs_target="$target_root/shared-fs"
adapter_target="$target_root/ruby-adapter"
stage="$target_root/ruby-stage"

if cargo tree --locked --offline --manifest-path "$prototype_dir/Cargo.toml" \
  --package opendal-ruby-runtime-poc \
  | rg -q 'opendal-core|opendal-service-|tokio'; then
  echo "Ruby adapter unexpectedly links OpenDAL core, a service, or Tokio" >&2
  exit 1
fi

CARGO_TARGET_DIR="$runtime_target" cargo build --release --locked --offline \
  --manifest-path "$shared_dir/Cargo.toml" --package opendal-runtime-poc
CARGO_TARGET_DIR="$fs_target" cargo build --release --locked --offline \
  --manifest-path "$shared_dir/Cargo.toml" --package fs-extension
CARGO_TARGET_DIR="$adapter_target" cargo build --release --locked --offline \
  --manifest-path "$prototype_dir/Cargo.toml" --package opendal-ruby-runtime-poc

runtime="$runtime_target/release/libopendal_runtime_poc.so"
fs="$fs_target/release/libfs_extension.so"
adapter="$adapter_target/release/libopendal_ruby_poc.so"

"$shared_dir/audit-elf-exports.sh" "$runtime" opendal_runtime_get_api_v1
"$shared_dir/audit-elf-exports.sh" "$fs" opendal_service_fs_bootstrap_v1
"$shared_dir/audit-elf-exports.sh" \
  "$adapter" Init_opendal_ruby_poc ruby_abi_version

mkdir -p \
  "$stage/main/lib/opendal/_native" \
  "$stage/fs/lib/opendal/services/fs/_native" \
  "$stage/python/main/opendal/_native" \
  "$stage/python/fs/opendal/services/fs/_native"
cp -R "$prototype_dir/ruby/main/." "$stage/main/"
cp -R "$prototype_dir/ruby/service-fs/." "$stage/fs/"
cp "$runtime" "$stage/main/lib/opendal/_native/"
cp "$adapter" "$stage/main/lib/opendal_ruby_poc.so"
cp "$fs" "$stage/fs/lib/opendal/services/fs/_native/"
cp -R "$shared_dir/python/main/." "$stage/python/main/"
cp -R "$shared_dir/python/service-fs/." "$stage/python/fs/"
cp "$runtime" "$stage/python/main/opendal/_native/"
cp "$fs" "$stage/python/fs/opendal/services/fs/_native/"

RUBYLIB="$stage/main/lib:$stage/fs/lib" \
OPENDAL_RUBY_POC_MAIN="$stage/main/lib" \
  ruby "$prototype_dir/ruby/test_poc.rb"
RUBYLIB="$stage/main/lib:$stage/fs/lib" ruby "$prototype_dir/ruby/example.rb"
PYTHONPATH="$stage/python/main:$stage/python/fs" \
  python3 "$prototype_dir/ruby/python_cross_check.py"
