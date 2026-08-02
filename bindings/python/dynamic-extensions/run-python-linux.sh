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

extension_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
runtime_target="$extension_dir/target/python-runtime"
s3_target="$extension_dir/target/python-s3"
fs_target="$extension_dir/target/python-fs"
stage="$extension_dir/target/python-stage"

CARGO_TARGET_DIR="$runtime_target" cargo build --release --locked \
  --manifest-path "$extension_dir/Cargo.toml" --package opendal-runtime-poc
CARGO_TARGET_DIR="$s3_target" cargo build --release --locked \
  --manifest-path "$extension_dir/Cargo.toml" --package s3-extension
CARGO_TARGET_DIR="$fs_target" cargo build --release --locked \
  --manifest-path "$extension_dir/Cargo.toml" --package fs-extension

runtime="$runtime_target/release/libopendal_runtime_poc.so"
s3="$s3_target/release/libs3_extension.so"
fs="$fs_target/release/libfs_extension.so"

"$extension_dir/audit-elf-exports.sh" "$runtime" opendal_runtime_get_api_v1
"$extension_dir/audit-elf-exports.sh" "$s3" opendal_service_s3_bootstrap_v1
"$extension_dir/audit-elf-exports.sh" "$fs" opendal_service_fs_bootstrap_v1

mkdir -p \
  "$stage/main/opendal/_native" \
  "$stage/s3/opendal/services/s3/_native" \
  "$stage/fs/opendal/services/fs/_native"
cp -R "$extension_dir/python/main/." "$stage/main/"
cp -R "$extension_dir/python/service-s3/." "$stage/s3/"
cp -R "$extension_dir/python/service-fs/." "$stage/fs/"
cp "$runtime" "$stage/main/opendal/_native/"
cp "$s3" "$stage/s3/opendal/services/s3/_native/"
cp "$fs" "$stage/fs/opendal/services/fs/_native/"

PYTHONPATH="$stage/main:$stage/s3:$stage/fs" \
  python3 "$extension_dir/python/test_poc.py"
PYTHONPATH="$stage/main:$stage/s3:$stage/fs" \
  python3 "$extension_dir/python/example.py"
