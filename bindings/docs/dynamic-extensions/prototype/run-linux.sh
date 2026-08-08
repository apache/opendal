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
s3_target="$prototype_dir/target/s3"
timeout_target="$prototype_dir/target/timeout"
host_target="$prototype_dir/target/host"
s3="$s3_target/release/libs3_extension.so"
timeout="$timeout_target/release/libtimeout_extension.so"
host="$host_target/release/host"

CARGO_TARGET_DIR="$s3_target" cargo build --release --locked \
  --manifest-path "$prototype_dir/Cargo.toml" --package s3-extension
CARGO_TARGET_DIR="$timeout_target" cargo build --release --locked \
  --manifest-path "$prototype_dir/Cargo.toml" --package timeout-extension
CARGO_TARGET_DIR="$host_target" cargo build --release --locked \
  --manifest-path "$prototype_dir/Cargo.toml" --package host
"$prototype_dir/audit-elf-exports.sh" \
  "$s3" opendal_service_s3_bootstrap_v1
"$prototype_dir/audit-elf-exports.sh" \
  "$timeout" opendal_layer_timeout_bootstrap_v1
"$host" "$s3" "$timeout"
