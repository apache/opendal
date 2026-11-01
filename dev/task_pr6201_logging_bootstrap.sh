#!/usr/bin/env bash
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

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
log_dir="${LOG_DIR:?a new log directory is required}"
product_head=8ae286dbe0ade959b835c8d7a5b8360e4d170130
export CARGO_BUILD_JOBS=4
export PATH="/home/xuanwo/.cargo/bin:$PATH"
test ! -e "$log_dir"
mkdir -p "$log_dir"
cd "$repo_root"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
finish() {
    local result=$?
    trap - EXIT
    set +e
    git rev-parse HEAD HEAD^{tree} > "$log_dir/final-head.txt"
    git status --porcelain --untracked-files=all > "$log_dir/final-status.txt"
    git diff --stat > "$log_dir/generated-diff-stat.txt"
    printf '%s\n' "$result" > "$log_dir/driver.exit"
    exit "$result"
}
trap finish EXIT

run_stage() {
    local stage="$1"
    shift
    set +e
    "$@" > "$log_dir/$stage.log" 2>&1
    local stage_status=$?
    set -e
    printf '%s\n' "$stage_status" > "$log_dir/$stage.status"
    return "$stage_status"
}

git status --porcelain --untracked-files=all > "$log_dir/pre-status.txt"
test ! -s "$log_dir/pre-status.txt"
git rev-parse HEAD HEAD^{tree} > "$log_dir/carrier-head.txt"
sha256sum dev/task_pr6201_logging_bootstrap.sh > "$log_dir/driver.sha256"
git switch --detach "$product_head" > "$log_dir/product-checkout.log" 2>&1
cargo --version > "$log_dir/toolchain.txt"
rustc --version >> "$log_dir/toolchain.txt"
run_stage fmt cargo fmt --manifest-path bindings/c/Cargo.toml
run_stage bootstrap-build cargo build --manifest-path bindings/c/Cargo.toml
run_stage exported-symbol bash -c 'set -o pipefail; nm --defined-only -D "$1" | rg " opendal_logging_init$"' bash "$repo_root/bindings/c/target/debug/libopendal_c.so"
sha256sum bindings/c/target/debug/libopendal_c.so > "$log_dir/native-library.sha256"
sha256sum bindings/c/Cargo.lock > "$log_dir/ignored-cargo-lock.sha256"
run_stage diff-check git diff --check
# Keep generated header/format changes for the coordinator's inspection,
# local commit and Git-over-SSH fetch. This bootstrap runs no product tests.
