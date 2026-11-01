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
product_head=5f4699225af61c9040815c5c9d6530b5b70a08bb
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
sha256sum dev/task_pr8350_port_bootstrap.sh > "$log_dir/driver.sha256"
git switch --detach "$product_head" > "$log_dir/product-checkout.log" 2>&1
cargo --version > "$log_dir/toolchain.txt"
rustc --version >> "$log_dir/toolchain.txt"
cd core
run_stage fmt cargo fmt --all
run_stage bootstrap-check cargo check --offline -p opendal-service-smb
cd "$repo_root"
run_stage generate-docs cargo run --quiet --manifest-path dev/Cargo.toml -- generate -l docs
run_stage diff-check git diff --check
# Keep generated source in this worktree for the coordinator's inspection,
# local commit and Git-over-SSH fetch. This bootstrap runs no product tests.
