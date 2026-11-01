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
log_dir="${LOG_DIR:?new log directory required}"
base_head="${BASE_HEAD:?restore head required}"
product_head=8e9c35cc53a44dc0612e423e6f49b802d56d256f
export PATH="/home/xuanwo/.cargo/bin:$PATH"
export CARGO_BUILD_JOBS=4
test ! -e "$log_dir"
mkdir -p "$log_dir"
cd "$repo_root"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
finish() {
    local result=$? restore_status
    trap - EXIT
    set +e
    git rev-parse HEAD HEAD^{tree} > "$log_dir/final-source-head.txt"
    git status --porcelain --untracked-files=all > "$log_dir/final-source-status.txt"
    if [ ! -s "$log_dir/final-source-status.txt" ]; then
        git switch --detach "$base_head" > "$log_dir/restore.log" 2>&1
        restore_status=$?
        printf '%s\n' "$restore_status" > "$log_dir/restore.status"
        if [ "$restore_status" -ne 0 ]; then result=2; fi
    else
        printf 'not attempted: preserve uncommitted task output\n' > "$log_dir/restore.status"
        result=2
    fi
    git rev-parse HEAD HEAD^{tree} > "$log_dir/restored-head.txt"
    git status --porcelain --untracked-files=all > "$log_dir/restored-status.txt"
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
    test "$stage_status" -eq 0
}
git rev-parse HEAD HEAD^{tree} > "$log_dir/carrier-head.txt"
git merge-base --is-ancestor "$product_head" HEAD
git diff --quiet "$product_head" -- core/core core/services core/src core/Cargo.toml core/Cargo.lock
sha256sum dev/task_pr6531_opendal_check.sh > "$log_dir/driver.sha256"
git status --porcelain --untracked-files=all > "$log_dir/pre-status.txt"
test ! -s "$log_dir/pre-status.txt"
cargo --version > "$log_dir/toolchain.txt"
rustc --version >> "$log_dir/toolchain.txt"
run_stage fmt cargo fmt --manifest-path core/Cargo.toml --all -- --check
run_stage compile cargo check --offline --locked --manifest-path core/Cargo.toml --example task_pr6531_opendal --features services-s3
run_stage diff-check git diff --check
git status --porcelain --untracked-files=all > "$log_dir/validated-status.txt"
test ! -s "$log_dir/validated-status.txt"
