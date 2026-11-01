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
test "$#" -eq 2
task_repo=$1
task_logs=$2
mkdir -p "$task_logs/control"
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.before"
git rev-parse 'HEAD^{tree}' > "$task_logs/tree.before"
git status --porcelain > "$task_logs/status.before"
test ! -s "$task_logs/status.before"
git merge-base --is-ancestor bfddde60f1089262bc0352bcdb64fcb9461e7b27 HEAD
mapfile -t task_paths < <(git diff --name-only bfddde60f1089262bc0352bcdb64fcb9461e7b27 HEAD)
test "${task_paths[*]}" = 'core/tests/task_pr6165_list.rs dev/task_pr6165_validate.sh'
test ! -e "$task_logs/root.redb"
test ! -e "$task_logs/flat.redb"
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export RUSTUP_TOOLCHAIN=1.99.1
export CARGO_BUILD_JOBS=4
rustc --version > "$task_logs/rustc.version"
df -h "$task_repo" > "$task_logs/storage.before"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
cd core
task_failed=0
run_check() {
    local task_name=$1
    shift
    local task_status=0
    "$@" > "$task_logs/$task_name.log" 2>&1 || task_status=$?
    printf '%s\n' "$task_status" > "$task_logs/$task_name.status"
    if test "$task_status" -ne 0; then task_failed=1; fi
}
run_check build cargo test --locked --test behavior --test task_pr6165_list --features tests,services-redb --no-run
test "$(cat "$task_logs/build.status")" -eq 0
sha256sum Cargo.lock > "$task_logs/cargo-lock.sha256"
cargo tree --locked --features tests,services-redb --invert redb --depth 1 > "$task_logs/dependency-tree.log" 2>&1
run_check clippy cargo clippy --locked -p opendal-service-redb --all-targets -- -D warnings
run_check format cargo fmt --all -- --check
unset OPENDAL_TEST_CAPABILITY_OVERRIDES OPENDAL_DISABLE_RANDOM_ROOT
export OPENDAL_TEST=redb
export OPENDAL_REDB_TABLE=listing
export OPENDAL_REDB_ROOT=/tenant/pr6165/
export OPENDAL_REDB_DATADIR="$task_logs/root.redb"
run_check behavior-root cargo test --locked --test behavior --features tests,services-redb
export OPENDAL_DISABLE_RANDOM_ROOT=true
export OPENDAL_REDB_ROOT=/
export OPENDAL_REDB_DATADIR="$task_logs/flat.redb"
run_check list-flat cargo test --locked --test behavior --features tests,services-redb -- test_list_
export OPENDAL_TASK_REDB_CONTROL_DIR="$task_logs/control"
run_check blocking cargo test --locked --test task_pr6165_list --features tests,services-redb -- --nocapture
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.after"
git status --porcelain > "$task_logs/status.after"
test ! -s "$task_logs/status.after" || task_failed=1
if test "$task_failed" -eq 0; then
    rm -- "$task_logs/root.redb" "$task_logs/flat.redb"
    printf '0\n' > "$task_logs/cleanup.status"
fi
printf '%s\n' "$task_failed" > "$task_logs/driver.status"
exit "$task_failed"
