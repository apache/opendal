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
test "$#" -eq 4
task_repo=$1
task_logs=$2
task_source=$3
task_mode=$4
mkdir -p "$task_logs"
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.before"
git rev-parse HEAD^ > "$task_logs/parent.before"
git rev-parse 'HEAD^{tree}' > "$task_logs/tree.before"
git status --porcelain > "$task_logs/status.before"
test ! -s "$task_logs/status.before"
test "$(cat "$task_logs/parent.before")" = "$task_source"
test "$(git diff --name-only "$task_source" HEAD)" = dev/task_pr7453_validate.sh
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export RUSTUP_TOOLCHAIN=1.99.1
export CARGO_BUILD_JOBS=4
rustc --version > "$task_logs/rustc.version"
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
    if test "$task_status" -ne 0; then task_failed=$task_status; fi
}
if test "$task_mode" = baseline; then
    run_check test cargo test --locked -p opendal-layer-throttle --lib tests::public_memory_rejects_oversized_bounded_reads -- --exact --nocapture
elif test "$task_mode" = head; then
    run_check test cargo test --locked -p opendal-layer-throttle --lib
    run_check doc cargo test --locked -p opendal-layer-throttle --doc
    run_check clippy cargo clippy --locked -p opendal-layer-throttle --all-targets -- -D warnings
    run_check format cargo fmt --all -- --check
elif test "$task_mode" = quality; then
    run_check clippy cargo clippy --locked -p opendal-layer-throttle --all-targets -- -D warnings
    run_check format cargo fmt --all -- --check
else
    exit 2
fi
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.after"
git status --porcelain > "$task_logs/status.after"
test ! -s "$task_logs/status.after" || task_failed=1
printf '%s\n' "$task_failed" > "$task_logs/driver.status"
exit "$task_failed"
