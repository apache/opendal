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
task_expected=9cfbdc40c6e403b86479d9f9955a2f433267bafc
mkdir -p "$task_logs"
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export CARGO_BUILD_JOBS=4
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.before"
git rev-parse 'HEAD^{tree}' > "$task_logs/tree.before"
git status --porcelain > "$task_logs/status.before"
test "$(cat "$task_logs/head.before")" = "$task_expected"
test ! -s "$task_logs/status.before"
cd core
rustc +1.99.1 --version > "$task_logs/rustc.version"
cargo +1.99.1 --version > "$task_logs/cargo.version"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
task_failed=0
run_check() {
    local task_name=$1
    shift
    local task_status=0
    "$@" > "$task_logs/$task_name.log" 2>&1 || task_status=$?
    printf '%s\n' "$task_status" > "$task_logs/$task_name.status"
    if test "$task_status" -ne 0; then task_failed=1; fi
}
run_check headers cargo +1.99.1 test --locked -p opendal-core --features reqsign --lib test_http_send_ -- --nocapture
run_check clippy cargo +1.99.1 clippy --locked -p opendal-core --features reqsign --all-targets -- -D warnings
run_check format cargo +1.99.1 fmt --all -- --check
cd "$task_repo"
run_check tracked-clean git diff --exit-code
git rev-parse HEAD > "$task_logs/head.after"
git rev-parse 'HEAD^{tree}' > "$task_logs/tree.after"
git status --porcelain > "$task_logs/status.after"
test "$(cat "$task_logs/head.after")" = "$task_expected"
test ! -s "$task_logs/status.after"
printf '%s\n' "$task_failed" > "$task_logs/driver.status"
exit "$task_failed"
