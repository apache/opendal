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

task_worktree=$1
task_logs=$2
task_expected_sha=$3
mkdir -p "$task_logs"
cd "$task_worktree/core"
test "$(git rev-parse HEAD)" = "$task_expected_sha"
test -z "$(git status --porcelain --untracked-files=no)"
git rev-parse HEAD > "$task_logs/tested-sha.txt"

exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
export CARGO_BUILD_JOBS=4

run_check() {
    local check_name=$1
    shift
    local check_status=0
    {
        printf 'sha=%s\n' "$task_expected_sha"
        printf 'command='
        printf '%q ' "$@"
        printf '\n'
    } > "$task_logs/$check_name.log"
    "$@" >> "$task_logs/$check_name.log" 2>&1 || check_status=$?
    printf 'exit=%s\n' "$check_status" >> "$task_logs/$check_name.log"
    printf '%s\n' "$check_status" > "$task_logs/$check_name.status.tmp"
    mv "$task_logs/$check_name.status.tmp" "$task_logs/$check_name.status"
}

run_check tests cargo nextest run -p opendal-layer-retry
run_check clippy cargo clippy -p opendal-layer-retry --all-targets -- -D warnings
run_check fmt cargo fmt --all -- --check

task_status=0
for check_name in tests clippy fmt; do
    test "$(cat "$task_logs/$check_name.status")" = 0 || task_status=1
done
test "$(git rev-parse HEAD)" = "$task_expected_sha" || task_status=1
printf '%s\n' "$task_status" > "$task_logs/done.status.tmp"
mv "$task_logs/done.status.tmp" "$task_logs/done.status"
exit "$task_status"
