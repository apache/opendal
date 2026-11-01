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

# Task-only review validation for the Go CGO environment correction.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
product_head="${PRODUCT_HEAD:?exact product head required}"
base_head="${BASE_HEAD:?original worktree head required}"
log_dir="${LOG_DIR:?unique log directory required}"
mkdir -p "$log_dir"
cd "$repo_root"
finish() {
    local result=$?
    trap - EXIT
    set +e
    git switch --detach "$base_head" > "$log_dir/restore.log" 2>&1
    local status=$?
    printf '%s\n' "$status" > "$log_dir/restore.status"
    git rev-parse HEAD HEAD^{tree} > "$log_dir/restored-head.txt"
    git status --porcelain --untracked-files=all > "$log_dir/restored-status.txt"
    if [ "$status" -ne 0 ] || [ -s "$log_dir/restored-status.txt" ]; then result=2; fi
    printf '%s\n' "$result" > "$log_dir/driver.exit"
    exit "$result"
}
trap finish EXIT
git switch --detach "$product_head" > "$log_dir/product-checkout.log" 2>&1
git rev-parse HEAD HEAD^{tree} > "$log_dir/source-head.txt"
git status --porcelain --untracked-files=all > "$log_dir/source-status.txt"
test ! -s "$log_dir/source-status.txt"
export GOTOOLCHAIN=local
go version > "$log_dir/toolchain.txt"
cd bindings/go
env -u CGO_ENABLED CGO_ENABLE=0 go env CGO_ENABLED > "$log_dir/legacy-cgo-env.txt"
CGO_ENABLED=0 go env CGO_ENABLED > "$log_dir/correct-cgo-env.txt"
test "$(cat "$log_dir/correct-cgo-env.txt")" = 0
set +e
CGO_ENABLED=0 go test -mod=readonly -count=1 -v ./... > "$log_dir/unit.log" 2>&1
status=$?
printf '%s\n' "$status" > "$log_dir/unit.status"
set -e
cd "$repo_root"
git diff --exit-code > "$log_dir/tracked-clean.log" 2>&1
printf '%s\n' "$?" > "$log_dir/tracked-clean.status"
exit "$status"
