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

# Task-only diagnostic runner for the DirectoryFile/Resource::File mismatch.
# The diagnostic patch must already be applied to core.rs and the filtered
# concurrency_probe test must already be present on the task-only branch.

set -u
set -o pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
base_head="${BASE_HEAD:-c33aaff7905ccf83bd3bf31674f7cf0888a6f036}"
project="opendal-pr8350-diagnostic"
log_dir="${LOG_DIR:-/home/xuanwo/Code/logs/opendal-pr8350-diagnostic-$(date +%Y%m%d%H%M%S)-$$}"
compose_file="$repo_root/fixtures/smb/docker-compose.yml"
probe_file="$repo_root/core/services/smb/tests/concurrency_probe.rs"
core_file="$repo_root/core/services/smb/src/core.rs"
started=0

mkdir -p "$log_dir"

cleanup() {
    local result=$?
    trap - EXIT
    if [ "$started" -eq 1 ]; then
        docker compose -p "$project" -f "$compose_file" logs --no-color \
            > "$log_dir/fixture-logs.log" 2>&1
        printf '%s\n' "$?" > "$log_dir/fixture-logs.status"
        docker compose -p "$project" -f "$compose_file" down -v \
            > "$log_dir/fixture-down.log" 2>&1
        printf '%s\n' "$?" > "$log_dir/fixture-down.status"
    fi
    exit "$result"
}
trap cleanup EXIT

cd "$repo_root"
printf '%s\n' "$(git rev-parse HEAD)" > "$log_dir/head-sha.txt"
printf '%s\n' "$base_head" > "$log_dir/base-head.txt"
git diff --name-only "$base_head" -- > "$log_dir/changed-files.txt"
git status --porcelain > "$log_dir/worktree-status.txt"

if [ ! -f "$probe_file" ]; then
    echo "missing filtered concurrency probe: $probe_file"
    exit 2
fi

if ! grep -q 'SMB_CREATE_DIR_DIAG' "$core_file"; then
    echo "diagnostic patch marker is missing from: $core_file"
    exit 2
fi

if ss -ltn | grep -q '127\.0\.0\.1:1445'; then
    echo "127.0.0.1:1445 is already in use; refusing to touch another fixture"
    exit 2
fi

if docker ps -a --filter "label=com.docker.compose.project=$project" \
    --format '{{.Names}}' | grep -q .; then
    echo "task fixture project already exists: $project"
    exit 2
fi

started=1
docker compose -p "$project" -f "$compose_file" up -d --wait \
    > "$log_dir/fixture-up.log" 2>&1
fixture_status=$?
printf '%s\n' "$fixture_status" > "$log_dir/fixture-up.status"
if [ "$fixture_status" -ne 0 ]; then
    exit "$fixture_status"
fi

cd "$repo_root/core"

flock /tmp/opendal-pr-delivery-20261031-build.lock \
    env PATH=/home/xuanwo/.cargo/bin:/usr/bin:/bin \
    CARGO_BUILD_JOBS=4 \
    cargo test -p opendal-service-smb --test concurrency_probe --locked \
    --no-run \
    > "$log_dir/compile.log" 2>&1
compile_status=$?
printf '%s\n' "$compile_status" > "$log_dir/compile.status"
if [ "$compile_status" -ne 0 ]; then
    echo "diagnostic probe compilation failed; no case was run"
    exit "$compile_status"
fi

captured=0
attempt=1
while [ "$attempt" -le 3 ]; do
    attempt_log="$log_dir/attempt-$attempt.log"
    flock /tmp/opendal-pr-delivery-20261031-build.lock \
        env PATH=/home/xuanwo/.cargo/bin:/usr/bin:/bin \
        CARGO_BUILD_JOBS=4 \
        OPENDAL_TEST=smb \
        OPENDAL_SMB_ENDPOINT=127.0.0.1:1445 \
        OPENDAL_SMB_SHARE=data \
        OPENDAL_SMB_ROOT=/opendal/ \
        OPENDAL_SMB_USER=opendal \
        OPENDAL_SMB_PASSWORD=opendal \
        cargo test -p opendal-service-smb --test concurrency_probe --locked \
        -- --ignored --exact concurrent_create_dir_with_missing_parent \
        --nocapture --test-threads=1 \
        > "$attempt_log" 2>&1
    attempt_status=$?
    printf '%s\n' "$attempt_status" > "$log_dir/attempt-$attempt.status"

    if grep -q 'running 1 test' "$attempt_log"; then
        case_ran=1
    else
        case_ran=0
    fi
    printf '%s\n' "$case_ran" > "$log_dir/attempt-$attempt-case-ran.txt"

    if [ "$case_ran" -ne 1 ]; then
        echo "filtered case did not run; stopping without retry"
        exit 5
    fi
    if grep -q 'SMB_CREATE_DIR_DIAG' "$attempt_log"; then
        if [ "$attempt_status" -ne 101 ]; then
            echo "diagnostic branch must retain the failing test result"
            exit 6
        fi
        grep 'SMB_CREATE_DIR_DIAG' "$attempt_log" \
            > "$log_dir/diagnostic-capture.txt"
        captured=1
        break
    fi

    if [ "$attempt_status" -ne 0 ]; then
        echo "filtered case failed without diagnostic marker; stopping without retry"
        exit 4
    fi

    attempt=$((attempt + 1))
done

printf '%s\n' "$captured" > "$log_dir/diagnostic-captured.txt"
if [ "$captured" -ne 1 ]; then
    echo "diagnostic marker was not captured after 3 passing attempts"
    exit 3
fi

echo "diagnostic_capture=$log_dir/diagnostic-capture.txt"
exit 0
