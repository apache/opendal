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
export PATH="/home/xuanwo/.cargo/bin:$PATH"
export RUSTUP_TOOLCHAIN=1.99.1
[[ -z ${CARGO_TARGET_DIR:-} && -z ${CARGO_BUILD_BUILD_DIR:-} ]] || exit 2

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
base_head="${BASE_HEAD:-c33aaff7905ccf83bd3bf31674f7cf0888a6f036}"
project="opendal-pr8350-fs-observation"
log_dir="${LOG_DIR:-/home/xuanwo/Code/logs/opendal-pr8350-diagnostic-$(date +%Y%m%d%H%M%S)-$$}"
compose_file="$repo_root/fixtures/smb/docker-compose.yml"
probe_file="$repo_root/core/services/smb/tests/concurrency_probe.rs"
core_file="$repo_root/core/services/smb/src/core.rs"
started=0

mkdir -p "$(dirname "$log_dir")"
mkdir "$log_dir" || exit 2

cleanup() {
    local result=$?
    trap - EXIT
    if [ "$started" -eq 1 ]; then
        if [ -f "$log_dir/attempt-1.log" ]; then
            root=$(sed -n 's/.*case=mkdir-missing root=\([^ ]*\) .*/\1/p' "$log_dir/attempt-1.log" | sort -u)
            printf '%s\n' "$root" > "$log_dir/observed-root.txt"
            if [[ $root =~ ^/probe-8350-[0-9]+-mkdir-missing-[0-9]+/$ ]]; then
                date -u +%Y-%m-%dT%H:%M:%SZ > "$log_dir/fs-observation-time.txt"
                for label in root parent a b; do
                    case "$label" in
                        root) suffix="" ;;
                        parent) suffix="/parent" ;;
                        a) suffix="/parent/a" ;;
                        b) suffix="/parent/b" ;;
                    esac
                    path="/shares/data${root%/}$suffix"
                    for mode in lstat stat link; do
                        case "$mode" in
                            lstat) args=(stat -c 'type=%F mode=%a uid=%u gid=%g inode=%i path=%n' -- "$path") ;;
                            stat) args=(stat -L -c 'type=%F mode=%a uid=%u gid=%g inode=%i path=%n' -- "$path") ;;
                            link) args=(readlink -- "$path") ;;
                        esac
                        docker compose -p "$project" -f "$compose_file" exec -T samba "${args[@]}" \
                            > "$log_dir/fs-$label-$mode.log" 2>&1
                        printf '%s\n' "$?" > "$log_dir/fs-$label-$mode.status"
                    done
                done
            else
                printf '%s\n' 'No unique valid probe root; no directory entry observation.' > "$log_dir/fs-not-observed.txt"
            fi
        fi
        docker compose -p "$project" -f "$compose_file" images --format json > "$log_dir/fixture-images.json" 2>&1
        docker compose -p "$project" -f "$compose_file" logs --no-color \
            > "$log_dir/fixture-logs.log" 2>&1
        printf '%s\n' "$?" > "$log_dir/fixture-logs.status"
        docker compose -p "$project" -f "$compose_file" down -v \
            > "$log_dir/fixture-down.log" 2>&1
        down_status=$?
        printf '%s\n' "$down_status" > "$log_dir/fixture-down.status"
        [ "$down_status" -eq 0 ] || result=1
    fi
    printf '%s\n' "$result" > "$log_dir/driver.exit"
    exit "$result"
}
trap cleanup EXIT

cd "$repo_root"
printf '%s\n' "$(git rev-parse HEAD)" > "$log_dir/head-sha.txt"
printf '%s\n' "$base_head" > "$log_dir/base-head.txt"
git diff --name-only "$base_head" -- > "$log_dir/changed-files.txt"
git status --porcelain > "$log_dir/worktree-status.txt"
test ! -s "$log_dir/worktree-status.txt" || exit 2
rustc -Vv > "$log_dir/rustc-version.log"
cargo -V > "$log_dir/cargo-version.log"

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
if docker volume ls --filter "label=com.docker.compose.project=$project" --format '{{.Name}}' | grep -q .; then
    echo "task fixture volume already exists; refusing to adopt it"
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
cargo metadata --locked --no-deps --format-version 1 > "$log_dir/workspace-metadata.json"

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

attempt_log="$log_dir/attempt-1.log"
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
printf '%s\n' "$attempt_status" > "$log_dir/attempt-1.status"

if grep -q 'running 1 test' "$attempt_log"; then
    case_ran=1
else
    case_ran=0
fi
printf '%s\n' "$case_ran" > "$log_dir/attempt-1-case-ran.txt"

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
    printf '%s\n' 1 > "$log_dir/diagnostic-captured.txt"
else
    printf '%s\n' 0 > "$log_dir/diagnostic-captured.txt"
fi
exit "$attempt_status"
