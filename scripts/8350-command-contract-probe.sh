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

# Task-only matched real-Samba command-contract runner.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
base_head="${BASE_HEAD:?original worktree head is required}"
product_head="${PRODUCT_HEAD:?expected production source is required}"
log_dir="${LOG_DIR:?a unique log directory is required}"
project="opendal-pr8350-command"
compose_file="$repo_root/fixtures/smb/docker-compose.yml"
image="ghcr.io/servercontainers/samba@sha256:ac7c406702a3bf4137fcd04ce031d87adbda1b819f698fe89a89b5ee46618dfb"
started=0
export PATH="/home/xuanwo/.cargo/bin:$PATH"
export CARGO_BUILD_JOBS=4
mkdir -p "$log_dir"
cd "$repo_root"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9

finish() {
    local result=$?
    local restore_status
    trap - EXIT
    set +e
    if [ "$started" -eq 1 ]; then
        docker compose -p "$project" -f "$compose_file" exec -T samba ls -lR /shares/data > "$log_dir/native-tree.log" 2>&1
        printf '%s\n' "$?" > "$log_dir/native-tree.status"
        docker compose -p "$project" -f "$compose_file" logs --no-color > "$log_dir/fixture-logs.log" 2>&1
        printf '%s\n' "$?" > "$log_dir/fixture-logs.status"
        docker compose -p "$project" -f "$compose_file" down -v > "$log_dir/fixture-down.log" 2>&1
        local down_status=$?
        printf '%s\n' "$down_status" > "$log_dir/fixture-down.status"
        if [ "$down_status" -ne 0 ]; then result="$down_status"; fi
    fi
    git switch --detach "$base_head" > "$log_dir/restore.log" 2>&1
    restore_status=$?
    printf '%s\n' "$restore_status" > "$log_dir/restore.status"
    git rev-parse HEAD HEAD^{tree} > "$log_dir/restored-head.txt"
    git status --porcelain --untracked-files=all > "$log_dir/restored-status.txt"
    if [ "$restore_status" -ne 0 ] || [ -s "$log_dir/restored-status.txt" ]; then result=2; fi
    printf '%s\n' "$result" > "$log_dir/driver.exit"
    exit "$result"
}
trap finish EXIT

git rev-parse HEAD HEAD^{tree} > "$log_dir/source-head.txt"
printf '%s\n' "$product_head" > "$log_dir/product-head.txt"
git diff --name-only "$product_head" HEAD > "$log_dir/task-files.txt"
git status --porcelain --untracked-files=all > "$log_dir/source-status.txt"
test ! -s "$log_dir/source-status.txt"
test "$(git rev-parse HEAD:core/services/smb/src/core.rs)" = "$(git rev-parse "$product_head":core/services/smb/src/core.rs)"
cargo --version > "$log_dir/toolchain.txt"
rustc --version >> "$log_dir/toolchain.txt"
docker image inspect "$image" --format '{{json .RepoDigests}}' > "$log_dir/image-digest.txt"
if ss -ltn | rg -q ':1445\b'; then echo 'fixture port1445 already occupied'; exit 2; fi
if docker ps -a --filter "label=com.docker.compose.project=$project" --format '{{.Names}}' | rg -q .; then echo 'fixture project already exists'; exit 2; fi
started=1
docker compose -p "$project" -f "$compose_file" up -d --wait --pull never > "$log_dir/fixture-up.log" 2>&1
printf '%s\n' "$?" > "$log_dir/fixture-up.status"
cd "$repo_root/core"
set +e
OPENDAL_TEST=smb OPENDAL_SMB_ENDPOINT=127.0.0.1:1445 OPENDAL_SMB_SHARE=data OPENDAL_SMB_ROOT=/opendal/ OPENDAL_SMB_USER=opendal OPENDAL_SMB_PASSWORD=opendal \
cargo test -p opendal-service-smb --test concurrency_probe --locked -- --ignored --nocapture --test-threads=2 > "$log_dir/probe.log" 2>&1
probe_status=$?
printf '%s\n' "$probe_status" > "$log_dir/probe.status"
set -e
cd "$repo_root"
git diff --exit-code > "$log_dir/tracked-clean.log" 2>&1
printf '%s\n' "$?" > "$log_dir/tracked-clean.status"
exit "$probe_status"
