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
mkdir -p "$task_logs"
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.before"
git rev-parse 'HEAD^{tree}' > "$task_logs/tree.before"
git status --porcelain > "$task_logs/status.before"
test ! -s "$task_logs/status.before"
git merge-base --is-ancestor d8ff20fc2ae5937175d88572746af1dafac9cfb3 HEAD
mapfile -t task_files < <(git diff --name-only d8ff20fc2ae5937175d88572746af1dafac9cfb3 HEAD)
test "${task_files[*]}" = "core/tests/task_pr8090_reproduce.rs dev/task_pr8090_reproduce.sh"
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export RUSTUP_TOOLCHAIN=1.99.1
export CARGO_BUILD_JOBS=4
export OPENDAL_HDFS_NATIVE_NAME_NODE=hdfs://localhost:8020
rustc --version > "$task_logs/rustc.version"
docker compose version > "$task_logs/compose.version"
df -h "$task_repo" > "$task_logs/storage.before"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
cd core
task_build_exit=0
cargo test --locked --test task_pr8090_reproduce --test behavior --features tests,services-hdfs-native,services-fs --no-run > "$task_logs/build.log" 2>&1 || task_build_exit=$?
printf '%s\n' "$task_build_exit" > "$task_logs/build.status"
test "$task_build_exit" -eq 0
sha256sum Cargo.lock > "$task_logs/cargo-lock.sha256"
cargo tree --locked --features tests,services-hdfs-native,services-fs --invert hdfs-native --depth 1 > "$task_logs/dependency-tree.log" 2>&1
task_failed=0
run_check() {
    local task_name=$1
    shift
    local task_status=0
    "$@" > "$task_logs/$task_name.log" 2>&1 || task_status=$?
    printf '%s\n' "$task_status" > "$task_logs/$task_name.status"
    if test "$task_status" -ne 0; then task_failed=1; fi
}
run_check clippy cargo clippy --locked -p opendal-service-hdfs-native --all-targets -- -D warnings
run_check format cargo fmt --all -- --check
cd "$task_repo"
docker ps -a --format '{{.Names}}' > "$task_logs/containers.before"
if rg -x 'namenode|datanode' "$task_logs/containers.before"; then
    printf '%s\n' 'Refusing to replace an existing Hadoop container.' >&2
    exit 3
fi
ss -H -ltn 'sport = :8020' > "$task_logs/namenode-port.before"
test ! -s "$task_logs/namenode-port.before"
task_compose=(docker compose --project-name opendal-pr8090-reproduce --file "$task_repo/fixtures/hdfs/docker-compose-hdfs-cluster.yml")
cleanup() {
    "${task_compose[@]}" logs --no-color > "$task_logs/cluster.log" 2>&1 || true
    local task_cleanup_exit=0
    "${task_compose[@]}" down --volumes > "$task_logs/cleanup.log" 2>&1 || task_cleanup_exit=$?
    printf '%s\n' "$task_cleanup_exit" > "$task_logs/cleanup.status"
}
trap cleanup EXIT
"${task_compose[@]}" up --quiet-pull --detach --wait --wait-timeout 180 > "$task_logs/setup.log" 2>&1
timeout 180 docker exec namenode hdfs dfsadmin -safemode wait > "$task_logs/safemode.log" 2>&1
docker exec namenode hdfs dfsadmin -report > "$task_logs/hadoop-report.log" 2>&1
cd core
task_probe_exit=0
cargo test --locked --test task_pr8090_reproduce --features tests,services-hdfs-native,services-fs -- --nocapture --test-threads=1 > "$task_logs/probe.log" 2>&1 || task_probe_exit=$?
printf '%s\n' "$task_probe_exit" > "$task_logs/probe.status"
if test "$task_probe_exit" -ne 0; then task_failed=1; fi
export OPENDAL_TEST=hdfs_native
export OPENDAL_HDFS_NATIVE_ROOT=/tmp/opendal/
run_check behavior cargo test --locked --test behavior --features tests,services-hdfs-native,services-fs
export OPENDAL_TEST=fs
export OPENDAL_FS_ROOT="$task_logs/fs"
mkdir -p "$OPENDAL_FS_ROOT"
run_check fs-race cargo test --locked --test behavior --features tests,services-hdfs-native,services-fs -- test_rename_with_if_not_exists_race
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.after"
git status --porcelain > "$task_logs/status.after"
printf '%s\n' "$task_failed" > "$task_logs/driver.status"
exit "$task_failed"
