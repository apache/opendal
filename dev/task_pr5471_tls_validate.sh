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
base_head="${BASE_HEAD:?original worktree HEAD required}"
log_dir="${LOG_DIR:?new log directory required}"
product_head=8b2dc788752020ad46222017cfa1650cc7a80987
image=docker.io/library/memcached@sha256:462fa779babc9b64a235a69dd843cb79fb3591f229ee1f3ff6cb2ca058d8a759
plain_name=opendal-pr5471-plain-control
task_cert_dir=""
tls_started=0
plain_started=0
export PATH="/home/xuanwo/.cargo/bin:$PATH"
export CARGO_BUILD_JOBS=4
test ! -e "$log_dir"
mkdir -p "$log_dir"
cd "$repo_root"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
finish() {
    local result=$?
    local cleanup_status
    trap - EXIT
    set +e
    if [ "$plain_started" -eq 1 ]; then
        docker logs "$plain_name" > "$log_dir/plain-fixture.log" 2>&1
        docker rm -f "$plain_name" > "$log_dir/plain-down.log" 2>&1
        cleanup_status=$?
        printf '%s\n' "$cleanup_status" > "$log_dir/plain-down.status"
        if [ "$cleanup_status" -ne 0 ]; then result=2; fi
    fi
    if [ "$tls_started" -eq 1 ]; then
        docker compose -p opendal-memcached-tls -f "$repo_root/fixtures/memcached/docker-compose-tls.yml" logs --no-color > "$log_dir/tls-fixture.log" 2>&1
        docker compose -p opendal-memcached-tls -f "$repo_root/fixtures/memcached/docker-compose-tls.yml" down -v > "$log_dir/tls-down.log" 2>&1
        cleanup_status=$?
        printf '%s\n' "$cleanup_status" > "$log_dir/tls-down.status"
        if [ "$cleanup_status" -ne 0 ]; then result=2; fi
    fi
    if [ -n "$task_cert_dir" ]; then rm -rf -- "$task_cert_dir"; fi
    git switch --detach "$base_head" > "$log_dir/restore.log" 2>&1
    cleanup_status=$?
    printf '%s\n' "$cleanup_status" > "$log_dir/restore.status"
    git rev-parse HEAD HEAD^{tree} > "$log_dir/restored-head.txt"
    git status --porcelain --untracked-files=all > "$log_dir/restored-status.txt"
    if [ "$cleanup_status" -ne 0 ] || [ -s "$log_dir/restored-status.txt" ]; then result=2; fi
    printf '%s\n' "$result" > "$log_dir/driver.exit"
    exit "$result"
}
trap finish EXIT
run_stage() {
    local stage="$1" expected="$2"
    shift 2
    set +e
    "$@" > "$log_dir/$stage.log" 2>&1
    local stage_status=$?
    set -e
    printf '%s\n' "$stage_status" > "$log_dir/$stage.status"
    test "$stage_status" -eq "$expected"
}
sha256sum dev/task_pr5471_tls_validate.sh > "$log_dir/driver.sha256"
git status --porcelain --untracked-files=all > "$log_dir/pre-status.txt"
test ! -s "$log_dir/pre-status.txt"
git switch --detach "$product_head" > "$log_dir/product-checkout.log" 2>&1
git rev-parse HEAD HEAD^{tree} > "$log_dir/source-head.txt"
cargo --version > "$log_dir/toolchain.txt"
rustc --version >> "$log_dir/toolchain.txt"
if ss -ltn | rg -q ':(11211|11212|11213)\b'; then echo 'required task port already occupied'; exit 2; fi
test -z "$(docker ps -aq --filter name=^/$plain_name'$')"
test -z "$(docker ps -aq --filter label=com.docker.compose.project=opendal-memcached-tls)"
cd core
run_stage unit 0 cargo test --locked -p opendal-service-memcached --lib
rg -q 'test result: ok\. [1-9][0-9]* passed' "$log_dir/unit.log"
run_stage clippy 0 cargo clippy --locked -p opendal-service-memcached --all-targets --all-features -- -D warnings
run_stage docs 0 cargo test --locked -p opendal-service-memcached --doc
run_stage fmt 0 cargo fmt --all -- --check
run_stage behavior-compile 0 cargo test --locked --features tests,services-memcached --test behavior --no-run
cd "$repo_root"
umask 077
task_cert_dir="$(mktemp -d /tmp/opendal-pr5471-tls-candidate.XXXXXX)"
export MEMCACHED_TLS_CERT_DIR="$task_cert_dir/server"
tls_started=1
run_stage fixture-setup 0 bash fixtures/memcached/setup-tls.sh "$task_cert_dir"
plain_started=1
run_stage plain-setup 0 docker run -d --name "$plain_name" -p 127.0.0.1:11213:11211 "$image" memcached -I 16777216
cd core
run_stage tls-canonical 0 env -u SSL_CERT_DIR SSL_CERT_FILE="$task_cert_dir/roots.pem" OPENDAL_TEST=memcached OPENDAL_MEMCACHED_ENDPOINT=tcp://127.0.0.1:11212 OPENDAL_MEMCACHED_ENABLE_TLS=true OPENDAL_MEMCACHED_ROOT=/pr5471-tls-candidate/ OPENDAL_TEST_IO_TIMEOUT=5 cargo test --locked --offline --features tests,services-memcached --test behavior -- behavior --nocapture
run_stage plain-control 0 env -u SSL_CERT_DIR -u OPENDAL_MEMCACHED_ENABLE_TLS SSL_CERT_FILE="$task_cert_dir/roots.pem" OPENDAL_TEST=memcached OPENDAL_MEMCACHED_ENDPOINT=tcp://127.0.0.1:11213 OPENDAL_MEMCACHED_ROOT=/pr5471-plain-control/ OPENDAL_TEST_IO_TIMEOUT=5 cargo test --locked --offline --features tests,services-memcached --test behavior -- behavior::test_read_full --exact --nocapture
rg -q '1 passed; 0 failed' "$log_dir/plain-control.log"
run_stage tls-untrusted 101 env -u SSL_CERT_DIR -u SSL_CERT_FILE OPENDAL_TEST=memcached OPENDAL_MEMCACHED_ENDPOINT=tcp://127.0.0.1:11212 OPENDAL_MEMCACHED_ENABLE_TLS=true OPENDAL_MEMCACHED_ROOT=/pr5471-untrusted/ OPENDAL_TEST_IO_TIMEOUT=5 cargo test --locked --offline --features tests,services-memcached --test behavior -- behavior::test_read_full --exact --nocapture
rg -q '0 passed; 1 failed' "$log_dir/tls-untrusted.log"
rg -q 'UnknownIssuer|unknown issuer' "$log_dir/tls-untrusted.log"
run_stage tls-wrong-name 101 env -u SSL_CERT_DIR SSL_CERT_FILE="$task_cert_dir/roots.pem" OPENDAL_TEST=memcached OPENDAL_MEMCACHED_ENDPOINT=tcp://localhost:11212 OPENDAL_MEMCACHED_ENABLE_TLS=true OPENDAL_MEMCACHED_ROOT=/pr5471-wrong-name/ OPENDAL_TEST_IO_TIMEOUT=5 cargo test --locked --offline --features tests,services-memcached --test behavior -- behavior::test_read_full --exact --nocapture
rg -q '0 passed; 1 failed' "$log_dir/tls-wrong-name.log"
rg -q 'NotValidForName|not valid for name' "$log_dir/tls-wrong-name.log"
cd "$repo_root"
run_stage tracked-clean 0 git diff --exit-code
