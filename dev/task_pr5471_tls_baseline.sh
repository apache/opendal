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

# Task-only actual-service baseline; no Memcached production code is changed.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
base_head="${BASE_HEAD:?original worktree head is required}"
log_dir="${LOG_DIR:?a unique log directory is required}"
product_head=58aec67cd76b1d450c2491d807c4229e09e1e49f
project=opendal-pr5471-tls-baseline
image=docker.io/library/memcached@sha256:462fa779babc9b64a235a69dd843cb79fb3591f229ee1f3ff6cb2ca058d8a759
compose_file="$repo_root/fixtures/memcached/task-tls-baseline.yml"
started=0
task_cert_dir=""
export PATH="/home/xuanwo/.cargo/bin:$PATH"
export CARGO_BUILD_JOBS=4
mkdir -p "$log_dir"
cd "$repo_root"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9

finish() {
    local result=$?
    local status
    trap - EXIT
    set +e
    if [ "$started" -eq 1 ]; then
        docker compose -p "$project" -f "$compose_file" logs --no-color > "$log_dir/fixture.log" 2>&1
        docker compose -p "$project" -f "$compose_file" down -v > "$log_dir/fixture-down.log" 2>&1
        status=$?
        printf '%s\n' "$status" > "$log_dir/fixture-down.status"
        if [ "$status" -ne 0 ]; then result="$status"; fi
    fi
    if [ -n "$task_cert_dir" ]; then rm -rf -- "$task_cert_dir"; fi
    git switch --detach "$base_head" > "$log_dir/restore.log" 2>&1
    status=$?
    printf '%s\n' "$status" > "$log_dir/restore.status"
    git rev-parse HEAD HEAD^{tree} > "$log_dir/restored-head.txt"
    git status --porcelain --untracked-files=all > "$log_dir/restored-status.txt"
    if [ "$status" -ne 0 ] || [ -s "$log_dir/restored-status.txt" ]; then result=2; fi
    printf '%s\n' "$result" > "$log_dir/driver.exit"
    exit "$result"
}
trap finish EXIT

git rev-parse HEAD HEAD^{tree} > "$log_dir/source-head.txt"
git diff --name-only "$product_head" HEAD > "$log_dir/task-files.txt"
git status --porcelain --untracked-files=all > "$log_dir/source-status.txt"
test ! -s "$log_dir/source-status.txt"
test "$(git diff --name-only "$product_head" HEAD | wc -l)" -eq 2
cargo --version > "$log_dir/toolchain.txt"
rustc --version >> "$log_dir/toolchain.txt"
openssl version >> "$log_dir/toolchain.txt"
if ss -ltn | rg -q ':(11212|11213)\b'; then echo 'task ports already occupied'; exit 2; fi
if docker ps -a --filter "label=com.docker.compose.project=$project" --format '{{.Names}}' | rg -q .; then echo 'task fixture already exists'; exit 2; fi
if ! docker image inspect "$image" > /dev/null 2>&1; then docker pull "$image" > "$log_dir/image-pull.log" 2>&1; fi
docker image inspect "$image" --format '{{json .RepoDigests}}' > "$log_dir/image-digest.txt"
docker run --rm --network none --entrypoint memcached "$image" --help > "$log_dir/memcached-help.log" 2>&1
rg -q 'ssl_chain_cert' "$log_dir/memcached-help.log"

umask 077
task_cert_dir="$(mktemp -d /tmp/opendal-pr5471-tls.XXXXXX)"
mkdir "$task_cert_dir/server"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj '/CN=OpenDAL task CA' -addext 'basicConstraints=critical,CA:TRUE' -addext 'keyUsage=critical,keyCertSign,cRLSign' -keyout "$task_cert_dir/ca.key" -out "$task_cert_dir/ca.pem" > "$log_dir/cert-setup.log" 2>&1
openssl req -newkey rsa:2048 -nodes -subj '/CN=127.0.0.1' -keyout "$task_cert_dir/server/server.key" -out "$task_cert_dir/server/server.csr" >> "$log_dir/cert-setup.log" 2>&1
printf '%s\n' 'subjectAltName=IP:127.0.0.1' 'extendedKeyUsage=serverAuth' 'basicConstraints=CA:FALSE' > "$task_cert_dir/server.ext"
openssl x509 -req -days 1 -in "$task_cert_dir/server/server.csr" -CA "$task_cert_dir/ca.pem" -CAkey "$task_cert_dir/ca.key" -CAcreateserial -extfile "$task_cert_dir/server.ext" -out "$task_cert_dir/server/server.pem" >> "$log_dir/cert-setup.log" 2>&1
chmod 755 "$task_cert_dir/server"
chmod 644 "$task_cert_dir/server/server.pem" "$task_cert_dir/server/server.key"
export TASK_MEMCACHED_CERT_DIR="$task_cert_dir/server"
started=1
docker compose -p "$project" -f "$compose_file" up -d --wait --pull never > "$log_dir/fixture-up.log" 2>&1
ready=0
for attempt in {1..20}; do
    if timeout 3 openssl s_client -brief -connect 127.0.0.1:11212 -verify_ip 127.0.0.1 -verify_return_error -CAfile "$task_cert_dir/ca.pem" < /dev/null > "$log_dir/tls-preflight.log" 2>&1; then ready=1; break; fi
    sleep 0.25
done
test "$ready" -eq 1

cd "$repo_root/core"
cargo test --locked --features tests,services-memcached --test behavior --no-run > "$log_dir/compile.log" 2>&1
printf '%s\n' "$?" > "$log_dir/compile.status"
set +e
env -u SSL_CERT_DIR SSL_CERT_FILE="$task_cert_dir/ca.pem" OPENDAL_TEST=memcached OPENDAL_MEMCACHED_ENDPOINT=tcp://127.0.0.1:11212 OPENDAL_MEMCACHED_ROOT=/pr5471-tls-baseline/ OPENDAL_TEST_IO_TIMEOUT=5 \
cargo test --locked --offline --features tests,services-memcached --test behavior -- behavior::test_read_full --exact --nocapture > "$log_dir/plain-client-to-tls.log" 2>&1
tls_status=$?
printf '%s\n' "$tls_status" > "$log_dir/plain-client-to-tls.status"
env -u SSL_CERT_DIR SSL_CERT_FILE="$task_cert_dir/ca.pem" OPENDAL_TEST=memcached OPENDAL_MEMCACHED_ENDPOINT=tcp://127.0.0.1:11213 OPENDAL_MEMCACHED_ROOT=/pr5471-plain-control/ OPENDAL_TEST_IO_TIMEOUT=5 \
cargo test --locked --offline --features tests,services-memcached --test behavior -- behavior::test_read_full --exact --nocapture > "$log_dir/plain-client-to-plain.log" 2>&1
plain_status=$?
printf '%s\n' "$plain_status" > "$log_dir/plain-client-to-plain.status"
set -e
test "$tls_status" -ne 0
test "$plain_status" -eq 0
rg -q '0 passed; 1 failed' "$log_dir/plain-client-to-tls.log"
rg -q '1 passed; 0 failed' "$log_dir/plain-client-to-plain.log"
cd "$repo_root"
git diff --exit-code > "$log_dir/tracked-clean.log" 2>&1
printf '%s\n' "$?" > "$log_dir/tracked-clean.status"
