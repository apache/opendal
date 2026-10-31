#!/usr/bin/env bash
#
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
export PATH=/home/xuanwo/.local/bin:/home/xuanwo/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

worktree=/home/xuanwo/Code/worktrees/od-review-8401-20261031
run_id=${RUN_ID:-fd4b6cb9-20261101}
base_run_id=${BASE_RUN_ID:-fd4b6cb9-20261101}
log_dir=/home/xuanwo/Code/logs/opendal-pr8401-${run_id}
base_log_dir=/home/xuanwo/Code/logs/opendal-pr8401-${base_run_id}
fs_root=/tmp/opendal-pr8401-behavior-${run_id}
expected_head=fd4b6cb9d3e50c877aa0a18fc321367a70f5e37e
expected_parents="fd4b6cb9d3e50c877aa0a18fc321367a70f5e37e 5c2820cd2fc7fd27bf0c3033b846d147b45c11e4 1113b93f2900ccea123113a67e88b099e704d772"
expected_tree=53f30360c4b9462285a9059bd5c45c30b795d745
expected_native_sha=222b7f26af1efc2ef8c1b6f5382fc2012eeb3b0fab5b031cae446f8fd80f84ee
image=maven:3.9-eclipse-temurin-21
lock=/tmp/opendal-pr-delivery-20261031-build.lock
native=target/classes/native/linux-x86_64/libopendal_java.so
fs_env="$log_dir/fs.env"

case "$run_id" in
    ""|*[!A-Za-z0-9._-]*)
        echo "invalid RUN_ID: $run_id"
        exit 2
        ;;
esac
case "$base_run_id" in
    ""|*[!A-Za-z0-9._-]*)
        echo "invalid BASE_RUN_ID: $base_run_id"
        exit 2
        ;;
esac

mkdir -p "$log_dir"
driver_status="$log_dir/00-driver-run.status"
if [ -e "$fs_root" ]; then
    echo "task filesystem root already exists: $fs_root"
    exit 4
fi
if [ -e "$driver_status" ] || [ -e "$log_dir/03-metadata-test.log" ]; then
    echo "refusing to overwrite prior validation logs; choose another RUN_ID"
    exit 5
fi

printf 'running\n' > "$driver_status"
fs_root_owned=0
cleanup() {
    result=$?
    trap - EXIT
    if [ "$fs_root_owned" -eq 1 ]; then
        rm -rf "$fs_root"
    fi
    if [ -e "$fs_root" ]; then
        printf 'not-clean\n' > "$log_dir/fs-root-cleanup.status"
        [ "$result" -ne 0 ] || result=3
    else
        printf 'clean\n' > "$log_dir/fs-root-cleanup.status"
    fi
    printf '%s\n' "$result" > "$driver_status"
    exit "$result"
}
trap cleanup EXIT

cd "$worktree"
test "$(git rev-parse HEAD)" = "$expected_head"
test "$(git rev-list --parents -n 1 HEAD)" = "$expected_parents"
test "$(git rev-parse HEAD^{tree})" = "$expected_tree"
test -z "$(git status --porcelain)"
test "$(cat "$base_log_dir/01-clean-compile-spotless.status")" = 0
test "$(cat "$base_log_dir/02-native-build.status")" = 0
sha256sum "$base_log_dir/01-clean-compile-spotless.log" \
    "$base_log_dir/02-native-build.log" \
    > "$log_dir/input-artifacts.sha256"
printf '%s\n' \
    "$base_log_dir/01-clean-compile-spotless.log" \
    "$base_log_dir/02-native-build.log" \
    > "$log_dir/input-artifacts.txt"

printf '%s\n' "$expected_head" > "$log_dir/validated-head.txt"
printf '%s\n' "$expected_parents" > "$log_dir/validated-parents.txt"
printf '%s\n' "$expected_tree" > "$log_dir/validated-tree.txt"
git status --porcelain > "$log_dir/worktree-status-before.txt"

cd bindings/java
test -f "$native"
actual_native_sha=$(sha256sum "$native" | awk '{print $1}')
test "$actual_native_sha" = "$expected_native_sha"
printf '%s  %s\n' "$actual_native_sha" "$native" > "$log_dir/native-validated.sha256"
stat -c 'size=%s mtime=%y owner=%U:%G path=%n' "$native" > "$log_dir/native-validated.stat.txt"
sha256sum target/classes/org/apache/opendal/Metadata.class > "$log_dir/metadata-class.sha256"
sha256sum src/convert.rs src/lib.rs src/main/java/org/apache/opendal/Metadata.java \
    > "$log_dir/source-file-sha256.txt"

mkdir "$fs_root"
fs_root_owned=1
printf 'OPENDAL_TEST=fs\nOPENDAL_FS_ROOT=%s/\n' "$fs_root" > "$fs_env"

task_uid=$(id -u)
task_gid=$(id -g)
docker_run=(
    docker run --rm --user "${task_uid}:${task_gid}" -e HOME=/home/xuanwo
    -v "${worktree}:/workspace"
    -v /home/xuanwo/.m2:/home/xuanwo/.m2
    -v "${fs_root}:${fs_root}"
    -w /workspace/bindings/java
)

run_logged() {
    local name=$1
    local status
    shift
    printf '%q ' "$@" > "$log_dir/${name}.command"
    printf '\n' >> "$log_dir/${name}.command"
    set +e
    /usr/bin/time -p "$@" > "$log_dir/${name}.log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" > "$log_dir/${name}.status"
    return "$status"
}

run_logged 03-metadata-test \
    "${docker_run[@]}" "$image" ./mvnw test -Dstyle.color=never \
    -Dexec.skip=true -Dtest=MetadataTest
grep -Fq '[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0' \
    "$log_dir/03-metadata-test.log"

run_logged 04-async-user-metadata \
    "${docker_run[@]}" --env-file "$fs_env" "$image" ./mvnw test \
    -Dstyle.color=never -Dexec.skip=true \
    -Dtest=AsyncWriteOptionsTest#testWriteWithUserMetadata
grep -Fq '[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0' \
    "$log_dir/04-async-user-metadata.log"

run_logged 05-blocking-user-metadata \
    "${docker_run[@]}" --env-file "$fs_env" "$image" ./mvnw test \
    -Dstyle.color=never -Dexec.skip=true \
    -Dtest=BlockingWriteOptionTest#testWriteWithUserMetadata
grep -Fq '[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0' \
    "$log_dir/05-blocking-user-metadata.log"

cp target/surefire-reports/TEST-org.apache.opendal.test.MetadataTest.xml \
    "$log_dir/03-metadata-test.xml"
cp target/surefire-reports/TEST-org.apache.opendal.test.behavior.AsyncWriteOptionsTest.xml \
    "$log_dir/04-async-user-metadata.xml"
cp target/surefire-reports/TEST-org.apache.opendal.test.behavior.BlockingWriteOptionTest.xml \
    "$log_dir/05-blocking-user-metadata.xml"

run_logged 06-java-spotless-check \
    "${docker_run[@]}" "$image" ./mvnw spotless:check \
    -Dstyle.color=never -Dexec.skip=true

run_logged 07-metadata-javap \
    docker run --rm --user "${task_uid}:${task_gid}" -e HOME=/home/xuanwo \
    -v "${worktree}:/workspace" -w /workspace/bindings/java \
    --entrypoint javap "$image" -classpath target/classes -s -p org.apache.opendal.Metadata
grep -Fq 'descriptor: (IJLjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/time/Instant;Ljava/lang/String;)V' \
    "$log_dir/07-metadata-javap.log"
grep -Fq 'descriptor: (IJLjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/time/Instant;Ljava/lang/String;Ljava/util/Map;)V' \
    "$log_dir/07-metadata-javap.log"
grep -Fq 'descriptor: ()Ljava/util/Map;' "$log_dir/07-metadata-javap.log"

run_logged 08-rustfmt-check cargo fmt -- --check
run_logged 09-java-binding-clippy \
    flock "$lock" env CARGO_BUILD_JOBS=4 cargo clippy -- -D warnings

cd "$worktree"
test -z "$(git status --porcelain)"
git status --porcelain > "$log_dir/worktree-status-after.txt"
printf '0\n' > "$log_dir/validation-complete.status"
