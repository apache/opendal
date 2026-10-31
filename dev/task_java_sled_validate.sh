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
test "$#" -eq 4
task_repo=$1
task_logs=$2
task_parent=$3
task_selection=$4
mkdir -p "$task_logs"
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.before"
git rev-parse 'HEAD^' > "$task_logs/parent.before"
git rev-parse 'HEAD^{tree}' > "$task_logs/tree.before"
git status --porcelain > "$task_logs/status.before"
test "$(cat "$task_logs/parent.before")" = "$task_parent"
test ! -s "$task_logs/status.before"

task_deps=/home/xuanwo/reviews/toolchains/opendal-8424-deps
export JAVA_HOME="$task_deps/root/usr/lib/jvm/java-21-openjdk-amd64"
export MAVEN_USER_HOME="$task_deps/m2"
export LD_LIBRARY_PATH="$task_deps/root/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PATH="$JAVA_HOME/bin:$task_deps/root/usr/bin:$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export JAVA_TOOL_OPTIONS=-Djavax.net.ssl.trustStorePassword=changeit
export RUSTUP_TOOLCHAIN=1.99.1
export CARGO_BUILD_JOBS=4
export RUST_TEST_THREADS=1
export OPENDAL_TEST=sled
export OPENDAL_SLED_ROOT=/
export OPENDAL_SLED_DATADIR="$task_logs/datadir"
export OPENDAL_DISABLE_RANDOM_ROOT=false
unset OPENDAL_TEST_CAPABILITY_OVERRIDES
case "$task_selection" in
    all) task_selection='behavior.*Test' ;;
    control)
        export OPENDAL_DISABLE_RANDOM_ROOT=true
        task_selection=behavior.BlockingWriteTest,behavior.SledFixtureIsolationTest
        ;;
    fs)
        export OPENDAL_TEST=fs
        export OPENDAL_FS_ROOT="$task_logs/fs"
        mkdir -p "$OPENDAL_FS_ROOT"
        task_selection=behavior.BlockingWriteTest,behavior.SledFixtureIsolationTest
        ;;
esac
printf '%s\n' "OPENDAL_TEST=$OPENDAL_TEST" "OPENDAL_DISABLE_RANDOM_ROOT=$OPENDAL_DISABLE_RANDOM_ROOT" \
    "OPENDAL_SLED_DATADIR=$OPENDAL_SLED_DATADIR" "selection=$task_selection" > "$task_logs/test-config.txt"
java -version > "$task_logs/java.version" 2>&1
rustc --version > "$task_logs/rustc.version"
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
cd bindings/java
run_check compile ./mvnw -B -ntp -DskipTests test-compile
if test "$(cat "$task_logs/compile.status")" -eq 0; then
    sha256sum target/classes/native/linux-x86_64/libopendal_java.so > "$task_logs/native.sha256"
    if test -d target/surefire-reports; then
        mv target/surefire-reports "$task_logs/surefire-before"
    fi
    run_check tests ./mvnw -B -ntp -Dexec.skip=true "-Dtest=$task_selection" test
    mkdir -p "$task_logs/surefire"
    cp target/surefire-reports/TEST-*.xml "$task_logs/surefire/"
    run_check spotless ./mvnw -B -ntp -Dexec.skip=true spotless:check
fi
cd "$task_repo"
run_check tracked-clean git diff --exit-code
git rev-parse HEAD > "$task_logs/head.after"
git status --porcelain > "$task_logs/status.after"
test ! -s "$task_logs/status.after"
printf '%s\n' "$task_failed" > "$task_logs/driver.status"
exit "$task_failed"
