#!/usr/bin/env bash
set -euo pipefail

task_repo=$1
task_logs=$2
task_source=$3
task_transport=$4
mkdir -p "$task_logs"
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.before"
git rev-parse HEAD^ > "$task_logs/parent.before"
git rev-parse HEAD^{tree} > "$task_logs/tree.before"
git status --porcelain > "$task_logs/status.before"
test "$(cat "$task_logs/head.before")" = "$task_transport"
test "$(cat "$task_logs/parent.before")" = "$task_source"
test ! -s "$task_logs/status.before"
test ! -e "$task_logs/driver.status"

export JAVA_HOME=/home/xuanwo/reviews/toolchains/opendal-8424-deps/root/usr/lib/jvm/java-21-openjdk-amd64
export PATH=$JAVA_HOME/bin:/home/xuanwo/.cargo/bin:/home/xuanwo/reviews/toolchains/opendal-8424-deps/root/usr/bin:$PATH
export LD_LIBRARY_PATH=/home/xuanwo/reviews/toolchains/opendal-8424-deps/root/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
export RUSTUP_TOOLCHAIN=1.99.1
export CARGO_BUILD_JOBS=4
unset OPENDAL_TEST OPENDAL_FS_ROOT
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock -w 3600 9
rustc --version > "$task_logs/rustc.version"
cargo --version > "$task_logs/cargo.version"
java -version > "$task_logs/java.version" 2>&1

task_result=0
run_check() {
    local task_label=$1
    shift
    local task_code=0
    "$@" > "$task_logs/$task_label.log" 2>&1 || task_code=$?
    printf '%s\n' "$task_code" > "$task_logs/$task_label.status"
    if test "$task_code" != 0; then task_result=1; fi
}

run_check product-diff git diff --check "$task_source^2" "$task_source"
cd bindings/java
cargo metadata --locked --no-deps --format-version 1 > "$task_logs/cargo-metadata.json"
run_check java-tests ./mvnw -Dtest=PresignOptionsTest,OperatorReaderTest,ReaderOptionsTest,OperatorInputOutputStreamTest test
mkdir -p "$task_logs/java-tests-reports"
cp target/surefire-reports/TEST-org.apache.opendal.test.PresignOptionsTest.xml target/surefire-reports/TEST-org.apache.opendal.test.OperatorReaderTest.xml target/surefire-reports/TEST-org.apache.opendal.test.ReaderOptionsTest.xml target/surefire-reports/TEST-org.apache.opendal.test.OperatorInputOutputStreamTest.xml "$task_logs/java-tests-reports/"
run_check fs-reader env OPENDAL_TEST=fs OPENDAL_FS_ROOT=/tmp/opendal-pr8424-fs-495b6cf1 ./mvnw -Dtest=BlockingWriteTest#testBlockingInputStreamWithReaderOptions test
mkdir -p "$task_logs/fs-reader-reports"
cp target/surefire-reports/TEST-org.apache.opendal.test.behavior.BlockingWriteTest.xml "$task_logs/fs-reader-reports/"
run_check clippy cargo clippy --locked -- -D warnings
run_check format cargo fmt --all -- --check
run_check spotless ./mvnw spotless:check
sha256sum target/classes/native/linux-x86_64/libopendal_java.so > "$task_logs/native.sha256"
sha256sum target/classes/org/apache/opendal/AsyncOperator.class target/classes/org/apache/opendal/ReadOptions.class target/classes/org/apache/opendal/ReaderOptions.class target/classes/org/apache/opendal/OperatorReader.class > "$task_logs/java-classes.sha256"

cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.after"
git rev-parse HEAD^{tree} > "$task_logs/tree.after"
git status --porcelain > "$task_logs/status.after"
if ! cmp -s "$task_logs/head.before" "$task_logs/head.after" || ! cmp -s "$task_logs/tree.before" "$task_logs/tree.after" || test -s "$task_logs/status.after"; then task_result=1; fi
printf '%s\n' "$task_result" > "$task_logs/driver.status"
exit "$task_result"
