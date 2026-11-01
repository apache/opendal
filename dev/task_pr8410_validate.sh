#!/usr/bin/env bash
set -euo pipefail
task_repo=$1
task_logs=$2
task_source=$3
mkdir -p "$task_logs"
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.before"
git rev-parse HEAD^ > "$task_logs/parent.before"
git rev-parse HEAD^{tree} > "$task_logs/tree.before"
git status --porcelain > "$task_logs/status.before"
test "$(cat "$task_logs/parent.before")" = "$task_source"
test ! -s "$task_logs/status.before"
export PATH=/home/xuanwo/.cargo/bin:$PATH
export RUSTUP_TOOLCHAIN=1.99.1
export CARGO_BUILD_JOBS=4
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock -w 3600 9
rustc --version > "$task_logs/rustc.version"
cargo --version > "$task_logs/cargo.version"
task_result=0
run_check() {
    local task_label=$1
    shift
    local task_code=0
    "$@" > "$task_logs/$task_label.log" 2>&1 || task_code=$?
    printf '%s\n' "$task_code" > "$task_logs/$task_label.status"
    if test "$task_code" != 0; then task_result=1; fi
}
cd core
run_check clippy cargo clippy --locked -p opendal-core -p opendal-service-gdrive --all-targets --all-features -- -D warnings
run_check format cargo fmt --all -- --check
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.after"
git rev-parse HEAD^{tree} > "$task_logs/tree.after"
git status --porcelain > "$task_logs/status.after"
if ! cmp -s "$task_logs/head.before" "$task_logs/head.after" || ! cmp -s "$task_logs/tree.before" "$task_logs/tree.after" || test -s "$task_logs/status.after"; then task_result=1; fi
printf '%s\n' "$task_result" > "$task_logs/driver.status"
exit "$task_result"
