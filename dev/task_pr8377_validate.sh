#!/usr/bin/env bash
set -u -o pipefail

task_repo=$(git rev-parse --show-toplevel) || exit 2
task_logs=${1:?provide an absolute log directory}
mkdir -p "$task_logs"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
export CARGO_BUILD_JOBS=4

git status --porcelain > "$task_logs/initial.status"
if [ -s "$task_logs/initial.status" ]; then exit 3; fi
rustc --version > "$task_logs/toolchain.log"
cargo --version >> "$task_logs/toolchain.log"

task_run() {
    task_label=$1
    shift
    "$@" > "$task_logs/$task_label.log" 2>&1
    task_exit=$?
    printf '%s\n' "$task_exit" > "$task_logs/$task_label.exit"
}

git checkout --detach e9577c5f053961ae87ad522a4ec16d00e12713cf > "$task_logs/baseline-checkout.log" 2>&1 || exit 4
git rev-parse HEAD HEAD^{tree} > "$task_logs/baseline.head"
test "$(git rev-parse HEAD)" = e9577c5f053961ae87ad522a4ec16d00e12713cf || exit 5
cd "$task_repo/core" || exit 6
task_run baseline cargo test --locked -p opendal-core --lib servicer_flat_lister

git checkout --detach 3d3c9711a388ae845b60375aff9f5a1abd42b4a2 > "$task_logs/fixed-checkout.log" 2>&1 || exit 7
git rev-parse HEAD HEAD^{tree} > "$task_logs/fixed.head"
test "$(git rev-parse HEAD)" = 3d3c9711a388ae845b60375aff9f5a1abd42b4a2 || exit 8
task_run core-lib cargo test --locked -p opendal-core --lib
task_run clippy cargo clippy --locked -p opendal-core --all-targets -- -D warnings
task_run fmt cargo fmt --all -- --check
cd "$task_repo" || exit 9
task_run diff-check git diff --check 0dfd56418044e204e5cadcfc1eeceeaec9ae9559 HEAD
git status --porcelain > "$task_logs/final.status"
git rev-parse HEAD HEAD^{tree} > "$task_logs/final.head"

task_ok=1
if [ "$(cat "$task_logs/baseline.exit")" != 101 ]; then task_ok=0; fi
if ! rg -q 'test result: FAILED\. 1 passed; 3 failed;' "$task_logs/baseline.log"; then task_ok=0; fi
for task_label in core-lib clippy fmt diff-check; do
    if [ "$(cat "$task_logs/$task_label.exit")" != 0 ]; then task_ok=0; fi
done
if [ -s "$task_logs/final.status" ]; then task_ok=0; fi
if [ "$task_ok" = 1 ]; then exit 0; else exit 1; fi
