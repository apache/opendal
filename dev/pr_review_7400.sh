#!/usr/bin/env bash
# Task-only experiment. A successful run includes one expected failing test.
set -euo pipefail

worktree=$1
baseline=$2
restored=$3
log_dir=$4
mkdir -p "$log_dir"
exec >"$log_dir/runner.log" 2>&1
trap 'result=$?; printf "%s\n" "$result" >"$log_dir/done.status.tmp"; mv "$log_dir/done.status.tmp" "$log_dir/done.status"' EXIT

export PATH="$HOME/.cargo/bin:$PATH"
cd "$worktree"
test -z "$(git status --porcelain)"
rustc --version
cargo --version

run_case() {
    local label=$1 sha=$2 expected_exit=$3 expected_bytes=$4 actual_exit
    git checkout --detach "$sha"
    test "$(git rev-parse HEAD)" = "$sha"
    test -z "$(git status --porcelain)"
    printf 'tested_sha=%s\n' "$sha" >"$log_dir/$label.meta.tmp"
    date -u '+started_at=%Y-%m-%dT%H:%M:%SZ' >>"$log_dir/$label.meta.tmp"
    if (
        cd core
        CARGO_BUILD_JOBS=4 cargo test --locked -p opendal-core --lib \
            test_probe_public_writer_replay_after_flush_failure -- --nocapture
    ) >"$log_dir/$label.log" 2>&1; then
        actual_exit=0
    else
        actual_exit=$?
    fi
    printf 'cargo_exit=%s\nexpected_exit=%s\n' "$actual_exit" "$expected_exit" >>"$log_dir/$label.meta.tmp"
    date -u '+finished_at=%Y-%m-%dT%H:%M:%SZ' >>"$log_dir/$label.meta.tmp"
    mv "$log_dir/$label.meta.tmp" "$log_dir/$label.meta"
    test "$actual_exit" -eq "$expected_exit"
    grep -F "stored_bytes=$expected_bytes payload_bytes=18" "$log_dir/$label.log"
    test -z "$(git status --porcelain)"
}

run_case baseline "$baseline" 0 18
grep -F 'test result: ok. 1 passed; 0 failed' "$log_dir/baseline.log"
run_case restored "$restored" 101 28
grep -F 'test result: FAILED. 0 passed; 1 failed' "$log_dir/restored.log"
printf 'comparison_complete=baseline_18_bytes_restored_28_bytes\n'
