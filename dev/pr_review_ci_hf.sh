#!/usr/bin/env bash
# Task-only validation; production source stays at the supplied repair commit.
set -euo pipefail
worktree=$1
source_sha=$2
log_dir=$3
mkdir -p "$log_dir"
exec >"$log_dir/runner.log" 2>&1
trap 'result=$?; printf "%s\n" "$result" >"$log_dir/done.status.tmp"; mv "$log_dir/done.status.tmp" "$log_dir/done.status"' EXIT
export PATH="$HOME/.cargo/bin:$PATH"
cd "$worktree"
test -z "$(git status --porcelain)"
git checkout --detach "$source_sha"
test "$(git rev-parse HEAD)" = "$source_sha"
printf 'tested_sha=%s\n' "$source_sha"
rustc --version
node --version
node --test .github/actions/hf-temp-repo/cleanup.test.js >"$log_dir/node.log" 2>&1
cd core
export CARGO_BUILD_JOBS=4
cargo test --locked -p opendal-testkit --lib >"$log_dir/testkit.log" 2>&1
cargo clippy --locked -p opendal-testkit --all-targets -- -D warnings >"$log_dir/clippy.log" 2>&1
cargo fmt --all -- --check >"$log_dir/fmt.log" 2>&1
OPENDAL_TEST=memory OPENDAL_TEST_IO_TIMEOUT=30 cargo test --locked --test behavior --features tests -- --exact behavior::test_write_only >"$log_dir/memory-override.log" 2>&1
grep -F 'test result: ok. 1 passed; 0 failed' "$log_dir/memory-override.log"
OPENDAL_TEST=memory OPENDAL_TEST_IO_TIMEOUT=__OPENDAL_TEST_UNSET__ cargo test --locked --test behavior --features tests -- --exact behavior::test_write_only >"$log_dir/memory-reset.log" 2>&1
grep -F 'test result: ok. 1 passed; 0 failed' "$log_dir/memory-reset.log"
for bad_value in invalid 0; do
    if OPENDAL_TEST=memory OPENDAL_TEST_IO_TIMEOUT=$bad_value cargo test --locked --test behavior --features tests -- --exact behavior::test_write_only >"$log_dir/memory-$bad_value.log" 2>&1; then
        printf 'invalid configuration unexpectedly succeeded\n'
        exit 1
    else
        invalid_exit=$?
    fi
    printf 'invalid_config=%s cargo_exit=%s\n' "$bad_value" "$invalid_exit"
    grep -F 'ConfigInvalid' "$log_dir/memory-$bad_value.log"
    grep -F 'OPENDAL_TEST_IO_TIMEOUT must be a positive whole number of seconds' "$log_dir/memory-$bad_value.log"
done
test -z "$(git status --porcelain)"
printf 'focused_validation_complete=true\n'
