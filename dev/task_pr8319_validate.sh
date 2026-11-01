#!/usr/bin/env bash
set -euo pipefail
task_repo=$1
task_logs=$2
task_product=$3
task_head=$4
task_tools=$5
mkdir -p "$task_logs/tmp"
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.before"
git rev-parse HEAD^ > "$task_logs/parent.before"
git rev-parse HEAD^{tree} > "$task_logs/tree.before"
git status --porcelain > "$task_logs/status.before"
test "$(cat "$task_logs/head.before")" = "$task_head"
test "$(cat "$task_logs/parent.before")" = "$task_product"
test ! -s "$task_logs/status.before"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
export CARGO_BUILD_JOBS=4 RUSTUP_TOOLCHAIN=1.99.1
unset CARGO_TARGET_DIR
export PATH="$task_tools/root/chrome-linux64:$task_tools/root/chromedriver-linux64:$PATH"
export LD_LIBRARY_PATH="$task_tools/sysroot/usr/lib/x86_64-linux-gnu:$task_tools/sysroot/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH:-}"
export TMPDIR="$task_logs/tmp"
rustc --version > "$task_logs/rustc.version"
cargo --version > "$task_logs/cargo.version"
wasm-pack --version > "$task_logs/wasm-pack.version"
chrome --version > "$task_logs/chrome.version"
chromedriver --version > "$task_logs/chromedriver.version"
sha256sum "$(command -v chrome)" "$(command -v chromedriver)" > "$task_logs/browser.sha256"
cd core/edge/opfs_wasm32
cargo metadata --locked --no-deps --format-version 1 > "$task_logs/cargo-metadata.json"
task_failed=0
run() {
    local task_name=$1
    shift
    local task_result=0
    "$@" > "$task_logs/$task_name.log" 2>&1 || task_result=$?
    printf '%s\n' "$task_result" > "$task_logs/$task_name.status"
    return "$task_result"
}
run main wasm-pack test --chrome --headless -- --locked || task_failed=1
run worker wasm-pack test --chrome --headless -- --locked --features worker || task_failed=1
cd "$task_repo/core"
run format cargo fmt --all -- --check || task_failed=1
cd "$task_repo"
run diff git diff --check "$task_product^2" "$task_product" || task_failed=1
git rev-parse HEAD > "$task_logs/head.after"
git rev-parse HEAD^{tree} > "$task_logs/tree.after"
git status --porcelain > "$task_logs/status.after"
printf '%s\n' "$task_failed" > "$task_logs/driver.status"
exit "$task_failed"
