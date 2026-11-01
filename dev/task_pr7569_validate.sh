#!/usr/bin/env bash
set -Eeuo pipefail

task_repo=$1
task_logs=$2
task_transport=$3
task_product=b694f5c6c331d66bca798b86ec739487c4a404dc
task_runner=$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel)
[[ $(git -C "$task_runner" rev-parse HEAD) == "$task_transport" ]]
[[ -z ${CARGO_TARGET_DIR:-} && -z ${CARGO_BUILD_BUILD_DIR:-} ]]
mkdir -p "$(dirname "$task_logs")"
mkdir "$task_logs"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9

[[ -z $(git -C "$task_repo" status --porcelain) ]]
task_original=$(git -C "$task_repo" rev-parse HEAD)
[[ $task_original == 4fe5f7c2fe1dd9dd3897cf3e9fcf21fae86c67c1 ]]
task_lock_path="$task_repo/integrations/dav-server/Cargo.lock"
task_lock_existed=0
task_state=$(mktemp -d /tmp/opendal-pr7569-state.XXXXXX)
if [[ -f $task_lock_path ]]; then
    [[ -z $(git -C "$task_repo" ls-files integrations/dav-server/Cargo.lock) ]]
    cp -p "$task_lock_path" "$task_state/original-Cargo.lock"
    sha256sum "$task_lock_path" >"$task_logs/original-ignored-lock.sha256"
    task_lock_existed=1
fi

run_step() {
    local task_step=$1
    shift
    local task_rc=0
    "$@" >"$task_logs/$task_step.log" 2>&1 || task_rc=$?
    printf '%s\n' "$task_rc" >"$task_logs/$task_step.exit"
    return "$task_rc"
}

cleanup() {
    task_exit=$?
    trap - EXIT
    git -C "$task_repo" status --porcelain >"$task_logs/before-restore.status"
    if [[ ! -s "$task_logs/before-restore.status" ]] &&
        git -C "$task_repo" checkout --detach "$task_original" >"$task_logs/restore.log" 2>&1; then
        task_lock_restored=1
        if [[ $task_lock_existed == 1 ]]; then
            cp -p "$task_state/original-Cargo.lock" "$task_lock_path" || task_lock_restored=0
            sha256sum -c "$task_logs/original-ignored-lock.sha256" >"$task_logs/ignored-lock-restored.log" 2>&1 || task_lock_restored=0
        else
            [[ ! -f $task_lock_path ]] || task_lock_restored=0
        fi
        if [[ $task_lock_restored == 1 ]]; then
            rm -f "$task_state/original-Cargo.lock"
            rmdir "$task_state" || task_exit=1
        else
            printf '%s\n' "$task_state" >"$task_logs/preserved-state-path.log"
            task_exit=1
        fi
    else
        printf '%s\n' "$task_state" >"$task_logs/preserved-state-path.log"
        task_exit=1
    fi
    git -C "$task_repo" rev-parse HEAD HEAD^{tree} >"$task_logs/restored-head-tree.log"
    git -C "$task_repo" status --porcelain >"$task_logs/restored-status.log"
    printf '%s\n' "$task_exit" >"$task_logs/driver.exit"
    exit "$task_exit"
}
trap cleanup EXIT

export PATH="/home/xuanwo/.cargo/bin:$PATH"
export RUSTUP_TOOLCHAIN=1.99.1
export CARGO_BUILD_JOBS=4
export RUST_BACKTRACE=1
git -C "$task_repo" checkout --detach "$task_transport" >"$task_logs/checkout.log" 2>&1
git -C "$task_repo" diff --exit-code "$task_product" "$task_transport" -- integrations/dav-server/src integrations/dav-server/tests >"$task_logs/runtime-reuse-proof.log"
git -C "$task_repo" rev-parse HEAD HEAD^{tree} >"$task_logs/before-head-tree.log"
sha256sum "$task_lock_path" >"$task_logs/test-lock.sha256"
rustc -Vv >"$task_logs/rustc-version.log"
cargo -V >"$task_logs/cargo-version.log"
cd "$task_repo/integrations/dav-server"
cargo metadata --locked --no-deps --format-version 1 >"$task_logs/workspace-metadata.json"
run_step tests cargo test --locked --all-targets
run_step docs cargo test --locked --doc
run_step clippy cargo clippy --locked --all-targets -- -D warnings
run_step fmt cargo fmt -- --check
run_step diff git -C "$task_repo" diff --exit-code
git -C "$task_repo" rev-parse HEAD HEAD^{tree} >"$task_logs/after-head-tree.log"
