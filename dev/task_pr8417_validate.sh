#!/usr/bin/env bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"

if [[ $# != 2 ]]; then
    printf 'Usage: %s WORKTREE ABSOLUTE_LOG_DIRECTORY\n' "$0" >&2
    exit 2
fi

repo=$1
logdir=$2
expected_head=bbb976aaad184a8b32f6cb14872e6f6002c4f93e
[[ "$logdir" == /* ]]
[[ "$(git -C "$repo" rev-parse HEAD)" == "$expected_head" ]]
[[ -z "$(git -C "$repo" status --porcelain=v1)" ]]
mkdir "$logdir"
exec > "$logdir/driver.log" 2>&1

git -C "$repo" rev-parse HEAD > "$logdir/head.sha"
printf '%s\n' "$repo" > "$logdir/worktree.path"
git -C "$repo" show -s --format='%H %P %T' HEAD > "$logdir/provenance.txt"
git -C "$repo" status --porcelain=v1 > "$logdir/worktree-status-before.txt"
sha256sum "$0" > "$logdir/driver.sha256"
cd "$repo/core"
rustc --version > "$logdir/toolchain.txt"
cargo --version >> "$logdir/toolchain.txt"
export CARGO_BUILD_JOBS=4
printf 'CARGO_BUILD_JOBS=%s\n' "$CARGO_BUILD_JOBS" >> "$logdir/toolchain.txt"

printf 'Waiting for the shared Rust build lock\n'
exec 9> /tmp/opendal-pr-delivery-20261031-build.lock
flock -x 9
failed=0

run_check() {
    local name=$1
    shift
    printf '%q ' "$@" > "$logdir/$name.command"
    printf '\n' >> "$logdir/$name.command"
    printf 'Starting %s\n' "$name"
    set +e
    "$@" > "$logdir/$name.log" 2>&1
    local check_exit=$?
    set -e
    printf '%s\n' "$check_exit" > "$logdir/$name.status"
    printf 'Finished %s: %s\n' "$name" "$check_exit"
    if [[ "$check_exit" != 0 ]]; then
        failed=1
    fi
}

run_check 01-fs-regression cargo test --locked -p opendal-service-fs --lib test_list_includes_dangling_symlink
run_check 02-clippy cargo clippy --locked -p opendal-core -p opendal-service-fs --all-targets -- -D warnings
run_check 03-fmt cargo fmt --all -- --check

git -C "$repo" rev-parse HEAD > "$logdir/head-after.sha"
git -C "$repo" status --porcelain=v1 > "$logdir/worktree-status-after.txt"
[[ "$(cat "$logdir/head-after.sha")" == "$expected_head" ]]
[[ ! -s "$logdir/worktree-status-after.txt" ]]
printf '%s\n' "$failed" > "$logdir/driver.status"
exit "$failed"
