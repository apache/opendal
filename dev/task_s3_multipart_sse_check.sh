#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
: "${LOG_DIR:?}"
: "${RESTORE_HEAD:?}"
test ! -e "$LOG_DIR"
mkdir "$LOG_DIR"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
export CARGO_BUILD_JOBS=4 CARGO_TERM_COLOR=never
red_head=488d55ed8ec85c1e105b757eae59ba760b92cd06
fix_head=8e9c35cc53a44dc0612e423e6f49b802d56d256f

finish() {
    task_exit=$?
    trap - EXIT
    set +e
    git status --porcelain=v1 --untracked-files=all >"$LOG_DIR/final-source-status.txt"
    git rev-parse HEAD 'HEAD^{tree}' >"$LOG_DIR/final-source-head.txt"
    git checkout --detach "$RESTORE_HEAD" >"$LOG_DIR/restore.log" 2>&1
    restore_exit=$?
    printf '%s\n' "$restore_exit" >"$LOG_DIR/restore.status"
    git rev-parse HEAD 'HEAD^{tree}' >"$LOG_DIR/restored-head.txt"
    git status --porcelain=v1 --untracked-files=all >"$LOG_DIR/restored-status.txt"
    if test "$restore_exit" -ne 0 || test -s "$LOG_DIR/restored-status.txt"; then
        task_exit=1
    fi
    printf '%s\n' "$task_exit" >"$LOG_DIR/driver.exit"
    exit "$task_exit"
}
trap finish EXIT

run_stage() {
    stage=$1
    shift
    stage_exit=0
    "$@" >"$LOG_DIR/$stage.log" 2>&1 || stage_exit=$?
    printf '%s\n' "$stage_exit" >"$LOG_DIR/$stage.status"
    return "$stage_exit"
}

git rev-parse HEAD 'HEAD^{tree}' >"$LOG_DIR/carrier-head.txt"
git status --porcelain=v1 --untracked-files=all >"$LOG_DIR/pre-status.txt"
test ! -s "$LOG_DIR/pre-status.txt"
test "$(git rev-parse HEAD^)" = "$fix_head"
test "$(git diff --name-only "$fix_head" HEAD)" = dev/task_s3_multipart_sse_check.sh
sha256sum dev/task_s3_multipart_sse_check.sh >"$LOG_DIR/driver.sha256"
rustc --version >"$LOG_DIR/toolchain.txt"
cargo --version >>"$LOG_DIR/toolchain.txt"

git checkout --detach "$red_head" >"$LOG_DIR/red-checkout.log" 2>&1
git rev-parse HEAD 'HEAD^{tree}' >"$LOG_DIR/red-head.txt"
cd core
run_stage red cargo test --locked --offline -p opendal-service-s3 --lib backend::tests::test_multipart_sse_kms_header_scope -- --exact || true
test "$(cat "$LOG_DIR/red.status")" = 101
rg -Fq 'test result: FAILED. 0 passed; 1 failed;' "$LOG_DIR/red.log"
rg -Fq 'assertion failed: !request.headers.contains_key("x-amz-server-side-encryption")' "$LOG_DIR/red.log"
cd ..

git checkout --detach "$fix_head" >"$LOG_DIR/fix-checkout.log" 2>&1
git rev-parse HEAD 'HEAD^{tree}' >"$LOG_DIR/fix-head.txt"
cd core
run_stage unit cargo test --locked --offline -p opendal-service-s3 --lib
run_stage clippy cargo clippy --locked --offline -p opendal-service-s3 --all-targets -- -D warnings
run_stage doc cargo test --locked --offline -p opendal-service-s3 --doc
run_stage fmt cargo fmt --all -- --check
cd ..
run_stage diff-check git diff --check
test -z "$(git status --porcelain=v1 --untracked-files=all)"
