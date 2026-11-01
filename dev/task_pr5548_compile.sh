#!/usr/bin/env bash
set -u
: "${LOG_DIR:?LOG_DIR must be explicit}"
: "${BASE_HEAD:?BASE_HEAD must be explicit}"
[ -z "${CARGO_TARGET_DIR:-}" ] || exit 2
mkdir -p "$LOG_DIR"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
export CARGO_BUILD_JOBS=4
repo=$(git rev-parse --show-toplevel)
cd "$repo"
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then exit 2; fi
git rev-parse HEAD HEAD^{tree} > "$LOG_DIR/source-head.txt"
rustc -V > "$LOG_DIR/rustc-version.log"
cargo -V > "$LOG_DIR/cargo-version.log"
cd core
cargo test --locked --features tests,services-gcs --test gcs_version_list_probe --no-run > "$LOG_DIR/compile.log" 2>&1
compile_status=$?
printf '%s\n' "$compile_status" > "$LOG_DIR/compile.status"
cd "$repo"
git diff --exit-code > "$LOG_DIR/tracked-clean.log" 2>&1
clean_status=$?
git switch --detach "$BASE_HEAD" > "$LOG_DIR/restore.log" 2>&1
restore_status=$?
git rev-parse HEAD HEAD^{tree} > "$LOG_DIR/restored-head.txt"
git status --porcelain --untracked-files=no >> "$LOG_DIR/restored-head.txt"
result=$compile_status
if [ "$clean_status" -ne 0 ] || [ "$restore_status" -ne 0 ]; then result=1; fi
printf '%s\n' "$result" > "$LOG_DIR/driver.exit"
exit "$result"
