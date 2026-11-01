#!/usr/bin/env bash
set -u
: "${LOG_DIR:?LOG_DIR must be explicit}"
: "${BASE_HEAD:?BASE_HEAD must be explicit}"
: "${PRODUCT_HEAD:?PRODUCT_HEAD must be explicit}"
[ -z "${CARGO_TARGET_DIR:-}" ] || exit 2
export PATH="/home/xuanwo/.cargo/bin:$PATH"
command -v cargo >/dev/null || exit 127
command -v rustc >/dev/null || exit 127
mkdir -p "$LOG_DIR"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
export CARGO_BUILD_JOBS=4
repo=$(git rev-parse --show-toplevel)
cd "$repo"
[ -z "$(git status --porcelain --untracked-files=no)" ] || exit 2
result=1
restore() {
    cd "$repo" || exit 1
    git switch --detach "$BASE_HEAD" > "$LOG_DIR/restore.log" 2>&1
    if [ "$?" -ne 0 ]; then result=1; fi
    git rev-parse HEAD HEAD^{tree} > "$LOG_DIR/restored-head.txt"
    git status --porcelain >> "$LOG_DIR/restored-head.txt"
    printf '%s\n' "$result" > "$LOG_DIR/driver.exit"
}
trap restore EXIT
git switch --detach "$PRODUCT_HEAD" > "$LOG_DIR/product-checkout.log" 2>&1 || exit 1
[ "$(git rev-parse HEAD)" = "$PRODUCT_HEAD" ] || exit 1
git rev-parse HEAD HEAD^{tree} > "$LOG_DIR/source-head.txt"
rustc -V > "$LOG_DIR/rustc-version.log"
cargo -V > "$LOG_DIR/cargo-version.log"
cd core
cargo test --locked -p opendal-service-gcs --lib > "$LOG_DIR/unit.log" 2>&1
unit_status=$?
printf '%s\n' "$unit_status" > "$LOG_DIR/unit.status"
cargo clippy --locked -p opendal-service-gcs --all-targets -- -D warnings > "$LOG_DIR/clippy.log" 2>&1
clippy_status=$?
printf '%s\n' "$clippy_status" > "$LOG_DIR/clippy.status"
cargo fmt --all -- --check > "$LOG_DIR/fmt.log" 2>&1
fmt_status=$?
printf '%s\n' "$fmt_status" > "$LOG_DIR/fmt.status"
cd "$repo"
git diff --exit-code > "$LOG_DIR/tracked-clean.log" 2>&1
clean_status=$?
printf '%s\n' "$clean_status" > "$LOG_DIR/tracked-clean.status"
if [ "$unit_status" -eq 0 ] && [ "$clippy_status" -eq 0 ] && [ "$fmt_status" -eq 0 ] && [ "$clean_status" -eq 0 ]; then result=0; fi
exit "$result"
