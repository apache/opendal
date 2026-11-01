#!/usr/bin/env bash
set -Eeuo pipefail

# Task-only experiment. The repair PR contains neither this driver nor the port override.
task_repo=$1
task_logs=$2
task_baseline=$3
task_candidate=$4
task_transport=$5
task_driver_repo=$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel)
[[ $(git -C "$task_driver_repo" rev-parse HEAD) == "$task_transport" ]]
[[ -z $(git -C "$task_repo" status --porcelain) ]]
[[ -z ${CARGO_TARGET_DIR:-} && -z ${CARGO_BUILD_BUILD_DIR:-} ]]
mkdir -p "$(dirname "$task_logs")"
mkdir "$task_logs"
task_original=$(git -C "$task_repo" rev-parse HEAD)
task_result=0
task_compose=()

cleanup() {
    task_exit=$?
    trap - EXIT
    if ((${#task_compose[@]})); then
        "${task_compose[@]}" logs --no-color >"$task_logs/interrupted-container.log" 2>&1 || true
        "${task_compose[@]}" down --volumes --remove-orphans >"$task_logs/interrupted-cleanup.log" 2>&1 || task_exit=1
    fi
    git -C "$task_repo" status --porcelain >"$task_logs/before-restore.status"
    if [[ ! -s "$task_logs/before-restore.status" ]]; then
        git -C "$task_repo" checkout --detach "$task_original" >"$task_logs/restore.log" 2>&1 || task_exit=1
    else
        task_exit=1
    fi
    git -C "$task_repo" rev-parse HEAD HEAD\^{tree} >"$task_logs/restored-head-tree.log"
    printf '%s\n' "$task_exit" >"$task_logs/driver.exit"
    exit "$task_exit"
}
trap cleanup EXIT

exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
export PATH="/home/xuanwo/.cargo/bin:$PATH"
export RUSTUP_TOOLCHAIN=1.99.1
export CARGO_BUILD_JOBS=4
export RUST_BACKTRACE=1
export RUST_LOG=opendal=warn
export OPENDAL_TEST=webdav
export OPENDAL_WEBDAV_ENDPOINT=http://localhost:18080/remote.php/webdav/
export OPENDAL_WEBDAV_USERNAME=admin
export OPENDAL_WEBDAV_PASSWORD=admin
export OPENDAL_WEBDAV_TOKEN=__OPENDAL_TEST_UNSET__
export OPENDAL_WEBDAV_ROOT=/
export OPENDAL_DISABLE_RANDOM_ROOT=false
export OPENDAL_TEST_CAPABILITY_OVERRIDES=write_with_user_metadata=false
export OPENDAL_TEST_IO_TIMEOUT=__OPENDAL_TEST_UNSET__
rustc -Vv >"$task_logs/rustc-version.log"
cargo -V >"$task_logs/cargo-version.log"
docker compose version >"$task_logs/compose-version.log"
printf '%s\n' "$task_original" "$task_baseline" "$task_candidate" "$task_transport" >"$task_logs/source-identities.log"
printf '%s\n' 'Full behavior suite: four test threads. Focused race: one test thread; the case retains its two concurrent calls and eight rounds.' 'RUST_LOG=opendal=warn; RUST_BACKTRACE=1; original default I/O deadline; max_times=4 unchanged.' >"$task_logs/experiment-scope.log"

run_check() {
    task_check_name=$1
    shift
    task_check_rc=0
    "$@" >"$task_logs/$task_check_name.log" 2>&1 || task_check_rc=$?
    printf '%s\n' "$task_check_rc" >"$task_logs/$task_check_name.exit"
    if [[ $task_phase == candidate && $task_check_rc != 0 ]]; then
        task_result=1
    fi
}

cd "$task_repo/core"
for task_phase in baseline candidate; do
    task_sha=$task_baseline
    [[ $task_phase != candidate ]] || task_sha=$task_candidate
    git -C "$task_repo" checkout --detach "$task_sha" >"$task_logs/$task_phase.checkout.log" 2>&1
    git -C "$task_repo" rev-parse HEAD HEAD\^{tree} >"$task_logs/$task_phase.before-head-tree.log"
    git -C "$task_repo" status --porcelain >"$task_logs/$task_phase.before-status.log"
    [[ ! -s "$task_logs/$task_phase.before-status.log" ]]
    task_project="odj0db5c13c$task_phase"
    task_compose=(docker compose -p "$task_project" -f "$task_repo/fixtures/webdav/docker-compose-webdav-owncloud.yml" -f "$task_driver_repo/dev/task_ci_owncloud_port.yml")
    task_existing=$("${task_compose[@]}" ps -a -q)
    [[ -z $task_existing ]]
    "${task_compose[@]}" config >"$task_logs/$task_phase.compose-config.yml"
    "${task_compose[@]}" up -d --wait --wait-timeout 180 >"$task_logs/$task_phase.compose-up.log" 2>&1
    docker image inspect owncloud/server:10.16.0 redis --format '{{.Id}} {{json .RepoDigests}}' >"$task_logs/$task_phase.images.log"
    task_occ_rc=0
    "${task_compose[@]}" exec -T owncloud occ config:system:get memcache.locking >"$task_logs/$task_phase.memcache-locking.log" 2>&1 || task_occ_rc=$?
    printf '%s\n' "$task_occ_rc" >"$task_logs/$task_phase.memcache-locking.exit"
    cargo metadata --locked --no-deps --format-version 1 >"$task_logs/$task_phase.workspace-metadata.json"
    run_check "$task_phase.build" cargo test --locked --features tests,services-webdav --test behavior --no-run
    [[ $task_check_rc == 0 ]]
    run_check "$task_phase.full" cargo test --locked --features tests,services-webdav --test behavior -- --test-threads=4
    for task_round in 1 2 3; do
        run_check "$task_phase.race-$task_round" cargo test --locked --features tests,services-webdav --test behavior test_rename_with_if_not_exists_race -- --test-threads=1
    done
    if [[ $task_phase == candidate ]]; then
        run_check candidate.testkit cargo test --locked -p opendal-testkit --lib
        run_check candidate.clippy cargo clippy --locked -p opendal-testkit --all-targets -- -D warnings
        run_check candidate.fmt cargo fmt --all -- --check
        run_check candidate.diff git -C "$task_repo" diff --check
    fi
    "${task_compose[@]}" logs --no-color >"$task_logs/$task_phase.container.log" 2>&1
    "${task_compose[@]}" down --volumes --remove-orphans >"$task_logs/$task_phase.cleanup.log" 2>&1
    task_compose=()
    git -C "$task_repo" rev-parse HEAD HEAD\^{tree} >"$task_logs/$task_phase.after-head-tree.log"
    git -C "$task_repo" status --porcelain >"$task_logs/$task_phase.after-status.log"
    [[ ! -s "$task_logs/$task_phase.after-status.log" ]]
done
cmp "$task_logs/baseline.images.log" "$task_logs/candidate.images.log"
exit "$task_result"
