#!/usr/bin/env bash
set -Eeuo pipefail

# Task-only validation of the isolated SFTP timeout candidate.
task_repo=$1
task_logs=$2
task_transport=$3
task_product=87ded55d08f59411ad8f5786777b14e5572878cf
task_runner=$(git -C "$(dirname "$0")/../.." rev-parse --show-toplevel)
[[ $(git -C "$task_runner" rev-parse HEAD) == "$task_transport" ]]
[[ -z $(git -C "$task_repo" status --porcelain) ]]
[[ -z ${CARGO_TARGET_DIR:-} && -z ${CARGO_BUILD_BUILD_DIR:-} ]]
mkdir -p "$(dirname "$task_logs")"
mkdir "$task_logs"
task_original=$(git -C "$task_repo" rev-parse HEAD)
task_active=0
task_result=0
task_project=""
task_compose=()

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
    if [[ $task_active == 1 ]]; then
        "${task_compose[@]}" logs --no-color >"$task_logs/interrupted-container.log" 2>&1 || true
        "${task_compose[@]}" down --volumes --remove-orphans >"$task_logs/interrupted-cleanup.log" 2>&1 || task_exit=1
    fi
    rm -f "$task_logs/test-key"
    git -C "$task_repo" status --porcelain >"$task_logs/before-restore.status"
    if [[ ! -s "$task_logs/before-restore.status" ]]; then
        git -C "$task_repo" checkout --detach "$task_original" >"$task_logs/restore.log" 2>&1 || task_exit=1
    else
        task_exit=1
    fi
    git -C "$task_repo" rev-parse HEAD HEAD^{tree} >"$task_logs/restored-head-tree.log"
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
export RUST_LOG=off
git -C "$task_repo" checkout --detach "$task_transport" >"$task_logs/checkout.log" 2>&1
[[ $(git -C "$task_repo" rev-parse HEAD:core/services/sftp) == $(git -C "$task_repo" rev-parse "$task_product:core/services/sftp") ]]
[[ $(git -C "$task_repo" rev-parse HEAD:core/Cargo.lock) == $(git -C "$task_repo" rev-parse "$task_product:core/Cargo.lock") ]]
git -C "$task_repo" rev-parse HEAD HEAD^{tree} >"$task_logs/before-head-tree.log"
rustc -Vv >"$task_logs/rustc-version.log"
cargo -V >"$task_logs/cargo-version.log"
export TASK_PR7424_CODE="$task_runner/dev/task_pr7424"
export TASK_PR7424_REPO="$task_repo"
export TASK_PR7424_KEY="$task_logs/test-key"
export TASK_PR7424_ENDPOINT=ssh://127.0.0.1:22222
install -m 600 "$task_repo/fixtures/sftp/test_ssh_key" "$TASK_PR7424_KEY"
sha256sum "$TASK_PR7424_KEY" >"$task_logs/fixture-key.sha256"
cd "$task_repo/core"
cargo metadata --locked --no-deps --format-version 1 >"$task_logs/workspace-metadata.json"
run_step units cargo test --locked -p opendal-service-sftp --lib
run_step docs cargo test --locked -p opendal-service-sftp --doc
run_step clippy cargo clippy --locked -p opendal-service-sftp --all-targets -- -D warnings
run_step fmt cargo fmt --all -- --check
cd "$task_repo"
for task_language in python java dotnet; do
    run_step "generate-$task_language" cargo run --locked --quiet --manifest-path dev/Cargo.toml -- generate -l "$task_language"
done
run_step generated-rust-fmt cargo fmt --manifest-path bindings/dotnet/Cargo.toml
run_step generated-clean git diff --exit-code
cd "$task_repo/core"
run_step build cargo build --locked --example task_pr7424_timeout --features services-sftp

for task_mode in slow fast; do
    task_case="$task_logs/$task_mode"
    mkdir "$task_case"
    task_delay=12
    [[ $task_mode != fast ]] || task_delay=1
    printf '%s\n' "$task_delay" >"$task_case/delay"
    export TASK_PR7424_CASE="$task_case"
    export TASK_PR7424_SSH_CONFIG="$task_case/ssh_config"
    export TASK_PR7424_SSH_LOG="$task_case/ssh-commands.log"
    export TASK_PR7424_EXPECTED="$task_mode"
    cat >"$TASK_PR7424_SSH_CONFIG" <<EOF
Host *
    UserKnownHostsFile $task_case/known_hosts
    GlobalKnownHostsFile /dev/null
    IdentityAgent none
    IdentitiesOnly yes
    BatchMode yes
    StrictHostKeyChecking no
EOF
    task_project="od7424candidate20261101$task_mode"
    task_compose=(docker compose -p "$task_project" -f "$TASK_PR7424_CODE/compose.yml")
    task_existing=$(docker compose -p "$task_project" -f "$TASK_PR7424_CODE/compose.yml" ps -a -q)
    [[ -z $task_existing ]]
    task_active=1
    docker compose -p "$task_project" -f "$TASK_PR7424_CODE/compose.yml" config >"$task_case/compose-config.yml"
    docker compose -p "$task_project" -f "$TASK_PR7424_CODE/compose.yml" up -d --wait --wait-timeout 180 >"$task_case/compose-up.log" 2>&1
    docker image inspect atmoz/sftp:debian --format '{{.Id}} {{json .RepoDigests}}' >"$task_case/image.log"
    docker compose -p "$task_project" -f "$TASK_PR7424_CODE/compose.yml" exec -T sftp /usr/sbin/sshd -T >"$task_case/sshd-effective.log"
    task_probe_rc=0
    PATH="$TASK_PR7424_CODE:$PATH" timeout 45 cargo run --locked --example task_pr7424_timeout --features services-sftp >"$task_case/opendal-probe.log" 2>&1 || task_probe_rc=$?
    printf '%s\n' "$task_probe_rc" >"$task_case/opendal-probe.exit"
    [[ $task_probe_rc == 0 ]] || task_result=1
    sleep 3
    task_ssh_rc=0
    task_begin=$(date -u +%s)
    timeout 45 "$TASK_PR7424_CODE/ssh" -o BatchMode=yes -i "$TASK_PR7424_KEY" -p 22222 foo@127.0.0.1 true >"$task_case/direct-ssh.stdout" 2>"$task_case/direct-ssh.stderr" || task_ssh_rc=$?
    task_end=$(date -u +%s)
    printf '%s\n' "$task_ssh_rc" >"$task_case/direct-ssh.exit"
    printf '%s\n' "$((task_end-task_begin))" >"$task_case/direct-ssh-seconds.log"
    [[ $task_ssh_rc == 255 ]] || task_result=1
    rg -qi 'Permission denied' "$task_case/direct-ssh.stderr" || task_result=1
    docker compose -p "$task_project" -f "$TASK_PR7424_CODE/compose.yml" logs --no-color >"$task_case/container.log" 2>&1
    docker compose -p "$task_project" -f "$TASK_PR7424_CODE/compose.yml" exec -T sftp cat /tmp/pr7424-auth.events >"$task_case/auth-events.log"
    rg -q "TASK_AUTH_BEGIN delay=$task_delay" "$task_case/auth-events.log" || task_result=1
    rg -q "TASK_AUTH_REJECT delay=$task_delay" "$task_case/auth-events.log" || task_result=1
    docker compose -p "$task_project" -f "$TASK_PR7424_CODE/compose.yml" down --volumes --remove-orphans >"$task_case/cleanup.log" 2>&1
    task_active=0
done
cmp "$task_logs/slow/image.log" "$task_logs/fast/image.log"
[[ $task_result == 0 ]] || exit "$task_result"

task_case="$task_logs/normal"
mkdir "$task_case"
export TASK_PR7424_SSH_CONFIG="$task_case/ssh_config"
export TASK_PR7424_SSH_LOG="$task_case/ssh-commands.log"
cat >"$TASK_PR7424_SSH_CONFIG" <<EOF
Host *
    UserKnownHostsFile $task_case/known_hosts
    GlobalKnownHostsFile /dev/null
    IdentityAgent none
    IdentitiesOnly yes
    BatchMode yes
    StrictHostKeyChecking no
EOF
task_project=od7424candidate20261101normal
task_compose=(docker compose -p "$task_project" -f "$task_repo/fixtures/sftp/docker-compose-sftp.yml" -f "$TASK_PR7424_CODE/normal-override.yml")
[[ -z $("${task_compose[@]}" ps -a -q) ]]
task_active=1
"${task_compose[@]}" config >"$task_case/compose-config.yml"
"${task_compose[@]}" up -d --wait --wait-timeout 180 >"$task_case/compose-up.log" 2>&1
docker image inspect atmoz/sftp:debian --format '{{.Id}} {{json .RepoDigests}}' >"$task_case/image.log"
cmp "$task_logs/slow/image.log" "$task_case/image.log"
"${task_compose[@]}" exec -T sftp /usr/sbin/sshd -T >"$task_case/sshd-effective.log"
unset OPENDAL_TEST_IO_TIMEOUT OPENDAL_SFTP_CONNECT_TIMEOUT OPENDAL_SFTP_ACQUIRE_TIMEOUT
export OPENDAL_TEST=sftp
export OPENDAL_SFTP_ENDPOINT=ssh://127.0.0.1:22222
export OPENDAL_SFTP_ROOT=/upload/sftp_test/
export OPENDAL_SFTP_USER=foo
export OPENDAL_SFTP_KEY="$TASK_PR7424_KEY"
export OPENDAL_SFTP_KNOWN_HOSTS_STRATEGY=accept
export RUST_LOG=warn
PATH="$TASK_PR7424_CODE:$PATH" run_step behavior cargo test --locked --features tests,services-sftp behavior -- --test-threads=3 || task_result=1
"${task_compose[@]}" logs --no-color >"$task_case/container.log" 2>&1
"${task_compose[@]}" down --volumes --remove-orphans >"$task_case/cleanup.log" 2>&1
task_active=0

git -C "$task_repo" status --porcelain >"$task_logs/after-status.log"
git -C "$task_repo" rev-parse HEAD HEAD^{tree} >"$task_logs/after-head-tree.log"
[[ ! -s "$task_logs/after-status.log" ]]
exit "$task_result"
