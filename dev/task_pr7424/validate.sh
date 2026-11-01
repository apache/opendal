#!/usr/bin/env bash
set -Eeuo pipefail

# Task-only real-sshd diagnostic; no service implementation changes.
task_repo=$1
task_logs=$2
task_transport=$3
task_base=f3701cd16e4530d8b7d36047c77d359d94c6e924
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

cleanup() {
    task_exit=$?
    trap - EXIT
    if [[ $task_active == 1 ]]; then
        docker compose -p "$task_project" -f "$TASK_PR7424_CODE/compose.yml" logs --no-color >"$task_logs/interrupted-container.log" 2>&1 || true
        docker compose -p "$task_project" -f "$TASK_PR7424_CODE/compose.yml" down --volumes --remove-orphans >"$task_logs/interrupted-cleanup.log" 2>&1 || task_exit=1
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
[[ $(git -C "$task_repo" rev-parse HEAD:core/services/sftp) == $(git -C "$task_repo" rev-parse "$task_base:core/services/sftp") ]]
[[ $(git -C "$task_repo" rev-parse HEAD:core/Cargo.lock) == $(git -C "$task_repo" rev-parse "$task_base:core/Cargo.lock") ]]
git -C "$task_repo" rev-parse HEAD HEAD^{tree} >"$task_logs/before-head-tree.log"
rustc -Vv >"$task_logs/rustc-version.log"
cargo -V >"$task_logs/cargo-version.log"
export TASK_PR7424_CODE="$task_runner/dev/task_pr7424"
export TASK_PR7424_KEY="$task_logs/test-key"
export TASK_PR7424_ENDPOINT=ssh://127.0.0.1:22222
install -m 600 "$task_repo/fixtures/sftp/test_ssh_key" "$TASK_PR7424_KEY"
sha256sum "$TASK_PR7424_KEY" >"$task_logs/fixture-key.sha256"
cd "$task_repo/core"
cargo metadata --locked --no-deps --format-version 1 >"$task_logs/workspace-metadata.json"
cargo build --locked --example task_pr7424_timeout --features services-sftp >"$task_logs/build.log" 2>&1
printf '0\n' >"$task_logs/build.exit"

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
    task_project="od7424timeout20261101$task_mode"
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
    rg -q "TASK_AUTH_BEGIN delay=$task_delay" "$task_case/container.log" || task_result=1
    rg -q "TASK_AUTH_REJECT delay=$task_delay" "$task_case/container.log" || task_result=1
    docker compose -p "$task_project" -f "$TASK_PR7424_CODE/compose.yml" down --volumes --remove-orphans >"$task_case/cleanup.log" 2>&1
    task_active=0
done
cmp "$task_logs/slow/image.log" "$task_logs/fast/image.log"
git -C "$task_repo" status --porcelain >"$task_logs/after-status.log"
git -C "$task_repo" rev-parse HEAD HEAD^{tree} >"$task_logs/after-head-tree.log"
[[ ! -s "$task_logs/after-status.log" ]]
exit "$task_result"
