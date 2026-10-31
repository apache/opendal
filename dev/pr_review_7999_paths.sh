#!/usr/bin/env bash
set -euo pipefail
probe_worktree=$1
probe_logs=$2
probe_sha=$3
mkdir -p "$probe_logs"
exec >"$probe_logs/runner.log" 2>&1
finish() {
  probe_status=$?
  printf '%s\n' "$probe_status" > "$probe_logs/done.status"
}
trap finish EXIT
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_BUILD_JOBS=4
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
cd "$probe_worktree"
test -z "$(git status --porcelain)"
test "$(git rev-parse HEAD)" = "$probe_sha"
git rev-parse HEAD
rustc --version
cd core
set +e
cargo test -p opendal --features services-fs --test deleter_path_contract -- --nocapture > "$probe_logs/probe.log" 2>&1
probe_actual=$?
set -e
printf '%s\n' "$probe_actual" > "$probe_logs/probe.status"
test "$probe_actual" -eq 101
rg -q 'test result: FAILED. 0 passed; 1 failed' "$probe_logs/probe.log"
rg -Fq 'PATH_CONTRACT_OBSERVED operator=(false, true) deleter=(true, false)' "$probe_logs/probe.log"
rg -Fq 'Deleter must normalize exactly like Operator::delete' "$probe_logs/probe.log"
printf 'Expected negative real-Fs comparison completed; no patch was applied.\n'
