#!/usr/bin/env bash
set -euo pipefail
proof_logs=$1
proof_base=$2
proof_head=$3
proof_worktree=/home/xuanwo/Code/worktrees/od-review-7569-20261031
mkdir -p "$proof_logs"
exec >"$proof_logs/runner.log" 2>&1
finish() {
  proof_status=$?
  printf '%s\n' "$proof_status" > "$proof_logs/done.status"
}
trap finish EXIT
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_BUILD_JOBS=4
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
printf 'Waiting for the shared build lock\n'
flock 9
cd "$proof_worktree"
test -z "$(git status --porcelain)"
rustc --version
cargo --version
for proof_label in baseline head; do
  if [ "$proof_label" = baseline ]; then
    proof_sha=$proof_base
    proof_expected=0
  else
    proof_sha=$proof_head
    proof_expected=101
  fi
  git checkout --detach "$proof_sha"
  test -z "$(git status --porcelain)"
  git rev-parse HEAD > "$proof_logs/$proof_label.sha"
  git rev-parse HEAD:integrations/dav-server/tests/single_shot_streaming.rs > "$proof_logs/$proof_label.probe-blob"
  cd "$proof_worktree/integrations/dav-server"
  set +e
  cargo test --test single_shot_streaming -- --nocapture > "$proof_logs/$proof_label.log" 2>&1
  proof_actual=$?
  set -e
  printf '%s\n' "$proof_actual" > "$proof_logs/$proof_label.status"
  test "$proof_actual" -eq "$proof_expected"
  if [ "$proof_label" = baseline ]; then
    rg -q 'test result: ok. 2 passed; 0 failed' "$proof_logs/$proof_label.log"
  else
    rg -q 'test result: FAILED. 0 passed; 2 failed' "$proof_logs/$proof_label.log"
    rg -q 'GeneralFailure' "$proof_logs/$proof_label.log"
  fi
  cd "$proof_worktree"
done
cmp "$proof_logs/baseline.probe-blob" "$proof_logs/head.probe-blob"
test -z "$(git status --porcelain)"
printf 'Expected baseline success and PR-head regression observed; this is not patch acceptance.\n'
