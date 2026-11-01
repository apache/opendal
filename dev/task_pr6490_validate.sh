#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail
test "$#" -eq 2
task_repo=$1
task_logs=$2
mkdir -p "$task_logs"
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.before"
git rev-parse 'HEAD^' > "$task_logs/parent.before"
git status --porcelain > "$task_logs/status.before"
git merge-base --is-ancestor 2c01cb19fcf8d397e5200d29facc8dbea8d0009f HEAD
git diff --name-only 2c01cb19fcf8d397e5200d29facc8dbea8d0009f HEAD > "$task_logs/task-paths.txt"
test "$(cat "$task_logs/task-paths.txt")" = $'dev/task_pr6490_check_stubs.sh\ndev/task_pr6490_validate.sh'
test ! -s "$task_logs/status.before"
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export RUSTUP_TOOLCHAIN=1.99.1
export CARGO_BUILD_JOBS=4
export TASK_PR6490_REPO="$task_repo"
export OPENDAL_TEST=fs
export OPENDAL_FS_ROOT="$task_logs/fs"
mkdir -p "$OPENDAL_FS_ROOT"
rustc --version > "$task_logs/rustc.version"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
task_failed=0
run_check() {
    local task_name=$1
    shift
    local task_status=0
    "$@" > "$task_logs/$task_name.log" 2>&1 || task_status=$?
    printf '%s\n' "$task_status" > "$task_logs/$task_name.status"
    if test "$task_status" -ne 0; then task_failed=1; fi
}
cd bindings/python
run_check sync uv sync --managed-python --python 3.11 --frozen --no-install-project --group dev --group test --group lint
test "$(cat "$task_logs/sync.status")" -eq 0
run_check resolve cargo generate-lockfile
test "$(cat "$task_logs/resolve.status")" -eq 0
sha256sum Cargo.lock > "$task_logs/cargo-lock.sha256"
run_check dependency-tree cargo tree --locked --invert pyo3 --depth 1
run_check generate cargo run --locked --quiet --manifest-path=../../dev/Cargo.toml -- generate -l python
run_check build uv run --no-sync maturin develop --locked --no-default-features -F services-fs --generate-stubs
if test "$(cat "$task_logs/build.status")" -eq 0; then
    run_check postprocess uv run --no-sync python scripts/postprocess_stubs.py
    run_check generated-format uv run --no-sync ruff format python/opendal
    run_check generated-imports uv run --no-sync ruff check --unsafe-fixes --fix python/opendal
    run_check canonical bash "$task_repo/dev/task_pr6490_check_stubs.sh" "$task_repo" "$task_logs/canonical"
    run_check provenance uv run --no-sync python - <<'PY'
from pathlib import Path
import hashlib
import os
import opendal
import opendal._opendal as native

expected = Path(os.environ["TASK_PR6490_REPO"]) / "bindings/python/python/opendal"
actual = Path(native.__file__).resolve()
assert actual.parent == expected.resolve(), (actual, expected)
print("package", opendal.__file__)
print("native", actual)
print("native_sha256", hashlib.sha256(actual.read_bytes()).hexdigest())
PY
    if test "$(cat "$task_logs/provenance.status")" -eq 0; then
        run_check tests uv run --no-sync pytest -vv --basetemp "$task_logs/pytest" tests/test_operator_construction.py tests/test_from_config.py tests/test_from_uri.py
    fi
    run_check ruff uv run --no-sync ruff check
    run_check types uv run --no-sync ty check
fi
run_check clippy cargo clippy --locked --no-default-features --features services-fs -- -D warnings -D clippy::dbg_macro -A clippy::incompatible_msrv
run_check format cargo fmt --all -- --check
cd "$task_repo"
git diff --name-only > "$task_logs/generated-paths.txt"
run_check tracked-clean git diff --quiet
git rev-parse HEAD > "$task_logs/head.after"
git status --porcelain > "$task_logs/status.after"
printf '%s\n' "$task_failed" > "$task_logs/driver.status"
exit "$task_failed"
