#!/usr/bin/env bash
set -euo pipefail
task_repo=$1
task_logs=$2
task_product=$3
task_head=$4
mkdir -p "$task_logs/fs"
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.before"
git rev-parse HEAD^ > "$task_logs/parent.before"
git rev-parse HEAD^{tree} > "$task_logs/tree.before"
git status --porcelain > "$task_logs/status.before"
test "$(cat "$task_logs/head.before")" = "$task_head"
test "$(cat "$task_logs/parent.before")" = "$task_product"
test ! -s "$task_logs/status.before"
exec 9>/tmp/opendal-pr-delivery-20261031-build.lock
flock 9
export CARGO_BUILD_JOBS=4 RUSTUP_TOOLCHAIN=1.99.1
rustc --version > "$task_logs/rustc.version"
cargo --version > "$task_logs/cargo.version"
task_failed=0
run() {
    local task_name=$1
    shift
    local task_result=0
    "$@" > "$task_logs/$task_name.log" 2>&1 || task_result=$?
    printf '%s\n' "$task_result" > "$task_logs/$task_name.status"
    return "$task_result"
}
cd bindings/python
run sync uv sync --frozen --no-install-project --python 3.11 --group dev --group test --group lint
run build uv run --no-sync maturin develop --locked --generate-stubs
run postprocess uv run --no-sync python scripts/postprocess_stubs.py
run stub-format uv run --no-sync ruff format python/opendal
run stub-imports uv run --no-sync ruff check --unsafe-fixes --fix --silent python/opendal
run canonical git diff --exit-code -- . || task_failed=1
run provenance uv run --no-sync python - <<'PY'
from pathlib import Path
import hashlib
import json
import sys
import opendal
import opendal._opendal as native
library = Path(native.__file__).resolve()
assert library.parent == (Path.cwd() / "python/opendal").resolve(), library
print(json.dumps({"python": sys.version, "package": opendal.__file__, "native": str(library), "sha256": hashlib.sha256(library.read_bytes()).hexdigest()}, indent=2))
PY
export OPENDAL_TEST=fs OPENDAL_FS_ROOT="$task_logs/fs"
run pytest uv run --no-sync pytest -vv tests/test_write.py tests/test_read.py || task_failed=1
run clippy cargo clippy --locked -- -D warnings -D clippy::dbg_macro -A clippy::incompatible_msrv || task_failed=1
run format cargo fmt --all -- --check || task_failed=1
run ruff uv run --no-sync ruff check || task_failed=1
cd "$task_repo"
git rev-parse HEAD > "$task_logs/head.after"
git rev-parse HEAD^{tree} > "$task_logs/tree.after"
git status --porcelain > "$task_logs/status.after"
printf '%s\n' "$task_failed" > "$task_logs/driver.status"
exit "$task_failed"
