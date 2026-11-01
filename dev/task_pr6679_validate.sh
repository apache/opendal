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
uv --version > "$task_logs/uv.version"
hawkeye --version > "$task_logs/hawkeye.version"
task_original_lock=$(mktemp /tmp/opendal-pr6679-lock.XXXXXX)
task_had_lock=0
if test -f bindings/python/Cargo.lock; then
    task_had_lock=1
    cp bindings/python/Cargo.lock "$task_original_lock"
    sha256sum bindings/python/Cargo.lock > "$task_logs/lock.before.sha256"
fi
finish() {
    local task_result=$?
    trap - EXIT
    set +e
    cd "$task_repo" || task_result=1
    if test "$task_had_lock" = 1; then
        cp "$task_original_lock" bindings/python/Cargo.lock || task_result=1
        sha256sum bindings/python/Cargo.lock > "$task_logs/lock.restored.sha256"
        cmp "$task_original_lock" bindings/python/Cargo.lock || task_result=1
    else
        rm -f bindings/python/Cargo.lock || task_result=1
    fi
    rm -f "$task_original_lock"
    git rev-parse HEAD > "$task_logs/head.after"
    git rev-parse HEAD^{tree} > "$task_logs/tree.after"
    git status --porcelain > "$task_logs/status.after"
    printf '%s\n' "$task_result" > "$task_logs/driver.status"
    exit "$task_result"
}
trap finish EXIT
cp dev/task_pr6679_python.lock bindings/python/Cargo.lock
sha256sum bindings/python/Cargo.lock > "$task_logs/lock.used.sha256"
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
run license hawkeye format --config ../../licenserc.toml .
run generated uv run --no-sync python - <<'PY'
from pathlib import Path
import hashlib
import json
import subprocess

paths = subprocess.check_output(["git", "diff", "--name-only"], text=True).splitlines()
assert paths == ["bindings/python/python/opendal/operator.pyi"], paths
stub = Path("python/opendal/operator.pyi")
assert stub.read_text().count("    def delete_many(") == 2
print(json.dumps({"generated": paths, "sha256": hashlib.sha256(stub.read_bytes()).hexdigest()}, indent=2))
PY
run provenance uv run --no-sync python - <<'PY'
from pathlib import Path
import hashlib
import json
import sys
import opendal
import opendal._opendal as native

library = Path(native.__file__).resolve()
assert library.parent == (Path.cwd() / "python/opendal").resolve(), library
assert hasattr(opendal.Operator, "delete_many")
assert hasattr(opendal.AsyncOperator, "delete_many")
print(json.dumps({"python": sys.version, "package": opendal.__file__, "native": str(library), "sha256": hashlib.sha256(library.read_bytes()).hexdigest()}, indent=2))
PY
export OPENDAL_TEST=fs OPENDAL_FS_ROOT="$task_logs/fs"
task_failed=0
run pytest uv run --no-sync pytest -vv tests/test_delete_many.py tests/test_delete_options.py tests/test_sync_delete.py tests/test_async_delete.py tests/test_write.py || task_failed=1
run clippy cargo clippy --locked -- -D warnings -D clippy::dbg_macro -A clippy::incompatible_msrv || task_failed=1
run format cargo fmt --all -- --check || task_failed=1
run ruff uv run --no-sync ruff check || task_failed=1
cd "$task_repo"
run diff-check git diff --check || task_failed=1
exit "$task_failed"
