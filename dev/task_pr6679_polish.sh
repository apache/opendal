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
git diff --quiet 8e8c0994d836ab2370b7394c8c395f94bc67895f HEAD -- bindings/python/src bindings/python/Cargo.toml bindings/python/pyproject.toml bindings/python/uv.lock
test "$(git hash-object bindings/python/python/opendal/operator.pyi)" = 10f8ab54445b3e5a828d5f1d5f502237ba88704a
sha256sum bindings/python/Cargo.lock > "$task_logs/lock.before.sha256"
run() {
    local task_name=$1
    shift
    local task_result=0
    "$@" > "$task_logs/$task_name.log" 2>&1 || task_result=$?
    printf '%s\n' "$task_result" > "$task_logs/$task_name.status"
    return "$task_result"
}
cd bindings/python
run provenance uv run --no-sync python - <<'PY'
from pathlib import Path
import hashlib
import json
import sys
import opendal
import opendal._opendal as native

library = Path(native.__file__).resolve()
assert library.parent == (Path.cwd() / "python/opendal").resolve(), library
digest = hashlib.sha256(library.read_bytes()).hexdigest()
assert digest == "d0f9a083bc34b8469906710ee005938439de00679bce7aaf7d687c9ada18a5d0", digest
print(json.dumps({"python": sys.version, "package": opendal.__file__, "native": str(library), "sha256": digest, "reuse": "Rust source and dependency inputs unchanged from canonical a9c8ea0 campaign"}, indent=2))
PY
export OPENDAL_TEST=fs OPENDAL_FS_ROOT="$task_logs/fs"
task_failed=0
run pytest uv run --no-sync pytest -vv tests/test_delete_many.py || task_failed=1
run ruff uv run --no-sync ruff check || task_failed=1
run test-format uv run --no-sync ruff format --check tests/test_delete_many.py || task_failed=1
cd "$task_repo"
run diff-check git diff --check || task_failed=1
git rev-parse HEAD > "$task_logs/head.after"
git rev-parse HEAD^{tree} > "$task_logs/tree.after"
git status --porcelain > "$task_logs/status.after"
test ! -s "$task_logs/status.after" || task_failed=1
sha256sum bindings/python/Cargo.lock > "$task_logs/lock.after.sha256"
cmp "$task_logs/lock.before.sha256" "$task_logs/lock.after.sha256" || task_failed=1
printf '%s\n' "$task_failed" > "$task_logs/driver.status"
exit "$task_failed"
