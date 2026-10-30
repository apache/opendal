#!/usr/bin/env python3
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

import asyncio
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def expect_type_error(call):
    try:
        call()
    except TypeError:
        return
    raise AssertionError("expected TypeError")


def probe_sync_inputs(opendal):
    op = opendal.Operator("memory")
    paths = ["sync-a", "sync-b"]
    for path in paths:
        op.write(path, b"payload")
    expect_type_error(lambda: op.delete_many(iter(paths)))
    assert all(op.exists(path) for path in paths)
    expect_type_error(lambda: op.delete([paths[0], object()]))
    assert all(op.exists(path) for path in paths)
    op.delete(iter(Path(path) for path in paths))
    assert all(not op.exists(path) for path in paths)
    op.delete_many([])
    op.write("bytes-path", b"payload")
    expect_type_error(lambda: op.delete([b"bytes-path"]))
    assert op.exists("bytes-path")
    op.delete_many([Path("bytes-path")])
    assert not op.exists("bytes-path")
    return "wrapper iterable works; native generator and bytes rejected"


async def probe_async_inputs(opendal):
    op = opendal.AsyncOperator("memory")
    paths = ["async-a", "async-b"]
    for path in paths:
        await op.write(path, b"payload")
    try:
        await op.delete_many(iter(paths))
    except TypeError:
        pass
    else:
        raise AssertionError("native generator unexpectedly accepted")
    for path in paths:
        assert await op.exists(path)
    try:
        await op.delete([paths[0], object()])
    except TypeError:
        pass
    else:
        raise AssertionError("invalid item unexpectedly accepted")
    for path in paths:
        assert await op.exists(path)
    await op.delete(iter(Path(path) for path in paths))
    for path in paths:
        assert not await op.exists(path)
    await op.delete_many([])
    await op.write("async-bytes", b"payload")
    try:
        await op.delete([b"async-bytes"])
    except TypeError:
        pass
    else:
        raise AssertionError("bytes unexpectedly accepted")
    assert await op.exists("async-bytes")
    await op.delete_many([Path("async-bytes")])
    assert not await op.exists("async-bytes")
    return "wrapper iterable works; native generator and bytes rejected"


def probe_sync_partial(opendal):
    with tempfile.TemporaryDirectory(prefix="opendal-pr6679-replay-sync-") as tmp:
        root = Path(tmp)
        locked = root / "locked"
        locked.mkdir()
        (locked / "file.txt").write_bytes(b"payload")
        op = opendal.Operator("fs", root=str(root))
        op.write("good.txt", b"payload")
        locked.chmod(0o555)
        try:
            try:
                op.delete([Path("good.txt"), Path("locked/file.txt")])
            except Exception as error:
                assert type(error).__name__ == "PermissionDenied", type(error)
            else:
                raise AssertionError("permission failure unexpectedly succeeded")
            assert not (root / "good.txt").exists()
            assert (locked / "file.txt").exists()
        finally:
            locked.chmod(0o755)
    return "PermissionDenied; first path removed and failed path retained"


async def probe_async_partial(opendal):
    with tempfile.TemporaryDirectory(prefix="opendal-pr6679-replay-async-") as tmp:
        root = Path(tmp)
        locked = root / "locked"
        locked.mkdir()
        (locked / "file.txt").write_bytes(b"payload")
        op = opendal.AsyncOperator("fs", root=str(root))
        await op.write("good.txt", b"payload")
        locked.chmod(0o555)
        try:
            try:
                await op.delete([Path("good.txt"), Path("locked/file.txt")])
            except Exception as error:
                assert type(error).__name__ == "PermissionDenied", type(error)
            else:
                raise AssertionError("permission failure unexpectedly succeeded")
            assert not (root / "good.txt").exists()
            assert (locked / "file.txt").exists()
        finally:
            locked.chmod(0o755)
    return "PermissionDenied; first path removed and failed path retained"


def main():
    worktree = Path(sys.argv[1]).resolve()
    expected_sha, expected_native = sys.argv[2:4]
    actual_sha = subprocess.check_output(
        ["git", "-C", str(worktree), "rev-parse", "HEAD"], text=True
    ).strip()
    assert actual_sha == expected_sha
    assert not subprocess.check_output(
        ["git", "-C", str(worktree), "status", "--porcelain", "--untracked-files=no"],
        text=True,
    ).strip()
    assert os.geteuid() != 0, "permission probe needs a non-root user"
    sys.path.insert(0, str(worktree / "bindings/python/python"))
    import opendal
    import opendal._opendal as native

    module = Path(native.__file__).resolve()
    assert module.is_relative_to(worktree)
    native_sha = hashlib.sha256(module.read_bytes()).hexdigest()
    assert native_sha == expected_native
    print(json.dumps({"source_sha": actual_sha, "python": sys.version,
                      "module": str(module), "native_sha256": native_sha}), flush=True)
    results = {
        "sync_inputs": probe_sync_inputs(opendal),
        "async_inputs": asyncio.run(probe_async_inputs(opendal)),
        "sync_partial_fs": probe_sync_partial(opendal),
        "async_partial_fs": asyncio.run(probe_async_partial(opendal)),
    }
    print(json.dumps(results, indent=2), flush=True)


if __name__ == "__main__":
    main()
