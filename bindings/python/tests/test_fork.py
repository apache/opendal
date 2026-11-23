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
import multiprocessing
import subprocess
import sys
from collections.abc import AsyncIterator
from textwrap import dedent

import pytest

import opendal
from opendal.file import AsyncFile
from opendal.types import Entry

pytestmark = [
    pytest.mark.skipif(
        "fork" not in multiprocessing.get_all_start_methods(),
        reason="requires multiprocessing with fork support",
    ),
    pytest.mark.filterwarnings(
        "ignore:This process .* is multi-threaded:DeprecationWarning"
    ),
]

FORK_ERROR = (
    r'OpenDAL.*Tokio runtime.*fork\(\).*multiprocessing\.get_context\("spawn"\)'
)


def _run_child(method, target, *args: object) -> None:
    process = multiprocessing.get_context(method).Process(target=target, args=args)
    process.start()
    try:
        process.join(timeout=10)
        assert process.exitcode is not None, f"{method} child timed out"
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.kill()
            process.join()
        process.close()


@pytest.mark.parametrize("operator_kind", ["inherited", "new"])
def test_blocking_operator_rejects_fork(tmp_path, operator_kind) -> None:
    (tmp_path / "probe").write_bytes(b"ok")
    operator = opendal.Operator("fs", root=tmp_path)
    assert operator.stat("probe").content_length == 2

    def use_operator() -> None:
        if operator_kind == "inherited":
            with pytest.raises(RuntimeError, match=FORK_ERROR):
                operator.stat("probe")
        else:
            with pytest.raises(RuntimeError, match=FORK_ERROR):
                opendal.Operator("fs", root=tmp_path).stat("probe")

    _run_child("fork", use_operator)
    assert operator.stat("probe").content_length == 2


def test_inherited_handles_reject_fork() -> None:
    operator = opendal.Operator("memory")
    operator.write("probe", b"ok")
    async_operator = operator.to_async_operator()

    async def open_handles() -> tuple[AsyncFile, AsyncIterator[Entry]]:
        return await async_operator.open("probe", "rb"), await async_operator.list("/")

    def use_handles() -> None:
        nonlocal reader, writer, lister
        with pytest.raises(RuntimeError, match=FORK_ERROR):
            reader.read()
        with pytest.raises(RuntimeError, match=FORK_ERROR):
            writer.close()
        with pytest.raises(RuntimeError, match=FORK_ERROR):
            next(lister)
        del reader, writer, lister

        async def use_async_handles() -> None:
            for operation in (
                lambda: async_operator.stat("probe"),
                async_reader.read,
                lambda: anext(async_lister),
            ):
                with pytest.raises(RuntimeError, match=FORK_ERROR):
                    await operation()

        asyncio.run(use_async_handles())

    reader = operator.open("probe", "rb")
    writer = operator.open("pending", "wb")
    assert writer.write(b"pending write") == len(b"pending write")
    lister = iter(operator.list("/"))
    async_reader, async_lister = asyncio.run(open_handles())
    _run_child("fork", use_handles)
    assert reader.read() == b"ok"
    writer.close()
    assert operator.read("pending") == b"pending write"
    assert next(lister).path == "probe"


def _check_spawned_operator(root) -> None:
    operator = opendal.Operator("fs", root=root)
    assert operator.stat("probe").content_length == 2


def test_spawn_works_after_runtime_initialization(tmp_path) -> None:
    (tmp_path / "probe").write_bytes(b"ok")
    _check_spawned_operator(tmp_path)
    _run_child("spawn", _check_spawned_operator, tmp_path)


def test_fork_before_runtime_initialization() -> None:
    # The pytest session can already have initialized the runtime.
    script = dedent("""
        import asyncio
        import os
        import signal

        import opendal

        async_operator = opendal.AsyncOperator("memory")

        pid = os.fork()
        if pid == 0:
            signal.alarm(5)
            operator = opendal.Operator("memory")
            operator.write("probe", b"ok")
            assert operator.read("probe") == b"ok"
            os._exit(0)
        assert os.waitpid(pid, 0)[1] == 0

        async def initialize():
            await async_operator.write("probe", b"ok")

        asyncio.run(initialize())
        pid = os.fork()
        if pid == 0:
            signal.alarm(5)

            async def check():
                try:
                    await async_operator.stat("probe")
                except RuntimeError as error:
                    assert 'multiprocessing.get_context("spawn")' in str(error)
                else:
                    raise AssertionError("inherited runtime was usable")

            asyncio.run(check())
            os._exit(0)
        assert os.waitpid(pid, 0)[1] == 0
    """)
    subprocess.run([sys.executable, "-c", script], check=True, timeout=15)
