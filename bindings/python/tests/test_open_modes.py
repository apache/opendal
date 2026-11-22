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

import pytest

import opendal
from opendal.exceptions import ConditionNotMatch, Unsupported


@pytest.fixture
def local_operator(service_name, tmp_path):
    if service_name == "memory":
        return opendal.Operator("memory")
    if service_name == "fs":
        return opendal.Operator("fs", root=str(tmp_path))
    pytest.skip("requires the memory or fs service")


BINARY_MODES = [
    ("r", "w", "a"),
    ("rb", "wb", "ab"),
    ("br", "bw", "ba"),
]

APPEND_MODES = ["a", "ab", "ba"]

CREATE_MODES = ["x", "xb", "bx"]


@pytest.mark.parametrize(("read", "write", "append"), BINARY_MODES)
def test_sync_open_modes(local_operator, read, write, append):
    op = local_operator
    op.write("data", b"old contents")
    with op.open("data", write) as file:
        file.write(b"new")
    with op.open("data", read) as file:
        assert file.read() == b"new"

    if op.capability().write_can_append:
        with op.open("data", append) as file:
            file.write(b" data")
        assert op.read("data") == b"new data"
    else:
        with pytest.raises(Unsupported, match="append"):
            op.open("data", append)
        assert op.read("data") == b"new"


@pytest.mark.parametrize("mode", APPEND_MODES)
def test_sync_open_append_creates_file(local_operator, mode):
    op = local_operator
    if not op.capability().write_can_append:
        with pytest.raises(Unsupported, match="append"):
            op.open("created", mode)
        assert not op.exists("created")
        return

    with op.open("created", mode) as file:
        assert file.writable()
        assert not file.readable()
        assert not file.seekable()
        assert file.write(b"data") == 4
    assert op.read("created") == b"data"


@pytest.mark.parametrize("mode", CREATE_MODES)
def test_sync_open_create_mode(local_operator, mode):
    op = local_operator
    if not op.capability().write_with_if_not_exists:
        with pytest.raises(Unsupported, match="if_not_exists"):
            op.open("new", mode)
        assert not op.exists("new")
        return

    with op.open("new", mode) as file:
        assert file.writable()
        assert not file.readable()
        assert not file.seekable()
        assert file.write(b"first") == 5
    assert op.read("new") == b"first"

    with pytest.raises(ConditionNotMatch), op.open("new", mode) as file:
        file.write(b"second")
    assert op.read("new") == b"first"


@pytest.mark.asyncio
@pytest.mark.parametrize(("read", "write", "append"), BINARY_MODES)
async def test_async_open_modes(local_operator, read, write, append):
    op = local_operator.to_async_operator()
    await op.write("data", b"old contents")
    async with await op.open("data", write) as file:
        await file.write(b"new")
    async with await op.open("data", read) as file:
        assert await file.read() == b"new"

    if op.capability().write_can_append:
        async with await op.open("data", append) as file:
            await file.write(b" data")
        assert await op.read("data") == b"new data"
    else:
        with pytest.raises(Unsupported, match="append"):
            await op.open("data", append)
        assert await op.read("data") == b"new"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", APPEND_MODES)
async def test_async_open_append_creates_file(local_operator, mode):
    op = local_operator.to_async_operator()
    if not op.capability().write_can_append:
        with pytest.raises(Unsupported, match="append"):
            await op.open("created", mode)
        assert not await op.exists("created")
        return

    async with await op.open("created", mode) as file:
        assert await file.writable()
        assert not await file.readable()
        assert not await file.seekable()
        assert await file.write(b"data") == 4
    assert await op.read("created") == b"data"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", CREATE_MODES)
async def test_async_open_create_mode(local_operator, mode):
    op = local_operator.to_async_operator()
    if not op.capability().write_with_if_not_exists:
        with pytest.raises(Unsupported, match="if_not_exists"):
            await op.open("new", mode)
        assert not await op.exists("new")
        return

    async with await op.open("new", mode) as file:
        assert await file.writable()
        assert not await file.readable()
        assert not await file.seekable()
        assert await file.write(b"first") == 5
    assert await op.read("new") == b"first"

    with pytest.raises(ConditionNotMatch):
        async with await op.open("new", mode) as file:
            await file.write(b"second")
    assert await op.read("new") == b"first"


INVALID_MODES = [
    "",
    "b",
    "t",
    "rt",
    "wt",
    "at",
    "xt",
    "tx",
    "rbt",
    "r+",
    "w+",
    "a+",
    "a+b",
    "x+",
    "xb+",
    "rw",
    "rr",
    "xx",
    "rbb",
    "rbz",
    "xbz",
    "rb\x00",
    " rb",
]


@pytest.mark.parametrize("mode", INVALID_MODES)
def test_sync_open_invalid_mode(local_operator, mode):
    with pytest.raises(ValueError, match="file mode"):
        local_operator.open("data", mode)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", INVALID_MODES)
async def test_async_open_invalid_mode(local_operator, mode):
    op = local_operator.to_async_operator()
    with pytest.raises(ValueError, match="file mode"):
        await op.open("data", mode)


CONFLICTING_OPEN_OPTIONS = [
    ("ab", {"append": False}, "append=False"),
    ("ab", {"if_not_exists": True}, "if_not_exists=True"),
    ("xb", {"if_not_exists": False}, "if_not_exists=False"),
    ("xb", {"append": True}, "append=True"),
]


@pytest.mark.parametrize(("mode", "kwargs", "match"), CONFLICTING_OPEN_OPTIONS)
def test_sync_open_conflicting_options(local_operator, mode, kwargs, match):
    with pytest.raises(ValueError, match=match):
        local_operator.open("data", mode, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "kwargs", "match"), CONFLICTING_OPEN_OPTIONS)
async def test_async_open_conflicting_options(local_operator, mode, kwargs, match):
    op = local_operator.to_async_operator()
    with pytest.raises(ValueError, match=match):
        await op.open("data", mode, **kwargs)
