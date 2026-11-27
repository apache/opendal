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

import contextlib

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


def test_sync_open_create_existing_file_large_write(service_name, tmp_path):
    if service_name != "fs":
        pytest.skip("requires the fs service")
    op = opendal.Operator("fs", root=str(tmp_path))
    if not op.capability().write_with_if_not_exists:
        pytest.skip("requires write_with_if_not_exists")
    op.write("data", b"old contents")

    file = op.open("data", "xb", chunk=64 * 1024)
    try:
        with pytest.raises(ConditionNotMatch):
            file.write(b"new" * (100 * 1024))
        with pytest.raises(ConditionNotMatch):
            file.flush()
    finally:
        with contextlib.suppress(ConditionNotMatch):
            file.close()
    assert op.read("data") == b"old contents"


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


@pytest.mark.asyncio
async def test_async_open_create_existing_file_large_write(service_name, tmp_path):
    if service_name != "fs":
        pytest.skip("requires the fs service")
    op = opendal.AsyncOperator("fs", root=str(tmp_path))
    if not op.capability().write_with_if_not_exists:
        pytest.skip("requires write_with_if_not_exists")
    await op.write("data", b"old contents")

    file = await op.open("data", "xb", chunk=64 * 1024)
    try:
        with pytest.raises(ConditionNotMatch):
            await file.write(b"new" * (100 * 1024))
    finally:
        with contextlib.suppress(ConditionNotMatch):
            await file.close()
    assert await op.read("data") == b"old contents"


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


NONE_WRITE_OPTIONS = {
    "append": None,
    "chunk": None,
    "concurrent": None,
    "cache_control": None,
    "content_type": None,
    "content_disposition": None,
    "content_encoding": None,
    "if_match": None,
    "if_none_match": None,
    "if_not_exists": None,
    "user_metadata": None,
    "checksum": None,
    "offset": None,
}

NONE_READ_OPTIONS = {
    "version": None,
    "concurrent": None,
    "chunk": None,
    "gap": None,
    "offset": None,
    "prefetch": None,
    "size": None,
    "if_match": None,
    "if_none_match": None,
    "if_modified_since": None,
    "if_unmodified_since": None,
    "append": None,
    "checksum": None,
    "content_type": None,
    "cache_control": None,
    "content_disposition": None,
}


def test_sync_open_none_options(local_operator):
    op = local_operator
    with op.open("data", "wb", **NONE_WRITE_OPTIONS) as file:
        assert file.write(b"hello") == 5
    with op.open("data", "rb", **NONE_READ_OPTIONS) as file:
        assert file.read() == b"hello"
    with pytest.raises(TypeError):
        op.open("data", "rb", offset="invalid")


@pytest.mark.asyncio
async def test_async_open_none_options(local_operator):
    op = local_operator.to_async_operator()
    async with await op.open("data", "wb", **NONE_WRITE_OPTIONS) as file:
        assert await file.write(b"hello") == 5
    async with await op.open("data", "rb", **NONE_READ_OPTIONS) as file:
        assert await file.read() == b"hello"
    with pytest.raises(TypeError):
        await op.open("data", "rb", offset="invalid")


INVALID_OPEN_KWARGS = [
    ("rb", {"if_matches": "etag"}, "if_matches"),
    ("rb", {"if_matches": None}, "if_matches"),
    ("wb", {"if_not_exist": True}, "if_not_exist"),
    ("rb", {"append": True}, "append"),
    ("rb", {"checksum": "crc32c"}, "checksum"),
    ("rb", {"if_not_exists": True}, "if_not_exists"),
    ("rb", {"content_encoding": "gzip"}, "content_encoding"),
    ("rb", {"user_metadata": {"a": "b"}}, "user_metadata"),
    ("rb", {"content_type": "text/plain"}, "content_type"),
    ("rb", {"cache_control": "no-cache"}, "cache_control"),
    ("rb", {"content_disposition": "inline"}, "content_disposition"),
    ("wb", {"offset": 0}, "offset"),
    ("wb", {"size": 10}, "size"),
    ("wb", {"gap": 4}, "gap"),
    ("wb", {"prefetch": 1}, "prefetch"),
    ("wb", {"version": "v1"}, "version"),
    ("ab", {"offset": 0}, "offset"),
    ("xb", {"size": 1}, "size"),
]


@pytest.mark.parametrize(("mode", "kwargs", "key"), INVALID_OPEN_KWARGS)
def test_sync_open_rejects_unknown_and_cross_mode_kwargs(
    local_operator, mode, kwargs, key
):
    local_operator.write("data", b"original")
    with pytest.raises(TypeError, match=key):
        local_operator.open("data", mode, **kwargs)
    assert local_operator.read("data") == b"original"


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "kwargs", "key"), INVALID_OPEN_KWARGS)
async def test_async_open_rejects_unknown_and_cross_mode_kwargs(
    local_operator, mode, kwargs, key
):
    op = local_operator.to_async_operator()
    await op.write("data", b"original")
    with pytest.raises(TypeError, match=key):
        await op.open("data", mode, **kwargs)
    assert await op.read("data") == b"original"


def test_sync_read_accepts_response_header_overrides(local_operator):
    local_operator.write("data", b"hello")
    assert (
        local_operator.read(
            "data",
            content_type="text/plain",
            cache_control="no-cache",
            content_disposition="inline",
        )
        == b"hello"
    )


@pytest.mark.asyncio
async def test_async_read_accepts_response_header_overrides(local_operator):
    op = local_operator.to_async_operator()
    await op.write("data", b"hello")
    assert (
        await op.read(
            "data",
            content_type="text/plain",
            cache_control="no-cache",
            content_disposition="inline",
        )
        == b"hello"
    )
