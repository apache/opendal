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
from opendal.exceptions import Unsupported


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


INVALID_MODES = [
    "",
    "b",
    "t",
    "rt",
    "wt",
    "rbt",
    "r+",
    "w+",
    "a+b",
    "x+",
    "rw",
    "rr",
    "rbb",
    "rbz",
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


def test_sync_open_conflicting_append_option(local_operator):
    with pytest.raises(ValueError, match="append=False"):
        local_operator.open("data", "ab", append=False)


@pytest.mark.asyncio
async def test_async_open_conflicting_append_option(local_operator):
    op = local_operator.to_async_operator()
    with pytest.raises(ValueError, match="append=False"):
        await op.open("data", "ab", append=False)
