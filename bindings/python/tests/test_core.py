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

import os
import opendal
import pytest


def test_blocking():
    op = opendal.Operator("memory")
    op.write("test", b"Hello, World!")
    bs = op.read("test")
    assert bs == b"Hello, World!", bs
    meta = op.stat("test")
    assert meta.content_length == 13, meta.content_length
    assert meta.mode.is_file()
    assert [str(entry) for entry in op.list("/")] == ["test"]
    assert [str(entry) for entry in op.scan("/")] == ["test"]

    reader = op.open_reader("test")
    bs = reader.read(5)
    assert bs == b"Hello", bs
    bs = reader.read()
    assert bs == b", World!", bs
    reader.seek(0, os.SEEK_SET)
    bs = reader.read()
    assert bs == b"Hello, World!", bs
    with op.open_reader("test") as f:
        bs = f.read()
        assert bs == b"Hello, World!", bs

    op.delete("test")

    op.create_dir("test/")


@pytest.mark.asyncio
async def test_async():
    op = opendal.AsyncOperator("memory")
    await op.write("test", b"Hello, World!")
    bs = await op.read("test")
    assert bs == b"Hello, World!", bs
    meta = await op.stat("test")
    assert meta.content_length == 13, meta.content_length
    assert meta.mode.is_file()
    assert [str(entry) async for entry in await op.list("/")] == ["test"]
    assert [str(entry) async for entry in await op.scan("/")] == ["test"]

    reader = op.open_reader("test")
    bs = await reader.read(5)
    assert bs == b"Hello", bs
    bs = await reader.read()
    assert bs == b", World!", bs
    await reader.seek(0, os.SEEK_SET)
    bs = await reader.read()
    assert bs == b"Hello, World!", bs
    async with op.open_reader("test") as f:
        bs = await f.read()
        assert bs == b"Hello, World!", bs

    await op.delete("test")

    await op.create_dir("test/")


def test_blocking_fs(tmp_path):
    op = opendal.Operator("fs", root=str(tmp_path))
    op.write("test.txt", b"Hello, World!")
    bs = op.read("test.txt")
    assert bs == b"Hello, World!", bs
    meta = op.stat("test.txt")
    assert meta.content_length == 13, meta.content_length
    assert [str(entry) for entry in op.list("/")] == ["test.txt"]
    assert [str(entry) for entry in op.scan("/")] == ["test.txt", "/"]

    op.create_dir("test/")


@pytest.mark.asyncio
async def test_async_fs(tmp_path):
    op = opendal.AsyncOperator("fs", root=str(tmp_path))
    await op.write("test.txt", b"Hello, World!")
    bs = await op.read("test.txt")
    assert bs == b"Hello, World!", bs
    meta = await op.stat("test.txt")
    assert meta.content_length == 13, meta.content_length

    await op.create_dir("test/")


def test_error():
    op = opendal.Operator("memory")
    with pytest.raises(FileNotFoundError):
        op.read("test")

    with pytest.raises(NotImplementedError):
        opendal.Operator("foobar")
