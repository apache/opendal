# Copyright 2022 Datafuse Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import opendal
import pytest


def test_blocking():
    op = opendal.Operator("memory")
    op.write("test", b"Hello, World!")
    bs = op.read("test")
    assert bs == b"Hello, World!", bs
    meta = op.stat("test")
    assert meta.content_length == 13, meta.content_length


@pytest.mark.asyncio
async def test_async():
    op = opendal.AsyncOperator("memory")
    await op.write("test", b"Hello, World!")
    bs = await op.read("test")
    assert bs == b"Hello, World!", bs
    meta = await op.stat("test")
    assert meta.content_length == 13, meta.content_length


def test_blocking_fs(tmp_path):
    op = opendal.Operator("fs", root=str(tmp_path))
    op.write("test.txt", b"Hello, World!")
    assert (tmp_path / "test.txt").read_bytes() == b"Hello, World!"
    bs = op.read("test.txt")
    assert bs == b"Hello, World!", bs
    meta = op.stat("test.txt")
    assert meta.content_length == 13, meta.content_length


@pytest.mark.asyncio
async def test_async_fs(tmp_path):
    op = opendal.AsyncOperator("fs", root=str(tmp_path))
    await op.write("test.txt", b"Hello, World!")
    assert (tmp_path / "test.txt").read_bytes() == b"Hello, World!"
    bs = await op.read("test.txt")
    assert bs == b"Hello, World!", bs
    meta = await op.stat("test.txt")
    assert meta.content_length == 13, meta.content_length
