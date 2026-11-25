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
import io
import os
from datetime import timedelta
from pathlib import Path
from random import choices, randint
from uuid import uuid4

import pytest

import opendal
from opendal.exceptions import ConditionNotMatch, NotFound, RangeNotSatisfied


@pytest.mark.need_capability("read", "write", "delete")
def test_sync_read(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"random_file_{str(uuid4())}"
    content = os.urandom(size)
    operator.write(filename, content)

    read_content = operator.read(filename)
    assert read_content is not None
    assert read_content == content

    operator.delete(filename)


@pytest.mark.need_capability("read", "write", "delete")
def test_sync_reader(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"random_file_{str(uuid4())}"
    content = os.urandom(size)
    operator.write(filename, content)

    with operator.open(filename, "rb") as reader:
        assert reader.readable()
        assert not reader.writable()
        assert not reader.closed

        read_content = reader.read()
        assert read_content is not None
        assert read_content == content

    with operator.open(filename, "rb") as reader:
        read_content = bytearray()
        while True:
            chunk = reader.read(size + 1)
            if not chunk:
                break
            read_content.extend(chunk)

        read_content = bytes(read_content)
        assert read_content is not None
        assert read_content == content

    buf = bytearray(1)
    with operator.open(filename, "rb") as reader:
        reader.readinto(buf)
        assert buf == content[:1]

    range_start = randint(0, len(content) - 1)
    range_end = randint(range_start, len(content) - 1)

    with operator.open(
        filename, "rb", offset=range_start, size=range_end - range_start
    ) as reader:
        assert reader.readable()
        assert not reader.writable()
        assert not reader.closed

        read_content = reader.read()
        assert read_content is not None
        assert read_content == content[range_start:range_end]

    operator.delete(filename)


@pytest.mark.need_capability("read", "write", "delete")
def test_sync_reader_read_sizes(operator):
    filename = f"random_file_{str(uuid4())}"
    content = b"hello, world"
    operator.write(filename, content)

    with operator.open(filename, "rb") as reader:
        assert reader.read(0) == b""
        assert reader.tell() == 0
        assert reader.read(5) == b"hello"
        assert reader.tell() == 5
        assert reader.read(1024) == b", world"
        assert reader.read(1) == b""
        reader.seek(2)
        assert reader.read() == b"llo, world"

    with pytest.raises(ValueError, match="closed file"):
        reader.tell()
    operator.delete(filename)


@pytest.mark.need_capability("read", "write", "delete")
def test_sync_reader_rejects_negative_seek_set(operator):
    filename = f"random_file_{str(uuid4())}"
    operator.write(filename, b"0123456789")

    try:
        with operator.open(filename, "rb") as reader:
            assert reader.seek(3) == 3
            with pytest.raises(ValueError, match="negative seek value -1"):
                reader.seek(-1, 0)
            assert reader.tell() == 3
            with pytest.raises(ValueError, match="negative seek value -1"):
                reader.seek(-1)
            assert reader.tell() == 3

            assert reader.read() == b"3456789"
    finally:
        operator.delete(filename)


@pytest.mark.need_capability("read", "write", "delete")
def test_sync_reader_readline(service_name, operator, async_operator):
    size = randint(1, 1024)
    lines = randint(1, min(100, size))
    filename = f"random_file_{str(uuid4())}"
    content = bytearray(os.urandom(size))

    for idx in choices(range(size), k=lines):
        content[idx] = ord("\n")
    operator.write(filename, content)

    line_contents = io.BytesIO(content).readlines()
    i = 0

    with operator.open(filename, "rb") as reader:
        assert reader.readable()
        assert not reader.writable()
        assert not reader.closed

        while (read_content := reader.readline()) != b"":
            assert read_content is not None
            assert read_content == line_contents[i]
            i += 1

    operator.delete(filename)


@pytest.mark.need_capability("read", "write", "delete")
def test_sync_reader_readline_with_size(operator):
    filename = f"random_file_{str(uuid4())}"
    content = b"alpha\nbeta\ngamma"
    operator.write(filename, content)

    expected = io.BytesIO(content)
    with operator.open(filename, "rb") as reader:
        for size in [3, 10, 0, 5, 2, 10, 10]:
            assert reader.readline(size) == expected.readline(size)

    operator.delete(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "delete")
async def test_async_read(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"random_file_{str(uuid4())}"
    content = os.urandom(size)
    await async_operator.write(filename, content)

    read_content = await async_operator.read(filename)
    assert read_content is not None
    assert read_content == content

    await async_operator.delete(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "delete")
async def test_async_read_path(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = Path(f"random_file_{str(uuid4())}")
    content = os.urandom(size)
    await async_operator.write(filename, content)

    read_content = await async_operator.read(filename)
    assert read_content is not None
    assert read_content == content

    await async_operator.delete(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "delete")
async def test_async_reader(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"random_file_{str(uuid4())}"
    content = os.urandom(size)
    await async_operator.write(filename, content)

    async with await async_operator.open(filename, "rb") as reader:
        assert await reader.readable()
        assert not await reader.writable()
        assert not await reader.closed

        read_content = await reader.read()
        assert read_content is not None
        assert read_content == content

    async with await async_operator.open(filename, "rb") as reader:
        read_content = bytearray()
        while True:
            chunk = await reader.read(size + 1)
            if not chunk:
                break
            read_content.extend(chunk)

        read_content = bytes(read_content)
        assert read_content is not None
        assert read_content == content

    range_start = randint(0, len(content) - 1)
    range_end = randint(range_start, len(content) - 1)

    async with await async_operator.open(
        filename, "rb", offset=range_start, size=range_end - range_start
    ) as reader:
        assert await reader.readable()
        assert not await reader.writable()
        assert not await reader.closed

        read_content = await reader.read()
        assert read_content is not None
        assert read_content == content[range_start:range_end]

    await async_operator.delete(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "delete")
async def test_async_reader_read_sizes(async_operator):
    filename = f"random_file_{str(uuid4())}"
    content = b"hello, world"
    await async_operator.write(filename, content)

    async with await async_operator.open(filename, "rb") as reader:
        assert await reader.read(0) == b""
        assert await reader.tell() == 0
        assert await reader.read(5) == b"hello"
        assert await reader.tell() == 5
        assert await reader.read(1024) == b", world"
        assert await reader.read(1) == b""
        await reader.seek(2)
        assert await reader.read() == b"llo, world"

    with pytest.raises(ValueError, match="closed file"):
        await reader.tell()
    await async_operator.delete(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "delete")
async def test_async_reader_rejects_negative_seek_set(async_operator):
    filename = f"random_file_{str(uuid4())}"
    await async_operator.write(filename, b"0123456789")

    try:
        async with await async_operator.open(filename, "rb") as reader:
            assert await reader.seek(3) == 3
            with pytest.raises(ValueError, match="negative seek value -1"):
                await reader.seek(-1, 0)
            assert await reader.tell() == 3
            with pytest.raises(ValueError, match="negative seek value -1"):
                await reader.seek(-1)
            assert await reader.tell() == 3

            assert await reader.read() == b"3456789"
    finally:
        await async_operator.delete(filename)


@pytest.mark.asyncio
async def test_async_reader_read_buffered_coalesces_chunks():
    operator = opendal.AsyncOperator("memory")
    await operator.write("object", b"0123456789")

    async with await operator.open("object", "rb", chunk=2) as reader:
        assert await reader.read(5) == b"01"
        assert await reader.read_buffered(5) == b"23456"
        assert await reader.tell() == 7
        assert await reader.read_buffered(5) == b"789"
        assert await reader.read_buffered(5) == b""


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "delete")
async def test_async_reader_read_buffered_range(async_operator):
    filename = f"read_buffered_{uuid4()}"
    await async_operator.write(filename, b"0123456789")
    try:
        async with await async_operator.open(
            filename, "rb", offset=2, size=5
        ) as reader:
            with pytest.raises(OverflowError):
                reader.read_buffered(-1)
            assert await reader.read_buffered(0) == b""
            assert await reader.tell() == 0
            assert await reader.read_buffered(2) == b"23"
            assert await reader.tell() == 2
            assert await reader.seek(-1, 1) == 1
            assert await reader.seek(-2, 2) == 3
            assert await reader.read_buffered(32) == b"56"
            assert await reader.tell() == 5
            assert await reader.read_buffered(32) == b""
            assert await reader.tell() == 5

        with pytest.raises(OSError, match="closed file"):
            await reader.read_buffered(1)
    finally:
        await async_operator.delete(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "delete")
async def test_async_reader_without_context(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"random_file_{str(uuid4())}"
    content = os.urandom(size)
    await async_operator.write(filename, content)

    reader = await async_operator.open(filename, "rb")
    read_content = await reader.read()
    assert read_content is not None
    assert read_content == content
    await reader.close()

    await async_operator.delete(filename)


@pytest.mark.need_capability("read", "write", "delete", "stat")
def test_sync_read_stat(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"random_file_{str(uuid4())}"
    content = os.urandom(size)
    operator.write(filename, content)

    metadata = operator.stat(filename)
    assert metadata is not None
    assert metadata.content_length == len(content)
    assert metadata.mode.is_file()

    operator.delete(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "delete", "stat")
async def test_async_read_stat(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"random_file_{str(uuid4())}"
    content = os.urandom(size)
    await async_operator.write(filename, content)

    metadata = await async_operator.stat(filename)
    assert metadata is not None
    assert metadata.content_length == len(content)
    assert metadata.mode.is_file()

    await async_operator.delete(filename)

    operator.delete(filename)


@pytest.mark.need_capability("read")
def test_sync_read_not_exists(service_name, operator, async_operator):
    with pytest.raises(NotFound):
        operator.read(str(uuid4()))


@pytest.mark.asyncio
@pytest.mark.need_capability("read")
async def test_async_read_not_exists(service_name, operator, async_operator):
    with pytest.raises(NotFound):
        await async_operator.read(str(uuid4()))


@pytest.mark.need_capability(
    "read",
    "write",
    "read_with_if_modified_since",
    "read_with_if_unmodified_since",
)
def test_sync_conditional_reads(service_name, operator):
    path = f"random_file_{str(uuid4())}"
    content = b"test data"
    operator.write(path, content)

    metadata = operator.stat(path)
    mod_time = metadata.last_modified
    assert mod_time is not None

    # Large delta: 1 minute earlier
    before = mod_time - timedelta(minutes=1)
    after = mod_time + timedelta(seconds=10)

    # Should succeed: file was modified after `before`
    assert operator.read(path, if_modified_since=before) == content

    # Should succeed: file was unmodified since `after`
    assert operator.read(path, if_unmodified_since=after) == content

    # Should fail: file was modified after `before`
    with pytest.raises(ConditionNotMatch):
        operator.read(path, if_unmodified_since=before)


@pytest.mark.need_capability("read", "write", "delete")
def test_sync_readinto_buffer_boundaries(operator):
    filename = f"readinto_{uuid4()}"
    operator.write(filename, b"abcdef")
    try:
        with operator.open(filename, "rb") as reader:
            with pytest.raises(OSError, match="not writable"):
                reader.readinto(b"readonly")
            with pytest.raises(OSError, match="not C contiguous"):
                reader.readinto(memoryview(bytearray(8))[::2])
            assert reader.tell() == 0
            assert reader.readinto(bytearray()) == 0
            assert reader.tell() == 0
            target = bytearray(b"??????????")
            assert reader.readinto(memoryview(target)[2:5]) == 3
            assert target == b"??abc?????"
            assert reader.tell() == 3
            assert reader.readinto(memoryview(target)[5:]) == 3
            assert target == b"??abcdef??"
            assert reader.tell() == 6
            assert reader.readinto(target) == 0
            assert target == b"??abcdef??"
    finally:
        operator.delete(filename)


@pytest.mark.need_capability("read", "write", "delete")
def test_sync_readinto_empty_buffers(operator):
    filename = f"readinto_empty_{uuid4()}"
    operator.write(filename, b"abcdef")
    try:
        with operator.open(filename, "rb") as reader:
            # Zero-length buffers may carry a NULL data pointer; `readinto`
            # must return 0 instead of panicking.
            assert reader.readinto(bytearray()) == 0
            assert reader.readinto(memoryview(bytearray(0))) == 0
            assert reader.tell() == 0

        with operator.open(filename, "rb") as reader:
            # A zero-length read at EOF also returns 0 without touching the
            # buffer pointer.
            reader.read()
            assert reader.readinto(bytearray(4)) == 0
    finally:
        operator.delete(filename)


@pytest.mark.need_capability("read", "write", "delete")
def test_sync_readinto_readonly_empty_buffer(operator):
    filename = f"readinto_empty_readonly_{uuid4()}"
    operator.write(filename, b"abcdef")
    try:
        with (
            operator.open(filename, "rb") as reader,
            pytest.raises(OSError, match="not writable"),
        ):
            # A zero-length but readonly buffer is still rejected.
            reader.readinto(memoryview(b""))
    finally:
        operator.delete(filename)


def test_sync_open_range_overflow_raises_range_not_satisfied():
    op = opendal.Operator("memory")
    op.write("test", b"hello")

    large_offset = (1 << 32) + 1024
    with op.open("test", "rb", offset=0, size=large_offset) as reader:
        assert reader.read(5) == b"hello"

    with (
        op.open("test", "rb", offset=large_offset, size=1024) as reader,
        pytest.raises(OSError, match="RangeNotSatisfied"),
    ):
        reader.read()

    with pytest.raises(RangeNotSatisfied):
        op.read("test", offset=large_offset, size=1024)

    with pytest.raises(RangeNotSatisfied):
        op.open("test", "rb", offset=(1 << 64) - 1, size=1)

    with pytest.raises(RangeNotSatisfied):
        op.open("test", "rb", offset=1, size=(1 << 64) - 1)

    with pytest.raises(RangeNotSatisfied):
        op.read("test", offset=(1 << 64) - 1, size=1)


@pytest.mark.asyncio
async def test_async_open_range_overflow_raises_range_not_satisfied():
    aop = opendal.AsyncOperator("memory")
    await aop.write("test", b"hello")

    large_offset = (1 << 32) + 1024
    async with await aop.open("test", "rb", offset=0, size=large_offset) as reader:
        assert await reader.read(5) == b"hello"

    async with await aop.open("test", "rb", offset=large_offset, size=1024) as reader:
        with pytest.raises(OSError, match="RangeNotSatisfied"):
            await reader.read()

    with pytest.raises(RangeNotSatisfied):
        await aop.read("test", offset=large_offset, size=1024)

    with pytest.raises(RangeNotSatisfied):
        await aop.open("test", "rb", offset=(1 << 64) - 1, size=1)

    with pytest.raises(RangeNotSatisfied):
        await aop.open("test", "rb", offset=1, size=(1 << 64) - 1)

    with pytest.raises(RangeNotSatisfied):
        await aop.read("test", offset=(1 << 64) - 1, size=1)


@pytest.mark.asyncio
async def test_async_file_reentrant_calls_do_not_raise_already_borrowed():
    aop = opendal.AsyncOperator("memory")
    await aop.write("test", b"abcdef")

    async with await aop.open("test", "rb") as reader:
        reentrant_tasks: list[asyncio.Future[int]] = []

        class ReentrantInt:
            def __index__(self) -> int:
                reentrant_tasks.append(asyncio.ensure_future(reader.tell()))
                return 3

        assert await reader.seek(ReentrantInt()) == 3
        assert await reentrant_tasks[0] in (0, 3)
        assert await reader.read() == b"def"

    loop = asyncio.get_running_loop()
    original_create_future = loop.create_future
    writer = await aop.open("reentrant_write", "wb")
    nested_calls: list[asyncio.Future[object]] = []
    entered = False

    def reentrant_create_future() -> asyncio.Future[object]:
        nonlocal entered
        if not entered:
            entered = True
            nested_calls.append(asyncio.ensure_future(writer.tell()))
            nested_calls.append(asyncio.ensure_future(writer.writable()))
        return original_create_future()

    loop.create_future = reentrant_create_future
    try:
        assert await writer.write(b"hello") == 5
        await asyncio.gather(*nested_calls)
        entered = False
        await writer.close()
    finally:
        loop.create_future = original_create_future
        await asyncio.gather(*nested_calls, return_exceptions=True)

    assert await aop.read("reentrant_write") == b"hello"
