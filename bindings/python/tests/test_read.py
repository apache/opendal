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
from random import randint
from uuid import uuid4

import pytest
from opendal.exceptions import NotFound


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
        read_content = reader.read(size + 1)
        assert read_content is not None
        assert read_content == content

    buf = bytearray(1)
    with operator.open(filename, 'rb') as reader:
        reader.readinto(buf)
        assert buf == content

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
async def test_async_reader(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"random_file_{str(uuid4())}"
    content = os.urandom(size)
    await async_operator.write(filename, content)

    async with await async_operator.open(filename, "rb") as reader:
        read_content = await reader.read()
        assert read_content is not None
        assert read_content == content

    async with await async_operator.open(filename, "rb") as reader:
        read_content = await reader.read(size + 1)
        assert read_content is not None
        assert read_content == content

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
