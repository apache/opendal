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
from pathlib import Path
from random import randint
from uuid import uuid4

import pytest

from opendal.exceptions import NotFound


@pytest.mark.need_capability("write", "delete", "stat")
def test_sync_write(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"test_file_{str(uuid4())}.txt"
    content = os.urandom(size)
    size = len(content)
    operator.write(filename, content, content_type="text/plain")
    metadata = operator.stat(filename)
    assert metadata is not None
    assert metadata.mode.is_file()
    assert metadata.content_length == size

    last_modified = (
        metadata.last_modified.strftime("%Y-%m-%dT%H:%M:%S")
        if metadata.last_modified
        else None
    )
    assert repr(metadata) == (
        "Metadata(mode=file, "
        f"content_length={metadata.content_length}, "
        f"content_type={metadata.content_type}, "
        f"last_modified={last_modified}, "
        f"etag={metadata.etag})"
    )

    operator.delete(filename)


@pytest.mark.need_capability("write", "delete", "stat")
def test_sync_write_path(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = Path(f"test_file_{str(uuid4())}.txt")
    content = os.urandom(size)
    size = len(content)
    operator.write(filename, content, content_type="text/plain")
    metadata = operator.stat(filename)
    assert metadata is not None
    assert metadata.mode.is_file()
    assert metadata.content_length == size

    last_modified = (
        metadata.last_modified.strftime("%Y-%m-%dT%H:%M:%S")
        if metadata.last_modified
        else None
    )
    assert repr(metadata) == (
        "Metadata(mode=file, "
        f"content_length={metadata.content_length}, "
        f"content_type={metadata.content_type}, "
        f"last_modified={last_modified}, "
        f"etag={metadata.etag})"
    )

    operator.delete(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("write", "delete", "stat")
async def test_async_write(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"test_file_{str(uuid4())}.txt"
    content = os.urandom(size)
    size = len(content)
    await async_operator.write(filename, content)
    metadata = await async_operator.stat(filename)
    assert metadata is not None
    assert metadata.mode.is_file()
    assert metadata.content_length == size

    await async_operator.delete(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("write", "delete", "stat")
async def test_async_write_path(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = Path(f"test_file_{str(uuid4())}.txt")
    content = os.urandom(size)
    size = len(content)
    await async_operator.write(filename, content)
    metadata = await async_operator.stat(filename)
    assert metadata is not None
    assert metadata.mode.is_file()
    assert metadata.content_length == size

    await async_operator.delete(filename)


@pytest.mark.need_capability("create_dir", "stat")
def test_sync_create_dir(service_name, operator, async_operator):
    path = f"test_dir_{str(uuid4())}/"
    operator.create_dir(path)
    metadata = operator.stat(path)
    assert metadata is not None
    assert metadata.mode.is_dir()

    operator.delete(path)


@pytest.mark.asyncio
@pytest.mark.need_capability("create_dir", "stat")
async def test_async_create_dir(service_name, operator, async_operator):
    path = f"test_dir_{str(uuid4())}/"
    await async_operator.create_dir(path)
    metadata = await async_operator.stat(path)
    assert metadata is not None
    assert metadata.mode.is_dir()

    await async_operator.delete(path)


@pytest.mark.need_capability("delete", "stat")
def test_sync_delete(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"test_file_{str(uuid4())}.txt"
    content = os.urandom(size)
    size = len(content)
    operator.write(filename, content)
    operator.delete(filename)
    with pytest.raises(NotFound):
        operator.stat(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("delete", "stat")
async def test_async_delete(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"test_file_{str(uuid4())}.txt"
    content = os.urandom(size)
    size = len(content)
    await async_operator.write(filename, content)
    await async_operator.delete(filename)
    with pytest.raises(NotFound):
        await async_operator.stat(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("write", "delete")
async def test_async_writer(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"test_file_{str(uuid4())}.txt"
    content = os.urandom(size)
    f = await async_operator.open(filename, "wb")
    written_bytes = await f.write(content)
    assert written_bytes == size
    await f.close()
    await async_operator.delete(filename)
    with pytest.raises(NotFound):
        await async_operator.stat(filename)


@pytest.mark.need_capability("write", "delete")
def test_sync_writer(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"test_file_{str(uuid4())}.txt"
    content = os.urandom(size)
    f = operator.open(filename, "wb")
    written_bytes = f.write(content)
    assert written_bytes == size
    f.close()
    operator.delete(filename)
    with pytest.raises(NotFound):
        operator.stat(filename)
