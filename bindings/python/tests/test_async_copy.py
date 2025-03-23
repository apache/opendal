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
from uuid import uuid4

import pytest

from opendal.exceptions import IsADirectory, IsSameFile, NotFound


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "copy")
async def test_async_copy(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}"
    content = os.urandom(1024)
    await async_operator.write(source_path, content)
    target_path = f"random_file_{str(uuid4())}"
    await async_operator.copy(source_path, target_path)
    read_content = await async_operator.read(target_path)
    assert read_content is not None
    assert read_content == content
    await async_operator.delete(source_path)
    await async_operator.delete(target_path)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "copy")
async def test_async_copy_non_exist(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}"
    target_path = f"random_file_{str(uuid4())}"
    with pytest.raises(NotFound):
        await async_operator.copy(source_path, target_path)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "copy", "create_dir")
async def test_async_copy_source_directory(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}/"
    await async_operator.create_dir(source_path)
    target_path = f"random_file_{str(uuid4())}"
    with pytest.raises(IsADirectory):
        await async_operator.copy(source_path, target_path)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "copy", "create_dir")
async def test_async_copy_target_directory(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}"
    content = os.urandom(1024)
    await async_operator.write(source_path, content)
    target_path = f"random_file_{str(uuid4())}/"
    await async_operator.create_dir(target_path)
    with pytest.raises(IsADirectory):
        await async_operator.copy(source_path, target_path)
    await async_operator.delete(source_path)
    await async_operator.delete(target_path)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "copy")
async def test_async_copy_self(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}"
    content = os.urandom(1024)
    await async_operator.write(source_path, content)
    with pytest.raises(IsSameFile):
        await async_operator.copy(source_path, source_path)
    await async_operator.delete(source_path)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "copy")
async def test_async_copy_nested(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}"
    target_path = f"random_file_{str(uuid4())}/{str(uuid4())}/{str(uuid4())}"
    content = os.urandom(1024)
    await async_operator.write(source_path, content)
    await async_operator.copy(source_path, target_path)
    target_content = await async_operator.read(target_path)
    assert target_content is not None
    assert target_content == content
    await async_operator.delete(source_path)
    await async_operator.delete(target_path)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "copy")
async def test_async_copy_overwrite(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}"
    target_path = f"random_file_{str(uuid4())}"
    source_content = os.urandom(1024)
    target_content = os.urandom(1024)
    assert source_content != target_content
    await async_operator.write(source_path, source_content)
    await async_operator.write(target_path, target_content)
    await async_operator.copy(source_path, target_path)
    target_content = await async_operator.read(target_path)
    assert target_content is not None
    assert target_content == source_content
    await async_operator.delete(source_path)
    await async_operator.delete(target_path)
