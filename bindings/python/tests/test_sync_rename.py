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

from opendal.exceptions import (
    ConditionNotMatch,
    IsADirectory,
    IsSameFile,
    NotFound,
    Unsupported,
)


@pytest.mark.need_capability("read", "write", "rename")
def test_sync_rename_file(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}"
    content = os.urandom(1024)
    operator.write(source_path, content)
    target_path = f"random_file_{str(uuid4())}"
    operator.rename(source_path, target_path)
    with pytest.raises(NotFound):
        operator.read(source_path)
    assert operator.read(target_path) == content
    operator.delete(target_path)
    operator.delete(source_path)


@pytest.mark.need_capability("read", "write", "rename")
def test_sync_rename_non_exists_file(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}"
    target_path = f"random_file_{str(uuid4())}"
    with pytest.raises(NotFound):
        operator.rename(source_path, target_path)


@pytest.mark.need_capability("read", "write", "rename", "create_dir")
def test_sync_rename_directory(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}/"
    operator.create_dir(source_path)
    target_path = f"random_file_{str(uuid4())}"
    with pytest.raises(IsADirectory):
        operator.rename(source_path, target_path)


@pytest.mark.need_capability("read", "write", "rename", "create_dir")
def test_sync_rename_file_to_directory(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}"
    content = os.urandom(1024)
    operator.write(source_path, content)
    target_path = f"random_file_{str(uuid4())}/"
    with pytest.raises(IsADirectory):
        operator.rename(source_path, target_path)
    operator.delete(source_path)


@pytest.mark.need_capability("read", "write", "rename")
def test_sync_rename_self(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}"
    content = os.urandom(1024)
    operator.write(source_path, content)
    with pytest.raises(IsSameFile):
        operator.rename(source_path, source_path)
    operator.delete(source_path)


@pytest.mark.need_capability("read", "write", "rename")
def test_sync_rename_nested(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}"
    content = os.urandom(1024)
    operator.write(source_path, content)
    target_path = f"random_file_{str(uuid4())}/{str(uuid4())}/{str(uuid4())}"
    operator.rename(source_path, target_path)
    with pytest.raises(NotFound):
        operator.read(source_path)
    assert operator.read(target_path) == content
    operator.delete(target_path)
    operator.delete(source_path)


@pytest.mark.need_capability("read", "write", "rename")
def test_sync_rename_overwrite(service_name, operator, async_operator):
    source_path = f"random_file_{str(uuid4())}"
    target_path = f"random_file_{str(uuid4())}"
    source_content = os.urandom(1024)
    target_content = os.urandom(1024)
    assert source_content != target_content
    operator.write(source_path, source_content)
    operator.write(target_path, target_content)
    operator.rename(source_path, target_path)
    with pytest.raises(NotFound):
        operator.read(source_path)
    assert operator.read(target_path) == source_content
    operator.delete(target_path)
    operator.delete(source_path)


@pytest.mark.need_capability("read", "write", "delete", "rename_with_if_not_exists")
def test_sync_rename_if_not_exists(operator):
    source_path = f"random_file_{uuid4()}"
    target_path = f"random_file_{uuid4()}"
    source_content = b"source contents"
    target_content = b"target contents"
    operator.write(source_path, source_content)
    operator.write(target_path, target_content)
    try:
        with pytest.raises(ConditionNotMatch):
            operator.rename(source_path, target_path, if_not_exists=True)
        assert operator.read(source_path) == source_content
        assert operator.read(target_path) == target_content

        operator.delete(target_path)
        operator.rename(source_path, target_path, if_not_exists=True)
        with pytest.raises(NotFound):
            operator.read(source_path)
        assert operator.read(target_path) == source_content
    finally:
        operator.delete(source_path)
        operator.delete(target_path)


@pytest.mark.need_capability("read", "write", "delete")
def test_sync_rename_if_not_exists_unsupported(service_name, operator):
    if service_name not in {"memory", "fs"}:
        pytest.skip("test requires memory or fs")
    if operator.capability().rename_with_if_not_exists:
        pytest.skip("service supports conditional rename")

    source_path = f"random_file_{uuid4()}"
    target_path = f"random_file_{uuid4()}"
    source_content = b"source contents"
    target_content = b"target contents"
    operator.write(source_path, source_content)
    operator.write(target_path, target_content)
    try:
        with pytest.raises(Unsupported, match="if_not_exists"):
            operator.rename(source_path, target_path, if_not_exists=True)
        assert operator.read(source_path) == source_content
        assert operator.read(target_path) == target_content
    finally:
        operator.delete(source_path)
        operator.delete(target_path)
