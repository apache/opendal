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

from uuid import uuid4

import opendal
import pytest


def test_sync_write(operator: opendal.Operator):
    filename = f'test_file_{str(uuid4())}.txt'
    content = b'Hello, world!'
    size = len(content)
    try:
        operator.write(filename, content)
    except Exception as e:
        pytest.fail(f'test_sync_write failed with {e}')
    metadata = operator.stat(filename)
    assert metadata is not None
    assert metadata.mode.is_file()
    assert metadata.content_length == size

    operator.delete(filename)


def test_sync_write_with_non_ascii_name(operator: opendal.Operator):
    filename = '❌😱中文.test'
    content = b'Hello, world!'
    size = len(content)
    try:
        operator.write(filename, content)
    except Exception as e:
        pytest.fail(f'test_sync_write_with_non_ascii_name failed with {e}')
    metadata = operator.stat(filename)
    assert metadata is not None
    assert metadata.mode.is_file()
    assert metadata.content_length == size

    operator.delete(filename)


def test_sync_create_dir(operator: opendal.Operator):
    path = f'test_dir_{str(uuid4())}/'
    try:
        operator.create_dir(path)
    except Exception as e:
        pytest.fail(f'test_sync_create_dir failed with {e}')
    metadata = operator.stat(path)
    assert metadata is not None
    assert metadata.mode.is_dir()

    operator.delete(path)
 

def test_sync_delete(operator: opendal.Operator):
    filename = f'test_file_{str(uuid4())}.txt'
    content = b'Hello, world!'
    size = len(content)
    operator.write(filename, content)
    try:
        operator.delete(filename)
    except Exception as e:
        pytest.fail(f'test_sync_delete failed with {e}')
    with pytest.raises(FileNotFoundError):
        operator.stat(filename)
