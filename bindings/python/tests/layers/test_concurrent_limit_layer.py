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
from opendal.exceptions import ConfigInvalid


def test_concurrent_limit_layer_rejects_zero():
    with pytest.raises(ConfigInvalid):
        opendal.layers.ConcurrentLimitLayer(0)


def test_concurrent_limit_layer_allows_sync_operations():
    op = opendal.Operator("memory").layer(opendal.layers.ConcurrentLimitLayer(1))
    op.write("test", b"hello")
    assert op.read("test") == b"hello"


@pytest.mark.asyncio
async def test_concurrent_limit_layer_allows_async_operations():
    op = opendal.AsyncOperator("memory").layer(opendal.layers.ConcurrentLimitLayer(1))
    await op.write("test", b"hello")
    assert await op.read("test") == b"hello"
