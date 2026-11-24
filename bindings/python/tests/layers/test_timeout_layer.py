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


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timeout": 0.0},
        {"timeout": -1.0},
        {"timeout": float("nan")},
        {"timeout": float("inf")},
        {"timeout": float("-inf")},
        {"timeout": 1e30},
        {"io_timeout": 0.0},
        {"io_timeout": -1.0},
        {"io_timeout": float("nan")},
        {"io_timeout": float("inf")},
        {"io_timeout": float("-inf")},
        {"io_timeout": 1e30},
    ],
)
def test_timeout_layer_rejects_invalid_values(kwargs):
    with pytest.raises(ConfigInvalid):
        opendal.layers.TimeoutLayer(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"timeout": 1e-10, "io_timeout": 1e-10},
        {"timeout": 1e-9},
        {"io_timeout": 1e-9},
        {"timeout": 0.5, "io_timeout": 2.0},
    ],
)
def test_timeout_layer_accepts_valid_values(kwargs):
    assert isinstance(
        opendal.layers.TimeoutLayer(**kwargs), opendal.layers.TimeoutLayer
    )


def test_timeout_layer_allows_sync_operations():
    op = opendal.Operator("memory").layer(
        opendal.layers.TimeoutLayer(timeout=10.0, io_timeout=5.0)
    )
    op.write("test", b"hello")
    assert op.read("test") == b"hello"


@pytest.mark.asyncio
async def test_timeout_layer_allows_async_operations():
    op = opendal.AsyncOperator("memory").layer(
        opendal.layers.TimeoutLayer(timeout=10.0, io_timeout=5.0)
    )
    await op.write("test", b"hello")
    assert await op.read("test") == b"hello"
