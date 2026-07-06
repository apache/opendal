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
import pickle
from random import randint
from uuid import uuid4

import pytest

import opendal
from opendal.exceptions import Unsupported


@pytest.fixture(scope="session")
def uri_async_operator(service_name, setup_config):
    # Mirror the `async_operator` fixture but construct via `from_uri`: a bare
    # `scheme://` URI plus the same config passed as keyword options. Apply the
    # same layers and capability overrides so capability gating and equality
    # assertions match the constructor-based operator the suite uses.
    operator = (
        opendal.AsyncOperator.from_uri(f"{service_name}://", **setup_config)
        .layer(opendal.layers.RetryLayer())
        .layer(opendal.layers.ConcurrentLimitLayer(1024))
        .layer(opendal.layers.MimeGuessLayer())
    )
    overrides = os.environ.get("OPENDAL_TEST_CAPABILITY_OVERRIDES")
    if overrides:
        operator = operator.layer(opendal.layers.CapabilityOverrideLayer(overrides))
    return operator


@pytest.fixture(scope="session")
def uri_operator(uri_async_operator):
    return uri_async_operator.to_operator()


def test_from_uri_returns_expected_types(uri_operator, uri_async_operator):
    assert isinstance(uri_operator, opendal.Operator)
    assert isinstance(uri_async_operator, opendal.AsyncOperator)


def test_from_uri_matches_constructor_capability(
    uri_operator, operator, uri_async_operator, async_operator
):
    # `from_uri("scheme://", **config)` and `Operator(scheme, **config)` resolve
    # to the same service, so their capabilities must match.
    assert uri_operator.capability().write == operator.capability().write
    assert uri_operator.capability().read == operator.capability().read
    assert uri_async_operator.capability().write == async_operator.capability().write


@pytest.mark.need_capability("write", "delete", "stat")
def test_sync_from_uri_write(service_name, uri_operator):
    size = randint(1, 1024)
    filename = f"test_file_{uuid4()}.txt"
    content = os.urandom(size)
    uri_operator.write(filename, content)
    metadata = uri_operator.stat(filename)
    assert metadata is not None
    assert metadata.mode.is_file()
    assert metadata.content_length == size

    uri_operator.delete(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("write", "delete", "stat")
async def test_async_from_uri_write(service_name, uri_async_operator):
    size = randint(1, 1024)
    filename = f"test_file_{uuid4()}.txt"
    content = os.urandom(size)
    await uri_async_operator.write(filename, content)
    metadata = await uri_async_operator.stat(filename)
    assert metadata is not None
    assert metadata.mode.is_file()
    assert metadata.content_length == size

    await uri_async_operator.delete(filename)


@pytest.mark.need_capability("read", "write", "delete", "shared")
def test_from_uri_operator_pickle(uri_operator):
    # Match `test_operator_pickle`: an operator built via `from_uri` must survive
    # a pickle round-trip and remain able to read data written before pickling.
    size = randint(1, 1024)
    filename = f"random_file_{uuid4()}"
    content = os.urandom(size)
    uri_operator.write(filename, content)

    deserialized = pickle.loads(pickle.dumps(uri_operator))
    assert deserialized.read(filename) == content

    uri_operator.delete(filename)


def test_from_uri_unsupported_scheme_raises():
    with pytest.raises(Unsupported):
        opendal.Operator.from_uri("thisdoesnotexist://bucket/path")
