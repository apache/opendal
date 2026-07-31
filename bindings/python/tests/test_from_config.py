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
from pathlib import PurePosixPath
from random import randint
from uuid import uuid4

import pytest

import opendal
from opendal.config import S3Config
from opendal.exceptions import NotFound, Unsupported


@pytest.fixture(scope="session")
def config_async_operator(service_name, setup_config):
    # Mirror the `async_operator` fixture but construct via `from_config`: the
    # scheme key plus the same config the constructor receives. Apply the same
    # layers and capability overrides so capability gating and equality
    # assertions match the constructor-based operator the suite uses.
    operator = (
        opendal.AsyncOperator.from_config({"scheme": service_name, **setup_config})
        .layer(opendal.layers.RetryLayer())
        .layer(opendal.layers.ConcurrentLimitLayer(1024))
        .layer(opendal.layers.MimeGuessLayer())
    )
    overrides = os.environ.get("OPENDAL_TEST_CAPABILITY_OVERRIDES")
    if overrides:
        operator = operator.layer(opendal.layers.CapabilityOverrideLayer(overrides))
    return operator


@pytest.fixture(scope="session")
def config_operator(config_async_operator):
    return config_async_operator.to_operator()


def test_from_config_returns_expected_types(config_operator, config_async_operator):
    assert isinstance(config_operator, opendal.Operator)
    assert isinstance(config_async_operator, opendal.AsyncOperator)


def test_from_config_matches_constructor_capability(
    config_operator, operator, config_async_operator, async_operator
):
    # `from_config({"scheme": ..., **config})` and `Operator(scheme, **config)`
    # resolve to the same service, so their capabilities must match.
    assert config_operator.capability().write == operator.capability().write
    assert config_operator.capability().read == operator.capability().read
    assert config_async_operator.capability().write == async_operator.capability().write


@pytest.mark.need_capability("write", "delete", "stat")
def test_sync_from_config_write(config_operator):
    size = randint(1, 1024)
    filename = f"test_file_{uuid4()}.txt"
    content = os.urandom(size)
    config_operator.write(filename, content)
    metadata = config_operator.stat(filename)
    assert metadata is not None
    assert metadata.mode.is_file()
    assert metadata.content_length == size

    config_operator.delete(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("write", "delete", "stat")
async def test_async_from_config_write(config_async_operator):
    size = randint(1, 1024)
    filename = f"test_file_{uuid4()}.txt"
    content = os.urandom(size)
    await config_async_operator.write(filename, content)
    metadata = await config_async_operator.stat(filename)
    assert metadata is not None
    assert metadata.mode.is_file()
    assert metadata.content_length == size

    await config_async_operator.delete(filename)


@pytest.mark.need_capability("read", "write", "delete")
def test_sync_from_config_read_write_roundtrip(config_operator):
    size = randint(1, 1024)
    filename = f"test_file_{uuid4()}.txt"
    content = os.urandom(size)
    config_operator.write(filename, content)
    assert config_operator.read(filename) == content

    config_operator.delete(filename)


@pytest.mark.asyncio
@pytest.mark.need_capability("read", "write", "delete")
async def test_async_from_config_read_write_roundtrip(config_async_operator):
    size = randint(1, 1024)
    filename = f"test_file_{uuid4()}.txt"
    content = os.urandom(size)
    await config_async_operator.write(filename, content)
    assert (await config_async_operator.read(filename)) == content

    await config_async_operator.delete(filename)


@pytest.mark.need_capability("read", "write", "delete", "shared")
def test_from_config_operator_pickle(config_operator):
    size = randint(1, 1024)
    filename = f"random_file_{uuid4()}"
    content = os.urandom(size)
    config_operator.write(filename, content)

    deserialized = pickle.loads(pickle.dumps(config_operator))
    assert deserialized.read(filename) == content

    config_operator.delete(filename)


@pytest.mark.need_capability("read")
def test_sync_from_config_read_not_exists(config_operator):
    with pytest.raises(NotFound):
        config_operator.read(str(uuid4()))


@pytest.mark.asyncio
@pytest.mark.need_capability("read")
async def test_async_from_config_read_not_exists(config_async_operator):
    with pytest.raises(NotFound):
        await config_async_operator.read(str(uuid4()))


def test_from_config_unsupported_scheme_raises():
    with pytest.raises(Unsupported):
        opendal.Operator.from_config({"scheme": "thisdoesnotexist"})  # type: ignore[arg-type]


def test_from_config_missing_scheme_raises():
    with pytest.raises(Unsupported):
        opendal.Operator.from_config({"bucket": "b"})  # type: ignore[arg-type]


def test_config_is_plain_dict_at_runtime():
    cfg = S3Config(scheme="s3", bucket="b")
    assert cfg == {"scheme": "s3", "bucket": "b"}
    assert type(cfg) is dict


def test_from_config_stringifies_native_values():
    # Non-string values are converted to the flat string map core consumes; the
    # always-enabled memory service ignores the probe keys.
    op = opendal.Operator.from_config(
        {"scheme": "memory", "probe_flag": True, "probe_path": PurePosixPath("/x")}  # type: ignore[arg-type]
    )
    _, args = op.__reduce__()
    assert args[1]["probe_flag"] == "true"
    assert args[1]["probe_path"] == "/x"
