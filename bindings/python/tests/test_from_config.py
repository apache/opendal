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

"""Tests for typed service configs and cross-path operator equivalence.

Uses the ``memory`` service so the suite runs without ``OPENDAL_TEST``; the
kwargs value-type error tests use ``s3`` but fail before any backend call.
"""

import pickle
from pathlib import PurePosixPath
from uuid import uuid4

import pytest

import opendal
from opendal.services import MemoryConfig, S3Config, ServiceConfig


@pytest.fixture(autouse=True)
def check_capability() -> None:
    # Override conftest's autouse fixture, which requires OPENDAL_TEST.
    return None


def test_config_subclasses_service_config():
    assert issubclass(MemoryConfig, ServiceConfig)
    assert issubclass(S3Config, ServiceConfig)


def test_scheme_is_bound_and_not_a_field():
    assert S3Config(bucket="b").scheme == "s3"
    assert MemoryConfig().scheme == "memory"
    with pytest.raises(TypeError):
        S3Config(bucket="b", scheme="hdfs")


def test_required_field_enforced():
    with pytest.raises(TypeError):
        S3Config()


def test_unknown_field_rejected():
    with pytest.raises(TypeError):
        S3Config(bucket="b", not_a_real_field=1)


def test_getters_read_back_values():
    cfg = S3Config(bucket="b", region="us-east-1", assume_role_duration_seconds=900)
    assert cfg.bucket == "b"
    assert cfg.region == "us-east-1"
    assert cfg.assume_role_duration_seconds == 900


def test_path_field_accepts_pathlike():
    cfg = S3Config(bucket="b", root=PurePosixPath("/data/x"))
    assert cfg.root == "/data/x"


def test_from_config_builds_blocking_operator():
    op = opendal.Operator.from_config(MemoryConfig())
    assert isinstance(op, opendal.Operator)
    key = f"k-{uuid4()}"
    op.write(key, b"hello")
    assert op.read(key) == b"hello"


@pytest.mark.asyncio
async def test_from_config_builds_async_operator():
    op = opendal.AsyncOperator.from_config(MemoryConfig())
    assert isinstance(op, opendal.AsyncOperator)
    key = f"k-{uuid4()}"
    await op.write(key, b"world")
    assert (await op.read(key)) == b"world"


def test_from_config_matches_constructor_capability():
    from_cfg = opendal.Operator.from_config(MemoryConfig())
    from_kwargs = opendal.Operator("memory")
    assert from_cfg.capability().write == from_kwargs.capability().write
    assert from_cfg.capability().read == from_kwargs.capability().read


def test_from_config_rejects_non_config():
    with pytest.raises(TypeError):
        opendal.Operator.from_config("memory")


def test_from_config_supports_map_valued_field():
    cfg = S3Config(bucket="b", assume_role_session_tags={"team": "eng"})
    assert cfg.assume_role_session_tags == {"team": "eng"}
    op = opendal.Operator.from_config(cfg)
    assert isinstance(op, opendal.Operator)


def test_from_config_pickle_roundtrip():
    op = opendal.Operator.from_config(MemoryConfig())
    restored = pickle.loads(pickle.dumps(op))
    assert isinstance(restored, opendal.Operator)
    key = f"k-{uuid4()}"
    restored.write(key, b"data")
    assert restored.read(key) == b"data"


def test_pickle_raises_when_map_field_set():
    cfg = S3Config(bucket="b", assume_role_session_tags={"team": "eng"})
    op = opendal.Operator.from_config(cfg)
    with pytest.raises(opendal.exceptions.Unsupported):
        pickle.dumps(op)


def _blocking_paths(root: str) -> dict[str, opendal.Operator]:
    return {
        "kwargs": opendal.Operator("memory", root=root),
        "from_uri": opendal.Operator.from_uri("memory://", root=root),
        "from_config": opendal.Operator.from_config(MemoryConfig(root=root)),
    }


def _async_paths(root: str) -> dict[str, opendal.AsyncOperator]:
    return {
        "kwargs": opendal.AsyncOperator("memory", root=root),
        "from_uri": opendal.AsyncOperator.from_uri("memory://", root=root),
        "from_config": opendal.AsyncOperator.from_config(MemoryConfig(root=root)),
    }


def _info(op: opendal.Operator) -> tuple:
    cap = op.capability()
    return (
        op.__class__.__name__,
        cap.read,
        cap.write,
        cap.delete,
        cap.list,
        cap.copy,
        cap.rename,
    )


def test_paths_produce_same_scheme_and_root():
    for op in _blocking_paths("/equiv/").values():
        assert 'Operator("memory"' in repr(op)
        assert 'root="/equiv/"' in repr(op)


def test_paths_produce_same_capability():
    infos = {name: _info(op) for name, op in _blocking_paths("/cap/").items()}
    assert infos["kwargs"] == infos["from_uri"] == infos["from_config"]


def test_paths_read_write_roundtrip_identically():
    for name, op in _blocking_paths("/rw/").items():
        key = f"k-{uuid4()}"
        op.write(key, b"payload")
        assert op.read(key) == b"payload", name
        assert op.stat(key).content_length == len(b"payload"), name


@pytest.mark.asyncio
async def test_async_paths_read_write_roundtrip_identically():
    for name, op in _async_paths("/arw/").items():
        key = f"k-{uuid4()}"
        await op.write(key, b"payload")
        assert (await op.read(key)) == b"payload", name


def test_paths_pickle_consistently():
    for name, op in _blocking_paths("/pk/").items():
        restored = pickle.loads(pickle.dumps(op))
        assert isinstance(restored, opendal.Operator), name
        assert 'root="/pk/"' in repr(restored), name


def test_kwargs_and_config_agree_on_string_options():
    from_kwargs = opendal.Operator("memory", root="/agree/")
    from_config = opendal.Operator.from_config(MemoryConfig(root="/agree/"))
    assert _info(from_kwargs) == _info(from_config)


@pytest.mark.parametrize(
    "build",
    [
        lambda: opendal.Operator("s3", bucket="b", enable_versioning=True),
        lambda: opendal.AsyncOperator("s3", bucket="b", enable_versioning=True),
        lambda: opendal.Operator.from_uri("s3://b", enable_versioning=True),
        lambda: opendal.AsyncOperator.from_uri("s3://b", enable_versioning=True),
    ],
)
def test_non_string_kwarg_raises_clear_error(build):
    with pytest.raises(opendal.exceptions.ConfigInvalid, match="must be a string"):
        build()


def test_string_kwarg_accepted():
    op = opendal.Operator("s3", bucket="b", enable_versioning="true")
    assert isinstance(op, opendal.Operator)
