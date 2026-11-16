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

import pickle
from datetime import timedelta
from pathlib import PurePosixPath

import pytest

import opendal
from opendal.config import FsConfig, MiniMokaConfig, MokaConfig, S3Config
from opendal.exceptions import Unsupported


@pytest.mark.need_capability("shared")
def test_from_config_constructs_configured_service(service_name, setup_config):
    operator = opendal.Operator.from_config(
        {"scheme": service_name, **setup_config}  # type: ignore[arg-type]
    )
    assert isinstance(operator, opendal.Operator)


def test_async_from_config_returns_expected_type():
    operator = opendal.AsyncOperator.from_config({"scheme": "memory"})
    assert isinstance(operator, opendal.AsyncOperator)


def test_from_config_operator_pickle(tmp_path):
    operator = opendal.Operator.from_config(FsConfig(scheme="fs", root=tmp_path))
    operator.write("test", b"hello")

    deserialized = pickle.loads(pickle.dumps(operator))
    assert deserialized.read("test") == b"hello"


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


@pytest.mark.parametrize("scheme", ["moka", "mini-moka"])
@pytest.mark.parametrize(
    "duration", ["2s", timedelta(days=1, seconds=2, microseconds=3)]
)
def test_from_config_cache_durations(service_name, scheme, duration):
    if service_name.replace("_", "-") != scheme:
        pytest.skip("requires the matching cache service")
    config_type = MokaConfig if scheme == "moka" else MiniMokaConfig
    config = config_type(scheme=scheme, time_to_live=duration, time_to_idle=duration)
    operator = opendal.Operator.from_config(config)
    operator.write("test", b"hello")
    assert operator.read("test") == b"hello"
    _, args = operator.__reduce__()
    expected = "2s" if isinstance(duration, str) else "86402000003us"
    assert args[1]["time_to_live"] == expected
    assert args[1]["time_to_idle"] == expected


@pytest.mark.parametrize("operator_type", [opendal.Operator, opendal.AsyncOperator])
@pytest.mark.parametrize(
    ("duration", "expected"),
    [
        (timedelta(microseconds=-1), "-1us"),
        (timedelta.max, "86399999999999999999us"),
    ],
)
def test_from_config_timedelta_precision(operator_type, duration, expected):
    operator = operator_type.from_config(
        {"scheme": "memory", "probe_duration": duration}  # type: ignore[arg-type]
    )
    _, args = operator.__reduce__()
    assert args[1]["probe_duration"] == expected
