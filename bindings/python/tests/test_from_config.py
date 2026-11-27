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
from collections import UserDict
from datetime import timedelta
from pathlib import PurePosixPath
from types import MappingProxyType

import pytest

import opendal
from opendal.config import (
    ConcurrentLimitLayerConfig,
    FsConfig,
    MiniMokaConfig,
    MokaConfig,
    RetryLayerConfig,
    S3Config,
)
from opendal.exceptions import ConfigInvalid, Unsupported


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


class TimedeltaSubclass(timedelta):
    microseconds = 0


@pytest.mark.parametrize("operator_type", [opendal.Operator, opendal.AsyncOperator])
@pytest.mark.parametrize(
    ("duration", "expected"),
    [
        (timedelta(0), "0us"),
        (timedelta(microseconds=-1), "-1us"),
        (timedelta(days=2, seconds=3, microseconds=4), "172803000004us"),
        (timedelta.min, "-86399999913600000000us"),
        (timedelta.max, "86399999999999999999us"),
        (TimedeltaSubclass(microseconds=1), "1us"),
    ],
)
def test_from_config_timedelta_precision(operator_type, duration, expected):
    operator = operator_type.from_config(
        {"scheme": "memory", "probe_duration": duration}  # type: ignore[arg-type]
    )
    _, args = operator.__reduce__()
    assert args[1]["probe_duration"] == expected


def test_from_config_stringifies_flat_list_and_tuple():
    op = opendal.Operator.from_config(
        {
            "scheme": "memory",
            "probe_list": ["a", "b"],
            "probe_tuple": (
                "x",
                1,
                True,
                PurePosixPath("/p"),
                timedelta(seconds=1),
            ),
        }  # type: ignore[arg-type]
    )
    _, args = op.__reduce__()
    assert args[1]["probe_list"] == "a,b"
    assert args[1]["probe_tuple"] == "x,1,true,/p,1000000us"


class BytesPathLike:
    def __fspath__(self) -> bytes:
        """Return a bytes path."""
        return b"/tmp/bytes-path"


def _make_cyclic_list() -> list[object]:
    cyclic: list[object] = []
    cyclic.append(cyclic)
    return cyclic


@pytest.mark.parametrize("operator_type", [opendal.Operator, opendal.AsyncOperator])
@pytest.mark.parametrize(
    "bad_value",
    [
        b"/tmp/foo",
        bytearray(b"/tmp/foo"),
        memoryview(b"/tmp/foo"),
        BytesPathLike(),
        {"a": "1"},
        MappingProxyType({"a": "1", "b": "2"}),
        UserDict({"a": "1"}),
        {"a", "b"},
        frozenset({"a", "b"}),
        [["nested"]],
        (("nested",),),
        _make_cyclic_list(),
        [b"/tmp/foo"],
        [MappingProxyType({"a": "1"})],
    ],
)
def test_from_config_rejects_unsupported_values(operator_type, bad_value):
    with pytest.raises(Unsupported):
        operator_type.from_config(
            {"scheme": "memory", "probe": bad_value}  # type: ignore[arg-type]
        )


def test_from_config_rejects_bytes_root_for_fs():
    with pytest.raises(Unsupported):
        opendal.Operator.from_config(
            {"scheme": "fs", "root": b"/tmp/foo"}  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("operator_type", [opendal.Operator, opendal.AsyncOperator])
@pytest.mark.parametrize("factor", [float("nan"), float("inf"), float("-inf")])
def test_from_config_rejects_nonfinite_floats(operator_type, factor):
    with pytest.raises(ConfigInvalid, match="config floats must be finite"):
        operator_type.from_config(
            {"scheme": "memory"}, layers=[RetryLayerConfig(name="retry", factor=factor)]
        )


@pytest.mark.parametrize("operator_type", [opendal.Operator, opendal.AsyncOperator])
def test_from_config_context_and_layers_are_keyword_only(operator_type):
    with pytest.raises(TypeError, match="positional"):
        operator_type.from_config({"scheme": "memory"}, {})
    with pytest.raises(TypeError, match="positional"):
        operator_type.from_config({"scheme": "memory"}, {}, [])


def test_from_config_validates_layer_options():
    with pytest.raises(ConfigInvalid, match="positive number of permits"):
        opendal.Operator.from_config(
            {"scheme": "memory"},
            layers=[ConcurrentLimitLayerConfig(name="concurrent_limit", permits=0)],
        )

    retry = RetryLayerConfig(name="retry")
    with pytest.raises(ConfigInvalid, match="duplicate name in layers list"):
        opendal.Operator.from_config({"scheme": "memory"}, layers=[retry, retry])
