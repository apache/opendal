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

from __future__ import annotations

import pytest

import opendal


@pytest.fixture(scope="session")
def operator():  # type: ignore[override]
    class _Caps:
        def __getattr__(self, item: str) -> bool:
            return False

    class _Dummy:
        def capability(self) -> _Caps:
            return _Caps()

    return _Dummy()


@pytest.fixture(scope="session")
def async_operator():  # type: ignore[override]
    class _Caps:
        def __getattr__(self, item: str) -> bool:
            return False

    class _Dummy:
        def capability(self) -> _Caps:
            return _Caps()

    return _Dummy()


def test_s3_region_auto_detect():
    op = opendal.Operator(
        "s3",
        bucket="auto-detect-test",
        endpoint="https://s3.eu-central-1.amazonaws.com",
        disable_config_load="true",
        allow_anonymous="true",
    )

    # Building succeeds without region provided and returns a valid operator
    assert 'Operator("s3"' in repr(op)
    async_op = op.to_async_operator()
    assert 'AsyncOperator("s3"' in repr(async_op)


def test_s3_region_auto_detect_failure():
    with pytest.raises(opendal.exceptions.ConfigInvalid):
        opendal.Operator(
            "s3",
            bucket="definitely-not-a-real-bucket",
            endpoint="https://no-such-endpoint.invalid",
            disable_config_load="true",
            allow_anonymous="true",
        )
