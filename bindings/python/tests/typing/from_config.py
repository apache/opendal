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

# pyright: strict, reportUnnecessaryTypeIgnoreComment=true

from datetime import timedelta
from typing import assert_type

from opendal import AsyncOperator, Operator
from opendal.config import (
    ConcurrentLimitLayerConfig,
    ContextConfig,
    HttpContextConfig,
    LayerConfig,
    LoggingLayerConfig,
    MimeGuessLayerConfig,
    RetryLayerConfig,
    S3Config,
    TimeoutLayerConfig,
)

service = S3Config(scheme="s3", bucket="bucket")
context = ContextConfig(
    http=HttpContextConfig(
        proxy="http://127.0.0.1:7890",
        no_proxy=["localhost", ".internal"],
        connect_timeout=timedelta(seconds=1),
    )
)
layers: list[LayerConfig] = [
    ConcurrentLimitLayerConfig(name="concurrent_limit", permits=2),
    LoggingLayerConfig(name="logging"),
    MimeGuessLayerConfig(name="mime_guess"),
    RetryLayerConfig(name="retry", factor=1.5),
    TimeoutLayerConfig(name="timeout", io_timeout=timedelta(seconds=1)),
]
assert_type(
    Operator.from_config(config=service, context=context, layers=layers), Operator
)
assert_type(
    AsyncOperator.from_config(
        service,
        context={"http": {"proxy": "http://127.0.0.1:7890"}},
        layers=({"name": "retry", "factor": 2},),
    ),
    AsyncOperator,
)

Operator.from_config(service, context)  # type: ignore
Operator.from_config(service, context={"http": {"prox": "bad-key"}})  # type: ignore
AsyncOperator.from_config(service, layers=[{"name": "concurrent_limit"}])  # type: ignore
AsyncOperator.from_config(service, layers=[{"name": "retry", "factor": "1.5"}])  # type: ignore
AsyncOperator.from_config(service, layers=[{"name": "throttle"}])  # type: ignore
