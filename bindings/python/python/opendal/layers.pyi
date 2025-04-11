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

from typing import Optional, final

class Layer:
    """Base class for all layers."""

@final
class RetryLayer(Layer):
    """Add retry for temporary failed operations.

    Args:
        max_times (int, optional): Maximum retry times. Defaults to None.
        factor (float, optional): Exponential backoff factor. Defaults to None.
        jitter (bool, optional): Whether to add jitter. Defaults to False.
        max_delay (float, optional): Maximum delay time. Defaults to None.
        min_delay (float, optional): Minimum delay time. Defaults to None.

    Example:
        ```python
        import opendal
        from opendal.layers import RetryLayer

        op = opendal.Operator('s3', region='us-east-1', bucket='mybucket').layer(
            RetryLayer(max_times=3, factor=2.0, jitter=True)
        )
        ```
    """
    def __init__(
        self,
        max_times: Optional[int] = None,
        factor: Optional[float] = None,
        jitter: bool = False,
        max_delay: Optional[float] = None,
        min_delay: Optional[float] = None,
    ) -> None: ...

@final
class ConcurrentLimitLayer(Layer):
    def __init__(self, limit: int) -> None: ...
