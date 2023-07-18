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

class ConcurrentLimitLayer:
    def __init__(self, permits: int) -> None: ...

class ImmutableIndexLayer:
    def insert(self, key: str) -> None: ...

class RetryLayer:
    def __init__(
        self,
        max_times: int | None = None,
        factor: float | None = None,
        jitter: bool = False,
        max_delay: float | None = None,
        min_delay: float | None = None,
    ) -> None: ...
