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

from collections.abc import Iterable
import os
from typing import Optional

from ._opendal import *

__all__ = _opendal.__all__

_PATH_TYPES = (str, bytes, os.PathLike)


def _coerce_many_paths(path: object) -> Optional[list[str]]:
    if isinstance(path, _PATH_TYPES):
        return None

    try:
        iterator = iter(path)  # type: ignore[arg-type]
    except TypeError:
        return None

    paths = list(iterator)
    if any(not isinstance(p, _PATH_TYPES) for p in paths):
        raise TypeError("all paths must be str, bytes, or os.PathLike objects")
    return paths


_blocking_delete = Operator.delete


def _operator_delete(self: Operator, path: object) -> None:
    paths = _coerce_many_paths(path)
    if paths is None:
        _blocking_delete(self, path)
        return
    if not paths:
        return
    self.delete_many(paths)


Operator.delete = _operator_delete  # type: ignore[assignment]

_async_delete = AsyncOperator.delete


async def _async_operator_delete(self: AsyncOperator, path: object) -> None:
    paths = _coerce_many_paths(path)
    if paths is None:
        await _async_delete(self, path)  # type: ignore[arg-type]
        return
    if not paths:
        return
    await self.delete_many(paths)


AsyncOperator.delete = _async_operator_delete  # type: ignore[assignment]
