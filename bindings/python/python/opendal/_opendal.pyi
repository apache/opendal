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

from typing import Final, final

from _typeshed import Incomplete

from .operator import AsyncOperator, Operator

__version__: Final[str]

@final
class DeleteOptions: ...

@final
class ListOptions: ...

@final
class ReadOptions: ...

@final
class StatOptions: ...

@final
class WriteOptions: ...

def _reconstruct_async_operator(scheme: str, map: dict[str, str]) -> AsyncOperator:
    """
    Rebuild an [`AsyncOperator`] while unpickling.

    See [`_reconstruct_operator`] for why a dedicated reconstructor is used.
    """

def _reconstruct_operator(scheme: str, map: dict[str, str]) -> Operator:
    """
    Rebuild a blocking [`Operator`] while unpickling.

    Reconstruction goes through `from_uri` (not the scheme-based `__new__`)
    because `__scheme` may hold a full URI that `__new__` would normalize and
    corrupt. A bare scheme is also accepted here, since the core resolves both
    bare schemes and full URIs through the same path.
    """

def __getattr__(name: str) -> Incomplete: ...
