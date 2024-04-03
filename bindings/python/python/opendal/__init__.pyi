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

from typing import AsyncIterable, Iterable, Optional

from opendal.layers import Layer


class Operator:
    def __init__(self, scheme: str, **kwargs): ...

    def layer(self, layer: Layer): ...

    def open(self, path: str, mode: str) -> File: ...

    def read(self, path: str) -> memoryview: ...

    def write(
        self,
        path: str,
        bs: bytes,
        append: Optional[bool] = None,
        buffer: Optional[int] = None,
        content_type: Optional[str] = None,
        content_disposition: Optional[str] = None,
        cache_control: Optional[str] = None,
    ): ...

    def stat(self, path: str) -> Metadata: ...

    def create_dir(self, path: str): ...

    def delete(self, path: str): ...

    def list(self, path: str) -> Iterable[Entry]: ...

    def scan(self, path: str) -> Iterable[Entry]: ...

    def capability(self) -> Capability: ...

    def copy(self, source: str, target: str): ...

    def rename(self, source: str, target: str): ...

    def remove_all(self, path: str): ...

    def to_async_operator(self) -> AsyncOperator: ...


class AsyncOperator:
    def __init__(self, scheme: str, **kwargs): ...

    def layer(self, layer: Layer): ...

    async def open(self, path: str, mode: str) -> AsyncFile: ...

    async def read(self, path: str) -> memoryview: ...

    async def write(
        self,
        path: str,
        bs: bytes,
        append: Optional[bool] = None,
        buffer: Optional[int] = None,
        content_type: Optional[str] = None,
        content_disposition: Optional[str] = None,
        cache_control: Optional[str] = None,
    ): ...

    async def stat(self, path: str) -> Metadata: ...

    async def create_dir(self, path: str): ...

    async def delete(self, path: str): ...

    async def list(self, path: str) -> AsyncIterable[Entry]: ...

    async def scan(self, path: str) -> AsyncIterable[Entry]: ...

    async def presign_stat(self, path: str, expire_second: int) -> PresignedRequest: ...

    async def presign_read(self, path: str, expire_second: int) -> PresignedRequest: ...

    async def presign_write(
        self, path: str, expire_second: int
    ) -> PresignedRequest: ...

    def capability(self) -> Capability: ...

    async def copy(self, source: str, target: str): ...

    async def rename(self, source: str, target: str): ...

    async def remove_all(self, path: str): ...

    def to_operator(self) -> Operator: ...


class File:
    def read(self, size: Optional[int] = None) -> memoryview: ...

    def write(self, bs: bytes): ...

    def seek(self, offset: int, whence: int = 0) -> int: ...

    def tell(self) -> int: ...

    def close(self): ...

    def __enter__(self) -> File: ...

    def __exit__(self, exc_type, exc_value, traceback) -> None: ...


class AsyncFile:
    async def read(self, size: Optional[int] = None) -> memoryview: ...

    async def write(self, bs: bytes): ...

    async def seek(self, offset: int, whence: int = 0) -> int: ...

    async def tell(self) -> int: ...

    async def close(self): ...

    def __aenter__(self) -> AsyncFile: ...

    def __aexit__(self, exc_type, exc_value, traceback) -> None: ...


class Entry:
    @property
    def path(self) -> str: ...


class Metadata:
    @property
    def content_disposition(self) -> Optional[str]: ...

    @property
    def content_length(self) -> int: ...

    @property
    def content_md5(self) -> Optional[str]: ...

    @property
    def content_type(self) -> Optional[str]: ...

    @property
    def etag(self) -> Optional[str]: ...

    @property
    def mode(self) -> EntryMode: ...


class EntryMode:
    def is_file(self) -> bool: ...

    def is_dir(self) -> bool: ...


class PresignedRequest:
    @property
    def url(self) -> str: ...

    @property
    def method(self) -> str: ...

    @property
    def headers(self) -> dict[str, str]: ...


class Capability:
    stat: bool
    stat_with_if_match: bool
    stat_with_if_none_match: bool

    read: bool
    read_with_if_match: bool
    read_with_if_none_match: bool
    read_with_override_cache_control: bool
    read_with_override_content_disposition: bool
    read_with_override_content_type: bool

    write: bool
    write_can_multi: bool
    write_can_empty: bool
    write_can_append: bool
    write_with_content_type: bool
    write_with_content_disposition: bool
    write_with_cache_control: bool
    write_multi_max_size: Optional[int]
    write_multi_min_size: Optional[int]
    write_multi_align_size: Optional[int]
    write_total_max_size: Optional[int]

    create_dir: bool
    delete: bool
    copy: bool
    rename: bool

    list: bool
    list_with_limit: bool
    list_with_start_after: bool
    list_without_recursive: bool
    list_with_recursive: bool

    presign: bool
    presign_read: bool
    presign_stat: bool
    presign_write: bool

    batch: bool
    batch_delete: bool
    batch_max_operations: Optional[int]

    blocking: bool
