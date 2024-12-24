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

from typing import (
    AsyncIterable,
    Iterable,
    Literal,
    Optional,
    TypeAlias,
    final,
    Union,
    Type,
    overload,
)
from types import TracebackType

from opendal import exceptions as exceptions
from opendal import layers as layers
from opendal.layers import Layer

# `true`/`false`` in any case, for example, `true`/`True`/`TRUE` `false`/`False`/`FALSE`
_bool: TypeAlias = str

class _Base:
    @overload
    def __init__(
        self,
        scheme: Literal["s3"],
        *,
        bucket: str,
        region: str,
        endpoint: str = ...,
        root: str = ...,
        access_key_id: str = ...,
        secret_access_key: str = ...,
        default_storage_class: str = ...,
        server_side_encryption: str = ...,
        server_side_encryption_aws_kms_key_id: str = ...,
        server_side_encryption_customer_algorithm: str = ...,
        server_side_encryption_customer_key: str = ...,
        server_side_encryption_customer_key_md5: str = ...,
        disable_config_load: _bool = ...,
        enable_virtual_host_style: _bool = ...,
        disable_write_with_if_match: _bool = ...,
    ) -> None: ...
    @overload
    def __init__(self, scheme: str, **kwargs: str) -> None: ...

@final
class Operator(_Base):
    def layer(self, layer: Layer) -> "Operator": ...
    def open(self, path: str, mode: str) -> File: ...
    def read(self, path: str) -> bytes: ...
    def write(
        self,
        path: str,
        bs: bytes,
        *,
        append: bool = ...,
        chunk: int = ...,
        content_type: str = ...,
        content_disposition: str = ...,
        cache_control: str = ...,
    ) -> None: ...
    def stat(self, path: str) -> Metadata: ...
    def create_dir(self, path: str) -> None: ...
    def delete(self, path: str) -> None: ...
    def list(self, path: str) -> Iterable[Entry]: ...
    def scan(self, path: str) -> Iterable[Entry]: ...
    def capability(self) -> Capability: ...
    def copy(self, source: str, target: str) -> None: ...
    def rename(self, source: str, target: str) -> None: ...
    def remove_all(self, path: str) -> None: ...
    def to_async_operator(self) -> AsyncOperator: ...

@final
class AsyncOperator(_Base):
    def layer(self, layer: Layer) -> "AsyncOperator": ...
    async def open(self, path: str, mode: str) -> AsyncFile: ...
    async def read(self, path: str) -> bytes: ...
    async def write(
        self,
        path: str,
        bs: bytes,
        *,
        append: bool = ...,
        chunk: int = ...,
        content_type: str = ...,
        content_disposition: str = ...,
        cache_control: str = ...,
    ) -> None: ...
    async def stat(self, path: str) -> Metadata: ...
    async def create_dir(self, path: str) -> None: ...
    async def delete(self, path: str) -> None: ...
    async def list(self, path: str) -> AsyncIterable[Entry]: ...
    async def scan(self, path: str) -> AsyncIterable[Entry]: ...
    async def presign_stat(self, path: str, expire_second: int) -> PresignedRequest: ...
    async def presign_read(self, path: str, expire_second: int) -> PresignedRequest: ...
    async def presign_write(
        self, path: str, expire_second: int
    ) -> PresignedRequest: ...
    def capability(self) -> Capability: ...
    async def copy(self, source: str, target: str) -> None: ...
    async def rename(self, source: str, target: str) -> None: ...
    async def remove_all(self, path: str) -> None: ...
    def to_operator(self) -> Operator: ...

@final
class File:
    def read(self, size: Optional[int] = None) -> bytes: ...
    def readline(self, size: Optional[int] = None) -> bytes: ...
    def write(self, bs: bytes) -> None: ...
    def seek(self, pos: int, whence: int = 0) -> int: ...
    def tell(self) -> int: ...
    def close(self) -> None: ...
    def __enter__(self) -> File: ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None: ...
    @property
    def closed(self) -> bool: ...
    def flush(self) -> None: ...
    def readable(self) -> bool: ...
    def readinto(self, buffer: Union[bytes, bytearray]) -> int: ...
    def seekable(self) -> bool: ...
    def writable(self) -> bool: ...

@final
class AsyncFile:
    async def read(self, size: Optional[int] = None) -> bytes: ...
    async def write(self, bs: bytes) -> None: ...
    async def seek(self, pos: int, whence: int = 0) -> int: ...
    async def tell(self) -> int: ...
    async def close(self) -> None: ...
    def __aenter__(self) -> AsyncFile: ...
    def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None: ...
    @property
    async def closed(self) -> bool: ...
    async def readable(self) -> bool: ...
    async def seekable(self) -> bool: ...
    async def writable(self) -> bool: ...

@final
class Entry:
    @property
    def path(self) -> str: ...

@final
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

@final
class EntryMode:
    def is_file(self) -> bool: ...
    def is_dir(self) -> bool: ...

@final
class PresignedRequest:
    @property
    def url(self) -> str: ...
    @property
    def method(self) -> str: ...
    @property
    def headers(self) -> dict[str, str]: ...

@final
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

    shared: bool
    blocking: bool
