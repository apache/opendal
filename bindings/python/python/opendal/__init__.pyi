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

import os
from types import TracebackType
from typing import Any, AsyncIterable, Iterable, Optional, Type, Union, final

from opendal import exceptions as exceptions
from opendal import layers as layers
from opendal.__base import _Base
from opendal.layers import Layer

PathBuf = Union[str, os.PathLike]

@final
class Operator(_Base):
    """The entry class for all public blocking APIs.

    Args:
        scheme (str): The service name that OpenDAL supports.
        **options (any): The options for the service.
            See the documentation of each service for more details.

    Example:
        ```python
        import opendal
        op = opendal.Operator("s3", bucket="bucket", region="us-east-1")
        op.write("hello.txt", b"hello world")
        ```
    """
    def __init__(self, scheme: str, **options: Any) -> None: ...
    def layer(self, layer: Layer) -> "Operator":
        """Add new layers upon the current operator.

        Args:
            layer (Layer): The layer to be added.

        Returns:
            The new operator with the layer added.
        """
    def open(self, path: PathBuf, mode: str) -> File:
        """Open a file at the given path for reading or writing.

        Args:
            path (str|Path): The path to the file.
            mode (str): The mode to open the file. Can be "rb" or "wb".

        Returns:
            A file-like object that can be used to read or write the file.

        Example:
            ```python
            import opendal
            op = opendal.Operator("s3", bucket="bucket", region="us-east-1")
            with op.open("hello.txt", "wb") as f:
                f.write(b"hello world")
            ```
        """
    def read(self, path: PathBuf) -> bytes:
        """Read the content of the object at the given path.

        Args:
            path (str|Path): The path to the object.

        Returns:
            The content of the object as bytes.
        """
    def write(
        self,
        path: PathBuf,
        bs: bytes,
        *,
        append: bool = ...,
        chunk: int = ...,
        content_type: str = ...,
        content_disposition: str = ...,
        cache_control: str = ...,
    ) -> None: ...
    def stat(self, path: PathBuf) -> Metadata: ...
    def create_dir(self, path: PathBuf) -> None: ...
    def delete(self, path: PathBuf) -> None: ...
    def exists(self, path: PathBuf) -> bool: ...
    def list(self, path: PathBuf) -> Iterable[Entry]: ...
    def scan(self, path: PathBuf) -> Iterable[Entry]: ...
    def capability(self) -> Capability: ...
    def copy(self, source: PathBuf, target: PathBuf) -> None: ...
    def rename(self, source: PathBuf, target: PathBuf) -> None: ...
    def remove_all(self, path: PathBuf) -> None: ...
    def to_async_operator(self) -> AsyncOperator: ...

@final
class AsyncOperator(_Base):
    def layer(self, layer: Layer) -> "AsyncOperator": ...
    async def open(self, path: PathBuf, mode: str) -> AsyncFile: ...
    async def read(self, path: PathBuf) -> bytes: ...
    async def write(
        self,
        path: PathBuf,
        bs: bytes,
        *,
        append: bool = ...,
        chunk: int = ...,
        content_type: str = ...,
        content_disposition: str = ...,
        cache_control: str = ...,
    ) -> None: ...
    async def stat(self, path: PathBuf) -> Metadata: ...
    async def create_dir(self, path: PathBuf) -> None: ...
    async def delete(self, path: PathBuf) -> None: ...
    async def exists(self, path: PathBuf) -> bool: ...
    async def list(self, path: PathBuf) -> AsyncIterable[Entry]: ...
    async def scan(self, path: PathBuf) -> AsyncIterable[Entry]: ...
    async def presign_stat(
        self, path: PathBuf, expire_second: int
    ) -> PresignedRequest: ...
    async def presign_read(
        self, path: PathBuf, expire_second: int
    ) -> PresignedRequest: ...
    async def presign_write(
        self, path: PathBuf, expire_second: int
    ) -> PresignedRequest: ...
    async def presign_delete(
        self, path: PathBuf, expire_second: int
    ) -> PresignedRequest: ...
    def capability(self) -> Capability: ...
    async def copy(self, source: PathBuf, target: PathBuf) -> None: ...
    async def rename(self, source: PathBuf, target: PathBuf) -> None: ...
    async def remove_all(self, path: PathBuf) -> None: ...
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
    presign_delete: bool

    shared: bool
    blocking: bool
