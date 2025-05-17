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
    ) -> None:
        """Write the content to the object at the given path.

        Args:
            path (str|Path): The path to the object.
            bs (bytes): The content to write.
            append (bool): Whether to append the content to the object.
                Defaults to False.
            chunk (int): The chunk size for writing. Defaults to write all.
            content_type (str): The content type of the object.
                Defaults to None.
            content_disposition (str): The content disposition of the object.
                Defaults to None.
            cache_control (str): The cache control of the object.
                Defaults to None.
        """
    def stat(self, path: PathBuf) -> Metadata:
        """Get the metadata of the object at the given path.

        Args:
            path (str|Path): The path to the object.

        Returns:
            The metadata of the object.
        """
    def create_dir(self, path: PathBuf) -> None:
        """Create a directory at the given path.

        Args:
            path (str|Path): The path to the directory.
        """
    def delete(self, path: PathBuf) -> None:
        """Delete the object at the given path.

        Args:
            path (str|Path): The path to the object.
        """
    def exists(self, path: PathBuf) -> bool:
        """Check if the object at the given path exists.

        Args:
            path (str|Path): The path to the object.

        Returns:
            True if the object exists, False otherwise.
        """
    def list(self, path: PathBuf, *, start_after: str | None = None) -> Iterable[Entry]:
        """List the objects at the given path.

        Args:
            path (str|Path): The path to the directory.
            start_after (str | None): The key to start listing from.

        Returns:
            An iterable of entries representing the objects in the directory.
        """
    def scan(self, path: PathBuf) -> Iterable[Entry]:
        """Scan the objects at the given path recursively.

        Args:
            path (str|Path): The path to the directory.

        Returns:
            An iterable of entries representing the objects in the directory.
        """
    def capability(self) -> Capability:
        """Get the capability of the operator.

        Returns:
            The capability of the operator.
        """
    def copy(self, source: PathBuf, target: PathBuf) -> None:
        """Copy the object from source to target.

        Args:
            source (str|Path): The source path.
            target (str|Path): The target path.
        """
    def rename(self, source: PathBuf, target: PathBuf) -> None:
        """Rename the object from source to target.

        Args:
            source (str|Path): The source path.
            target (str|Path): The target path.
        """
    def remove_all(self, path: PathBuf) -> None:
        """Remove all objects at the given path.

        Args:
            path (str|Path): The path to the directory.
        """
    def to_async_operator(self) -> AsyncOperator: ...

@final
class AsyncOperator(_Base):
    """The entry class for all public async APIs.

    Args:
        scheme (str): The service name that OpenDAL supports.
        **options (any): The options for the service.
            See the documentation of each service for more details.

    Example:
        ```python
        import opendal
        op = opendal.AsyncOperator("s3", bucket="bucket", region="us-east-1")
        await op.write("hello.txt", b"hello world")
        ```
    """
    def __init__(self, scheme: str, **options: Any) -> None: ...
    def layer(self, layer: Layer) -> "AsyncOperator": ...
    async def open(self, path: PathBuf, mode: str) -> AsyncFile:
        """Open a file at the given path for reading or writing.

        Args:
            path (str|Path): The path to the file.
            mode (str): The mode to open the file. Can be "rb" or "wb".

        Returns:
            A file-like object that can be used to read or write the file.

        Example:
            ```python
            import opendal
            op = opendal.AsyncOperator("s3", bucket="bucket", region="us-east-1")
            async with await op.open("hello.txt", "wb") as f:
                await f.write(b"hello world")
            ```
        """
    async def read(self, path: PathBuf) -> bytes:
        """Read the content of the object at the given path.

        Args:
            path (str|Path): The path to the object.

        Returns:
            The content of the object as bytes.
        """
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
    ) -> None:
        """Write the content to the object at the given path.

        Args:
            path (str|Path): The path to the object.
            bs (bytes): The content to write.
            append (bool): Whether to append the content to the object.
                Defaults to False.
            chunk (int): The chunk size for writing. Defaults to write all.
            content_type (str): The content type of the object.
                Defaults to None.
            content_disposition (str): The content disposition of the object.
                Defaults to None.
            cache_control (str): The cache control of the object.
                Defaults to None.
        """
    async def stat(self, path: PathBuf) -> Metadata:
        """Get the metadata of the object at the given path.

        Args:
            path (str|Path): The path to the object.

        Returns:
            The metadata of the object.
        """
    async def create_dir(self, path: PathBuf) -> None:
        """Create a directory at the given path.

        Args:
            path (str|Path): The path to the directory.
        """
    async def delete(self, path: PathBuf) -> None:
        """Delete the object at the given path.

        Args:
            path (str|Path): The path to the object.
        """
    async def exists(self, path: PathBuf) -> bool:
        """Check if the object at the given path exists.

        Args:
            path (str|Path): The path to the object.

        Returns:
            True if the object exists, False otherwise.
        """
    async def list(
        self, path: PathBuf, *, start_after: str | None = None
    ) -> AsyncIterable[Entry]:
        """List the objects at the given path.

        Args:
            path (str|Path): The path to the directory.
            start_after (str | None): The key to start listing from.

        Returns:
            An iterable of entries representing the objects in the directory.
        """
    async def scan(self, path: PathBuf) -> AsyncIterable[Entry]:
        """Scan the objects at the given path recursively.

        Args:
            path (str|Path): The path to the directory.

        Returns:
            An iterable of entries representing the objects in the directory.
        """
    async def presign_stat(self, path: PathBuf, expire_second: int) -> PresignedRequest:
        """Generate a presigned URL for stat operation.

        Args:
            path (str|Path): The path to the object.
            expire_second (int): The expiration time in seconds.

        Returns:
            A presigned request object.
        """
    async def presign_read(self, path: PathBuf, expire_second: int) -> PresignedRequest:
        """Generate a presigned URL for read operation.

        Args:
            path (str|Path): The path to the object.
            expire_second (int): The expiration time in seconds.

        Returns:
            A presigned request object.
        """
    async def presign_write(
        self, path: PathBuf, expire_second: int
    ) -> PresignedRequest:
        """Generate a presigned URL for write operation.

        Args:
            path (str|Path): The path to the object.
            expire_second (int): The expiration time in seconds.

        Returns:
            A presigned request object.
        """
    async def presign_delete(
        self, path: PathBuf, expire_second: int
    ) -> PresignedRequest:
        """Generate a presigned URL for delete operation.

        Args:
            path (str|Path): The path to the object.
            expire_second (int): The expiration time in seconds.

        Returns:
            A presigned request object.
        """
    def capability(self) -> Capability: ...
    async def copy(self, source: PathBuf, target: PathBuf) -> None:
        """Copy the object from source to target.

        Args:
            source (str|Path): The source path.
            target (str|Path): The target path.
        """
    async def rename(self, source: PathBuf, target: PathBuf) -> None:
        """Rename the object from source to target.

        Args:
            source (str|Path): The source path.
            target (str|Path): The target path.
        """
    async def remove_all(self, path: PathBuf) -> None:
        """Remove all objects at the given path.

        Args:
            path (str|Path): The path to the directory.
        """
    def to_operator(self) -> Operator: ...

@final
class File:
    """
    A file-like object for reading and writing data.

    Created by the `open` method of the `Operator` class.
    """
    def read(self, size: Optional[int] = None) -> bytes:
        """Read the content of the file.

        Args:
            size (int): The number of bytes to read. If None, read all.

        Returns:
            The content of the file as bytes.
        """
    def readline(self, size: Optional[int] = None) -> bytes:
        """Read a single line from the file.

        Args:
            size (int): The number of bytes to read. If None, read until newline.

        Returns:
            The line read from the file as bytes.
        """
    def write(self, bs: bytes) -> None:
        """Write the content to the file.

        Args:
            bs (bytes): The content to write.
        """
    def seek(self, pos: int, whence: int = 0) -> int:
        """Set the file's current position.

        Args:
            pos (int): The position to set.
            whence (int): The reference point for the position. Can be 0, 1, or 2.

        Returns:
            The new position in the file.
        """
    def tell(self) -> int:
        """Get the current position in the file.

        Returns:
            The current position in the file.
        """
    def close(self) -> None:
        """Close the file."""
    def __enter__(self) -> File:
        """Enter the runtime context related to this object."""
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Exit the runtime context related to this object."""
    @property
    def closed(self) -> bool:
        """Check if the file is closed."""
    def flush(self) -> None:
        """Flush the internal buffer."""
    def readable(self) -> bool:
        """Check if the file is readable."""
    def readinto(self, buffer: Union[bytes, bytearray]) -> int:
        """Read bytes into a buffer.

        Args:
            buffer (bytes|bytearray): The buffer to read into.

        Returns:
            The number of bytes read.
        """
    def seekable(self) -> bool:
        """Check if the file supports seeking."""
    def writable(self) -> bool:
        """Check if the file is writable."""

@final
class AsyncFile:
    """
    A file-like object for reading and writing data.

    Created by the `open` method of the `AsyncOperator` class.
    """
    async def read(self, size: Optional[int] = None) -> bytes:
        """Read the content of the file.

        Args:
            size (int): The number of bytes to read. If None, read all.

        Returns:
            The content of the file as bytes.
        """
    async def write(self, bs: bytes) -> None:
        """Write the content to the file.

        Args:
            bs (bytes): The content to write.
        """
    async def seek(self, pos: int, whence: int = 0) -> int:
        """Set the file's current position.

        Args:
            pos (int): The position to set.
            whence (int): The reference point for the position. Can be 0, 1, or 2.

        Returns:
            The new position in the file.
        """
    async def tell(self) -> int:
        """Get the current position in the file.

        Returns:
            The current position in the file.
        """
    async def close(self) -> None:
        """Close the file."""
    def __aenter__(self) -> AsyncFile:
        """Enter the runtime context related to this object."""
    def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Exit the runtime context related to this object."""
    @property
    async def closed(self) -> bool:
        """Check if the file is closed."""
    async def readable(self) -> bool:
        """Check if the file is readable."""
    async def seekable(self) -> bool:
        """Check if the file supports seeking."""
    async def writable(self) -> bool:
        """Check if the file is writable."""

@final
class Entry:
    """An entry in the directory listing."""
    @property
    def path(self) -> str:
        """The path of the entry."""

@final
class Metadata:
    @property
    def content_disposition(self) -> Optional[str]:
        """The content disposition of the object."""
    @property
    def content_length(self) -> int:
        """The content length of the object."""
    @property
    def content_md5(self) -> Optional[str]:
        """The MD5 checksum of the object."""
    @property
    def content_type(self) -> Optional[str]:
        """The mime type of the object."""
    @property
    def etag(self) -> Optional[str]:
        """The ETag of the object."""
    @property
    def mode(self) -> EntryMode:
        """The mode of the object."""

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
    """Storage capability information."""

    stat: bool
    """If operator supports stat"""

    stat_with_if_match: bool
    """If operator supports stat with if match"""

    stat_with_if_none_match: bool
    """If operator supports stat with if none match"""

    read: bool
    """If operator supports read"""

    read_with_if_match: bool
    """If operator supports read with if match"""

    read_with_if_none_match: bool
    """If operator supports read with if none match"""

    read_with_override_cache_control: bool
    """If operator supports read with override cache control"""

    read_with_override_content_disposition: bool
    """If operator supports read with override content disposition"""

    read_with_override_content_type: bool
    """If operator supports read with override content type"""

    write: bool
    """If operator supports write"""

    write_can_multi: bool
    """If operator supports write can be called in multi times"""

    write_can_empty: bool
    """If operator supports write with empty content"""

    write_can_append: bool
    """If operator supports write by append"""

    write_with_content_type: bool
    """If operator supports write with content type"""

    write_with_content_disposition: bool
    """If operator supports write with content disposition"""

    write_with_cache_control: bool
    """If operator supports write with cache control"""

    write_multi_max_size: Optional[int]
    """Write_multi_max_size is the max size that services support in write_multi.
    For example, AWS S3 supports 5GiB as max in write_multi."""

    write_multi_min_size: Optional[int]
    """Write_multi_min_size is the min size that services support in write_multi.
    For example, AWS S3 requires at least 5MiB in write_multi expect the last one."""

    write_total_max_size: Optional[int]
    """Write_total_max_size is the max size that services support in write_total.
    For example, Cloudflare D1 supports 1MB as max in write_total."""

    create_dir: bool
    """If operator supports create dir"""

    delete: bool
    """If operator supports delete"""

    copy: bool
    """If operator supports copy"""

    rename: bool
    """If operator supports rename"""

    list: bool
    """If operator supports list"""

    list_with_limit: bool
    """If backend supports list with limit"""

    list_with_start_after: bool
    """If backend supports list with start after"""

    list_with_recursive: bool
    """If backend supports list with recursive"""

    presign: bool
    """If operator supports presign"""

    presign_read: bool
    """If operator supports presign read"""

    presign_stat: bool
    """If operator supports presign stat"""

    presign_write: bool
    """If operator supports presign write"""

    presign_delete: bool
    """If operator supports presign delete"""

    shared: bool
    """If operator supports shared"""

    blocking: bool
    """If operator supports blocking"""
