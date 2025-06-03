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
from collections.abc import AsyncIterable, Iterable
from types import TracebackType
from typing import Any, Union, final

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
    def layer(self, layer: Layer) -> Operator:
        """Add new layers upon the current operator.

        Args:
            layer (Layer): The layer to be added.

        Returns:
            The new operator with the layer added.
        """
    def open(self, path: PathBuf, mode: str, **options: Any) -> File:
        """Open a file at the given path for reading or writing.

        Args:
            path (str | Path): The path to the file.
            mode (str): The mode to open the file. Must be either `"rb"` for reading or
                `"wb"` for writing.
            **options (Any): Additional options passed to the underlying OpenDAL reader
                or writer.
                - If `mode == "rb"`: options match the
                  [OpenDAL `ReaderOptions`](https://opendal.apache.org/docs/rust/opendal/options/struct.ReaderOptions.html).
                - If `mode == "wb"`: options match the
                  [OpenDAL `WriteOptions`](https://opendal.apache.org/docs/rust/opendal/options/struct.WriteOptions.html).

        Returns:
            File: A file-like object that can be used to read or write the file.

        Example:
            ```python
            import opendal

            op = opendal.Operator("s3", bucket="bucket", region="us-east-1")
            with op.open("hello.txt", "wb") as f:
                f.write(b"hello world")
            ```
        """
    def read(self, path: PathBuf, **options: Any) -> bytes:
        """Read the content of the object at the given path.

        Args:
            path (str | Path): The path to the object.
            **options (Any): Optional read parameters matching the
                [OpenDAL `ReadOptions`](https://opendal.apache.org/docs/rust/opendal/options/struct.ReadOptions.html):

                - offset (int): Byte offset to start reading from. Defaults to 0
                    if not specified.
                - size (int): Number of bytes to read. If not specified, reads until
                    the end of the object.
                  Together, `offset` and `size` define the byte range for reading.
                - version (str): Specify the version of the object to read, if
                    supported by the backend.
                - concurrent (int): Level of concurrency for reading. Defaults to
                    backend-specific value.
                - chunk (int): Read chunk size in bytes.
                - gap (int): Minimum gap (in bytes) between chunks to consider
                    them separate.
                - if_match (str): Read only if the ETag matches the given value.
                - if_none_match (str): Read-only if the ETag does not match the
                    given value.
                - if_modified_since (datetime): Only read if the object was modified
                    since this timestamp. This timestamp must be in UTC.
                - if_unmodified_since (datetime): Only read if the object was not
                    modified since this timestamp. This timestamp must be in UTC.

        Returns:
            bytes: The content of the object as bytes.
        """
    def write(self, path: PathBuf, bs: bytes, **options: Any) -> None:
        """Write the content to the object at the given path.

        Args:
            path (str | Path): The path to the object.
            bs (bytes): The content to write.
            **options (Any): Optional write parameters matching the
                [OpenDAL `WriteOptions`](https://opendal.apache.org/docs/rust/opendal/options/struct.WriteOptions.html):

                - append (bool): If True, append to the object instead of overwriting.
                - chunk (int): Specify the chunk size in bytes for multipart uploads.
                - concurrent (int): Number of concurrent upload parts. Larger values can
                    improve performance.
                - cache_control (str): Override the cache-control header for the object.
                - content_type (str): Explicitly set the Content-Type header for
                    the object.
                - content_disposition (str): Sets how the object should be presented
                    (e.g., as an attachment).
                - content_encoding (str): Override the Content-Encoding header.
                - if_match (str): Perform the write only if the object's current
                    ETag matches the given one.
                - if_none_match (str): Perform the write only if the object's
                    current ETag does NOT match the given one.
                - if_not_exists (bool): Only write the object if it doesn't
                    already exist.
                - user_metadata (dict[str, str]): Custom user metadata to associate
                    with the object.

        Returns:
            None
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
        """Convert into an async operator"""
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
    def layer(self, layer: Layer) -> AsyncOperator: ...
    async def open(self, path: PathBuf, mode: str, **options: Any) -> AsyncFile:
        """Open a file at the given path for reading or writing.

        Args:
            path (str | Path): The path to the file.
            mode (str): The mode to open the file. Must be either `"rb"` for reading or
                `"wb"` for writing.
            **options (Any): Additional options passed to the underlying OpenDAL reader
                or writer.
                - If `mode == "rb"`: options match the
                  [OpenDAL `ReaderOptions`](https://opendal.apache.org/docs/rust/opendal/options/struct.ReaderOptions.html).
                - If `mode == "wb"`: options match the
                  [OpenDAL `WriteOptions`](https://opendal.apache.org/docs/rust/opendal/options/struct.WriteOptions.html).

        Returns:
            AsyncFile: A file-like object that can be used to read or write the file.

        Example:
            ```python
            import opendal

            op = opendal.AsyncOperator("s3", bucket="bucket", region="us-east-1")
            async with await op.open("hello.txt", "wb") as f:
                await f.write(b"hello world")
            ```
        """
    async def read(self, path: PathBuf, **options: Any) -> bytes:
        """Read the content of the object at the given path.

        Args:
            path (str | Path): The path to the object.
            **options (Any): Optional read parameters matching the
                [OpenDAL `ReadOptions`](https://opendal.apache.org/docs/rust/opendal/options/struct.ReadOptions.html):

                - offset (int): Byte offset to start reading from. Defaults to 0
                    if not specified.
                - size (int): Number of bytes to read. If not specified, reads until
                    the end of the object.
                  Together, `offset` and `size` define the byte range for reading.
                - version (str): Specify the version of the object to read, if
                    supported by the backend.
                - concurrent (int): Level of concurrency for reading. Defaults to
                    backend-specific value.
                - chunk (int): Read chunk size in bytes.
                - gap (int): Minimum gap (in bytes) between chunks to consider
                    them separate.
                - override_content_type (str): Override the returned content type.
                - if_match (str): Read only if the ETag matches the given value.
                - if_none_match (str): Read-only if the ETag does not match the
                    given value.
                - if_modified_since (datetime): Only read if the object was modified
                    since this timestamp. This timestamp must be in UTC.
                - if_unmodified_since (datetime): Only read if the object was not
                    modified since this timestamp. This timestamp must be in UTC.

        Returns:
            The content of the object as bytes.
        """
    async def write(self, path: PathBuf, bs: bytes, **options: Any) -> None:
        """Write the content to the object at the given path.

        Args:
            path (str | Path): The path to the object.
            bs (bytes): The content to write.
            **options (Any): Optional write parameters matching the
                [OpenDAL `WriteOptions`](https://opendal.apache.org/docs/rust/opendal/options/struct.WriteOptions.html):

                - append (bool): If True, append to the object instead of overwriting.
                - chunk (int): Specify the chunk size in bytes for multipart uploads.
                - concurrent (int): Number of concurrent upload parts. Larger values can
                    improve performance.
                - cache_control (str): Override the cache-control header for the object.
                - content_type (str): Explicitly set the Content-Type header for
                    the object.
                - content_disposition (str): Sets how the object should be presented
                    (e.g., as an attachment).
                - content_encoding (str): Override the Content-Encoding header.
                - if_match (str): Perform the write only if the object's current
                    ETag matches the given one.
                - if_none_match (str): Perform the write only if the object's
                    current ETag does NOT match the given one.
                - if_not_exists (bool): Only write the object if it doesn't
                    already exist.
                - user_metadata (dict[str, str]): Custom user metadata to associate
                    with the object.

        Returns:
            None
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
    def read(self, size: int | None = None) -> bytes:
        """Read the content of the file.

        Args:
            size (int): The number of bytes to read. If None, read all.

        Returns:
            The content of the file as bytes.
        """
    def readline(self, size: int | None = None) -> bytes:
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
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit the runtime context related to this object."""
    @property
    def closed(self) -> bool:
        """Check if the file is closed."""
    def flush(self) -> None:
        """Flush the internal buffer."""
    def readable(self) -> bool:
        """Check if the file is readable."""
    def readinto(self, buffer: bytes | bytearray) -> int:
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
    async def read(self, size: int | None = None) -> bytes:
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
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
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
    def content_disposition(self) -> str | None:
        """The content disposition of the object."""
    @property
    def content_length(self) -> int:
        """The content length of the object."""
    @property
    def content_md5(self) -> str | None:
        """The MD5 checksum of the object."""
    @property
    def content_type(self) -> str | None:
        """The mime type of the object."""
    @property
    def etag(self) -> str | None:
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
    """If operator supports stat."""

    stat_with_if_match: bool
    """If operator supports stat with if match."""

    stat_with_if_none_match: bool
    """If operator supports stat with if none match."""

    read: bool
    """Indicates if the operator supports read operations."""

    read_with_if_match: bool
    """Indicates if conditional read operations using If-Match are supported."""

    read_with_if_none_match: bool
    """Indicates if conditional read operations using If-None-Match are supported."""

    read_with_if_modified_since: bool
    """If-Modified-Since condition supported for read."""

    read_with_if_unmodified_since: bool
    """If-Unmodified-Since condition supported for read."""

    read_with_override_cache_control: bool
    """Cache-Control header override supported for read."""

    read_with_override_content_disposition: bool
    """Content-Disposition header override supported for read."""

    read_with_override_content_type: bool
    """Indicates if Content-Type header override is supported during read operations."""

    read_with_version: bool
    """Indicates if versions read operations are supported."""

    write: bool
    """Indicates if the operator supports write operations."""

    write_can_multi: bool
    """Indicates if multiple write operations can be performed on the same object."""

    write_can_empty: bool
    """Indicates if writing empty content is supported."""

    write_can_append: bool
    """Indicates if append operations are supported."""

    write_with_content_type: bool
    """Indicates if Content-Type can be specified during write operations."""

    write_with_content_disposition: bool
    """Indicates if Content-Disposition can be specified during write operations."""

    write_with_content_encoding: bool
    """Indicates if Content-Encoding can be specified during write operations."""

    write_with_cache_control: bool
    """Indicates if Cache-Control can be specified during write operations."""

    write_with_if_match: bool
    """Indicates if conditional write operations using If-Match are supported."""

    write_with_if_none_match: bool
    """Indicates if conditional write operations using If-None-Match are supported."""

    write_with_if_not_exists: bool
    """Indicates if write operations can be conditional on object non-existence."""

    write_with_user_metadata: bool
    """Indicates if custom user metadata can be attached during write operations."""

    write_multi_max_size: int | None
    """Maximum part size for multipart uploads (e.g. 5GiB for AWS S3)."""

    write_multi_min_size: int | None
    """Minimum part size for multipart uploads (e.g. 5MiB for AWS S3)."""

    write_total_max_size: int | None
    """Maximum total size for write operations (e.g. 1MB for Cloudflare D1)."""

    create_dir: bool
    """If operator supports create dir."""

    delete: bool
    """If operator supports delete."""

    copy: bool
    """If operator supports copy."""

    rename: bool
    """If operator supports rename."""

    list: bool
    """If operator supports list."""

    list_with_limit: bool
    """If backend supports list with limit."""

    list_with_start_after: bool
    """If backend supports list with start after."""

    list_with_recursive: bool
    """If backend supports list with recursive."""

    presign: bool
    """If operator supports presign."""

    presign_read: bool
    """If operator supports presign read."""

    presign_stat: bool
    """If operator supports presign stat."""

    presign_write: bool
    """If operator supports presign write."""

    presign_delete: bool
    """If operator supports presign delete."""

    shared: bool
    """If operator supports shared."""
