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

import collections.abc
from datetime import datetime
from os import PathLike
from typing import Any, SupportsIndex, final

from typing_extensions import Unpack

from .capability import Capability
from .config import ContextConfig, LayerConfig, ServiceConfig
from .file import AsyncFile, File
from .layers import Layer
from .options import ChecksumAlgorithm, OpenKwargs
from .services import Scheme
from .types import Entry, Metadata, PresignedRequest

__all__ = ["AsyncOperator", "Operator"]

@final
class AsyncOperator:
    """
    The async equivalent of `Operator`.

    `AsyncOperator` is the entry point for all async APIs.

    See Also
    --------
    Operator
    """

    def __new__(
        cls, /, scheme: str | Scheme, **kwargs: str | PathLike[str]
    ) -> AsyncOperator:
        """
        Create a new `AsyncOperator`.

        Parameters
        ----------
        scheme : str | Scheme
            The scheme of the service.
        **kwargs : dict
            The options for the service. Path-like values are converted to strings.

        Returns
        -------
        AsyncOperator
            The new async operator.
        """
    def __reduce__(self, /) -> Any: ...
    def capability(self, /) -> Capability:
        """
        Get all capabilities of this operator.

        Returns
        -------
        Capability
            The capability of the operator.
        """
    def check(self, /) -> collections.abc.Awaitable[None]:
        """
        Check if the operator is able to work correctly.

        Returns
        -------
        coroutine
            An awaitable that completes when the check is finished.

        Raises
        ------
        Exception
            If the operator is not able to work correctly.
        """
    def copy(
        self,
        /,
        source: str | PathLike[str],
        target: str | PathLike[str],
        *,
        if_not_exists: bool | None = None,
    ) -> collections.abc.Awaitable[None]:
        """
        Copy a file from one path to another.

        Parameters
        ----------
        source : str
            The path to the source file.
        target : str
            The path to the target file.
        if_not_exists : bool, optional
            If True, require `copy_with_if_not_exists` and raise `Unsupported`
            if the service lacks it. Raise `ConditionNotMatch` if the target
            already exists. Defaults to False, which allows overwriting.

        Returns
        -------
        coroutine
            An awaitable that completes when the copy is finished.
        """
    def create_dir(
        self, /, path: str | PathLike[str]
    ) -> collections.abc.Awaitable[None]:
        """
        Create a directory at the given path.

        Notes
        -----
        To indicate that a path is a directory, it must end with a `/`.
        This operation is always recursive, like `mkdir -p`.

        Parameters
        ----------
        path : str
            The path to the directory.

        Returns
        -------
        coroutine
            An awaitable that completes when the directory is created.
        """
    def delete(
        self,
        /,
        path: str | PathLike[str],
        *,
        version: str | None = None,
        recursive: bool | None = None,
        if_match: str | None = None,
    ) -> collections.abc.Awaitable[None]:
        """
        Delete a file at the given path.

        Notes
        -----
        This operation will not return an error if the path does not exist.

        Parameters
        ----------
        path : str
            The path to the file.

        Returns
        -------
        coroutine
            An awaitable that completes when the file is deleted.
        version : str, optional
            The version of the file to delete. Only supported on version-aware backends.
        recursive : bool, optional
            If True, delete the path recursively.
            Only supported on backends that support recursive delete.
        if_match : str, optional
            If set, only delete when the existing object's ETag matches.
        """
    def delete_many(
        self, /, paths: collections.abc.Iterable[str | PathLike[str]]
    ) -> collections.abc.Awaitable[None]:
        """
        Delete an explicit collection of paths with default delete options.

        Notes
        -----
        This method collects the iterable and converts its Python path objects
        before deletion starts, using memory proportional to the number of paths.
        Empty collections do nothing. Paths that do not exist are ignored.
        Storage or path-validation errors can leave some paths deleted; this
        operation is not atomic and does not guarantee a single remote request.
        Use `remove_all` to recursively delete a path prefix.

        Parameters
        ----------
        paths : Iterable[str | PathLike[str]]
            The paths to delete. A single path is not accepted.

        Returns
        -------
        Awaitable[None]
            An awaitable that completes when deletion finishes.
        """
    def exists(self, /, path: str | PathLike[str]) -> collections.abc.Awaitable[bool]:
        """
        Check if a path exists.

        Parameters
        ----------
        path : str
            The path to check.

        Returns
        -------
        coroutine
            An awaitable that returns True if the path exists, False otherwise.
        """
    @classmethod
    def from_config(
        cls,
        /,
        config: ServiceConfig,
        *,
        context: ContextConfig | None = None,
        layers: collections.abc.Sequence[LayerConfig] | None = None,
    ) -> AsyncOperator:
        """
        Create a new `AsyncOperator` from typed configuration.

        A static type checker validates the service scheme and the options for
        the service, context, and layers. The binding converts non-string values
        such as booleans, integers, finite floats, durations, paths, and flat
        lists or tuples to the strings the registry consumes.
        Service config values such as bytes, mappings, and nested sequences
        raise ``Unsupported``.

        Construction options are stored when pickling the operator and may
        include credentials. Reapply layers added via ``.layer()`` after unpickling.

        Parameters
        ----------
        config : ServiceConfig
            A service configuration such as ``opendal.config.S3Config``.
        context : ContextConfig | None
            Optional per-operator runtime settings, such as an HTTP proxy.
        layers : Sequence[LayerConfig] | None
            Optional declarative layers, applied in sequence order.

        Returns
        -------
        AsyncOperator
            The new async operator.

        Examples
        --------
        .. code-block:: python

            import opendal
            from opendal.config import S3Config

            op = opendal.AsyncOperator.from_config(
                S3Config(scheme="s3", bucket="my-bucket")
            )
        """
    @classmethod
    def from_uri(cls, /, uri: str, **kwargs: str) -> AsyncOperator:
        """
        Create a new `AsyncOperator` from a URI string.

        The URI encodes the scheme and configuration in a single string, e.g.
        ``memory://`` or ``s3://bucket/path?region=us-east-1``. The scheme must
        belong to a service enabled in this build. Encode service options as
        query parameters; use ``urllib.parse.urlencode`` when building the URI
        dynamically.

        Parameters
        ----------
        uri : str
            The URI of the service, including any options as query parameters.
        **kwargs : dict
            Overrides for URI options. Prefer the URI query string.

        Returns
        -------
        AsyncOperator
            The new async operator.

        Examples
        --------
        .. code-block:: python

            from urllib.parse import urlencode
            import opendal

            op = opendal.AsyncOperator.from_uri("memory://")
            query = urlencode({"region": "us-east-1"})
            op = opendal.AsyncOperator.from_uri(
                f"s3://bucket/path?{query}"
            )
        """
    def layer(self, /, layer: Layer) -> AsyncOperator:
        """
        Add a new layer to the operator.

        Parameters
        ----------
        layer : Layer
            The layer to add.

        Returns
        -------
        AsyncOperator
            A new operator with the layer added.
        """
    def list(
        self,
        /,
        path: str | PathLike[str],
        *,
        limit: SupportsIndex | None = None,
        start_after: str | None = None,
        recursive: bool | None = None,
        versions: bool | None = None,
        deleted: bool | None = None,
    ) -> collections.abc.Awaitable[collections.abc.AsyncIterable[Entry]]:
        """
        List entries in the given directory.

        Parameters
        ----------
        path : str
            The path to the directory.
        limit : int, optional
            The maximum number of entries the service returns per request, as
            a hint. It does not cap the total: iteration continues across
            pages until every entry is returned. Some services, such as fs
            and memory, ignore it.
        start_after : str, optional
            The entry to start after.
        recursive : bool, optional
            Whether to list recursively.
        versions : bool, optional
            Whether to list versions.
        deleted : bool, optional
            Whether to list deleted entries.

        Returns
        -------
        coroutine
            An awaitable that returns an async iterator over the entries.
        """
    def open(
        self, /, path: str | PathLike[str], mode: str, **kwargs: Unpack[OpenKwargs]
    ) -> collections.abc.Awaitable[AsyncFile]:
        """
        Open an async file-like object for the given path.

        The returned file is an async context manager and reads or writes bytes.
        Use `read()` to supply response header overrides when reading.

        Parameters
        ----------
        path : str
            The path to the file.
        mode : str
            `"r"` to read, `"w"` to write (overwrite by default), `"a"` to append,
            or `"x"` to create a new file. An optional `"b"` can appear before or
            after the letter. All supported modes are binary; text modes such as
            `"rt"` raise `ValueError`. `AsyncFile` writes bytes, so encode text
            with `str.encode()` and decode reads with `bytes.decode()`.
        **kwargs : dict
            Additional options for the underlying reader or writer. In write and
            create modes, `checksum` selects the `ChecksumAlgorithm` the service
            uses to verify the full object before committing. Omit `checksum` or
            pass `None` to use the service default.

        Raises
        ------
        TypeError
            If `**kwargs` contains an unknown keyword argument, an option with a
            non-`None` value that does not apply to `mode`, or a value with an
            incompatible type.
        ValueError
            If the mode is invalid or unsupported, including text (`"t"`) and
            update (`"+"`) modes, or if the mode conflicts with `append` or
            `if_not_exists` in `**kwargs`, or if the checksum name is unrecognized.
        ConditionNotMatch
            If create mode (`"x"`) targets an existing file. The failure can
            surface from `write()` or `close()`.
        Unsupported
            If the service does not support the requested write options, including
            the selected checksum algorithm. Append writes cannot request a checksum.
        ChecksumMismatch
            If the checksum fails verification. The failure can surface from
            `write()` or `close()`.

        Returns
        -------
        coroutine
            An awaitable that returns a file-like object.
        """
    def presign_delete(
        self,
        /,
        path: str | PathLike[str],
        expire_second: SupportsIndex,
        *,
        version: str | None = None,
    ) -> collections.abc.Awaitable[PresignedRequest]:
        """
        Create a presigned request for a delete operation.

        Parameters
        ----------
        path : str
            The path of the object to delete.
        expire_second : int
            The number of seconds until the presigned URL expires.
        version : str, optional
            The version of the file to delete.

        Returns
        -------
        coroutine
            An awaitable that returns a presigned request object.
        """
    def presign_read(
        self,
        /,
        path: str | PathLike[str],
        expire_second: SupportsIndex,
        *,
        version: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
        if_unmodified_since: datetime | None = None,
        content_type: str | None = None,
        cache_control: str | None = None,
        content_disposition: str | None = None,
    ) -> collections.abc.Awaitable[PresignedRequest]:
        """
        Create a presigned request for a read operation.

        Parameters
        ----------
        path : str
            The path of the object to read.
        expire_second : int
            The number of seconds until the presigned URL expires.
        version : str, optional
            The version of the file.
        if_match : str, optional
            The ETag to match.
        if_none_match : str, optional
            The ETag to not match.
        if_modified_since : datetime, optional
            Only return if modified since this time.
        if_unmodified_since : datetime, optional
            Only return if unmodified since this time.
        content_type : str, optional
            Override the content type in the presigned response.
        cache_control : str, optional
            Override the cache control in the presigned response.
        content_disposition : str, optional
            Override the content disposition in the presigned response.

        Returns
        -------
        coroutine
            An awaitable that returns a presigned request object.
        """
    def presign_stat(
        self,
        /,
        path: str | PathLike[str],
        expire_second: SupportsIndex,
        *,
        version: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
        if_unmodified_since: datetime | None = None,
        content_type: str | None = None,
        cache_control: str | None = None,
        content_disposition: str | None = None,
    ) -> collections.abc.Awaitable[PresignedRequest]:
        """
        Create a presigned request for a stat operation.

        Parameters
        ----------
        path : str
            The path of the object to stat.
        expire_second : int
            The number of seconds until the presigned URL expires.
        version : str, optional
            The version of the file.
        if_match : str, optional
            The ETag to match.
        if_none_match : str, optional
            The ETag to not match.
        if_modified_since : datetime, optional
            Only return if modified since this time.
        if_unmodified_since : datetime, optional
            Only return if unmodified since this time.
        content_type : str, optional
            Override the content type in the presigned response.
        cache_control : str, optional
            Override the cache control in the presigned response.
        content_disposition : str, optional
            Override the content disposition in the presigned response.

        Returns
        -------
        coroutine
            An awaitable that returns a presigned request object.
        """
    def presign_write(
        self,
        /,
        path: str | PathLike[str],
        expire_second: SupportsIndex,
        *,
        content_type: str | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        cache_control: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        if_not_exists: bool | None = None,
        user_metadata: dict[str, str] | None = None,
    ) -> collections.abc.Awaitable[PresignedRequest]:
        """
        Create a presigned request for a write operation.

        Parameters
        ----------
        path : str
            The path of the object to write to.
        expire_second : int
            The number of seconds until the presigned URL expires.
        content_type : str, optional
            The content type header to set on the file.
        content_disposition : str, optional
            The content disposition header to set on the file.
        content_encoding : str, optional
            The content encoding header to set on the file.
        cache_control : str, optional
            The cache control header to set on the file.
        if_match : str, optional
            The ETag to match when writing the file.
        if_none_match : str, optional
            The ETag to not match when writing the file.
        if_not_exists : bool, optional
            Whether to fail if the file already exists.
        user_metadata : dict, optional
            The user metadata to set on the file.

        Returns
        -------
        coroutine
            An awaitable that returns a presigned request object.
        """
    def read(
        self,
        /,
        path: str | PathLike[str],
        *,
        version: str | None = None,
        concurrent: SupportsIndex | None = None,
        chunk: SupportsIndex | None = None,
        gap: SupportsIndex | None = None,
        offset: SupportsIndex | None = None,
        prefetch: SupportsIndex | None = None,
        size: SupportsIndex | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
        if_unmodified_since: datetime | None = None,
        content_type: str | None = None,
        cache_control: str | None = None,
        content_disposition: str | None = None,
    ) -> collections.abc.Awaitable[bytes]:
        """
        Read all or a selected range of bytes from a file.

        A positive `prefetch` cannot be combined with `content_type`,
        `cache_control`, or `content_disposition`. Combining them raises `ValueError`.

        Parameters
        ----------
        path : str
            The path to the file.
        version : str, optional
            The version of the file.
        concurrent : int, optional
            The number of concurrent readers.
        chunk : int, optional
            The size of each chunk.
        gap : int, optional
            The gap between each chunk.
        offset : int, optional
            The offset of the file.
        prefetch : int, optional
            The number of completed chunks to buffer during concurrent reads.
        size : int, optional
            The size of the file.
        if_match : str, optional
            The ETag of the file.
        if_none_match : str, optional
            The ETag of the file.
        if_modified_since : datetime, optional
            The last modified time of the file.
        if_unmodified_since : datetime, optional
            The last modified time of the file.
        content_type : str, optional
            The Content-Type response header override, where supported.
        cache_control : str, optional
            The Cache-Control response header override, where supported.
        content_disposition : str, optional
            The Content-Disposition response header override, where supported.

        Returns
        -------
        coroutine
            An awaitable that returns the contents of the file as bytes.
        """
    def remove_all(
        self, /, path: str | PathLike[str]
    ) -> collections.abc.Awaitable[None]:
        """
        Recursively remove all files and directories at the given path.

        Parameters
        ----------
        path : str
            The path to remove.

        Returns
        -------
        coroutine
            An awaitable that completes when the removal is finished.
        """
    def rename(
        self,
        /,
        source: str | PathLike[str],
        target: str | PathLike[str],
        *,
        if_not_exists: bool | None = None,
    ) -> collections.abc.Awaitable[None]:
        """
        Rename (move) a file from one path to another.

        Parameters
        ----------
        source : str
            The path to the source file.
        target : str
            The path to the target file.
        if_not_exists : bool, optional
            If True, require `rename_with_if_not_exists` and raise `Unsupported`
            if the service lacks it. Raise `ConditionNotMatch` if the target
            already exists, leaving both files unchanged. Defaults to False,
            which allows overwriting.

        Returns
        -------
        coroutine
            An awaitable that completes when the rename is finished.
        """
    def scan(
        self,
        /,
        path: str | PathLike[str],
        *,
        limit: SupportsIndex | None = None,
        start_after: str | None = None,
        versions: bool | None = None,
        deleted: bool | None = None,
    ) -> collections.abc.Awaitable[collections.abc.AsyncIterable[Entry]]:
        """
        Recursively list entries in the given directory.

        Notes
        -----
        This method is deprecated. Use `list()` with `recursive=True` instead.

        Parameters
        ----------
        path : str
            The path to the directory.
        limit : int, optional
            The maximum number of entries the service returns per request, as
            a hint. It does not cap the total: iteration continues across
            pages until every entry is returned. Some services, such as fs
            and memory, ignore it.
        start_after : str, optional
            The entry to start after.
        versions : bool, optional
            Whether to list versions.
        deleted : bool, optional
            Whether to list deleted entries.

        Returns
        -------
        coroutine
            An awaitable that returns an async iterator over the entries.
        """
    def stat(
        self,
        /,
        path: str | PathLike[str],
        *,
        version: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
        if_unmodified_since: datetime | None = None,
        content_type: str | None = None,
        cache_control: str | None = None,
        content_disposition: str | None = None,
        checksum: bool = False,
    ) -> collections.abc.Awaitable[Metadata]:
        """
        Get the metadata of a file at the given path.

        Parameters
        ----------
        path : str
            The path to the file.
        version : str, optional
            The version of the file.
        if_match : str, optional
            The ETag of the file.
        if_none_match : str, optional
            The ETag of the file.
        if_modified_since : datetime, optional
            The last modified time of the file.
        if_unmodified_since : datetime, optional
            The last modified time of the file.
        content_type : str, optional
            The content type of the file.
        cache_control : str, optional
            The cache control of the file.
        content_disposition : str, optional
            The content disposition of the file.
        checksum : bool, optional
            Request the service's full-object checksum when available.
            Defaults to False. See `Metadata.checksum` for the returned value.

        Returns
        -------
        coroutine
            An awaitable that returns the metadata of the file.
        """
    def to_operator(self, /) -> Operator:
        """
        Create a new blocking `Operator` from this async operator.

        Returns
        -------
        Operator
            The blocking operator.
        """
    def write(
        self,
        /,
        path: str | PathLike[str],
        bs: bytes,
        *,
        append: bool | None = None,
        chunk: SupportsIndex | None = None,
        concurrent: SupportsIndex | None = None,
        cache_control: str | None = None,
        content_type: str | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        if_not_exists: bool | None = None,
        user_metadata: dict[str, str] | None = None,
        checksum: ChecksumAlgorithm | None = None,
    ) -> collections.abc.Awaitable[None]:
        """
        Write bytes to a file at the given path.

        This function will create a file if it does not exist, and will
        overwrite its contents if it does.

        Parameters
        ----------
        path : str
            The path to the file.
        bs : bytes
            The contents to write to the file.
        append : bool, optional
            Whether to append to the file instead of overwriting it.
        chunk : int, optional
            The chunk size to use when writing the file.
        concurrent : int, optional
            The number of concurrent requests to make when writing the file.
        cache_control : str, optional
            The cache control header to set on the file.
        content_type : str, optional
            The content type header to set on the file.
        content_disposition : str, optional
            The content disposition header to set on the file.
        content_encoding : str, optional
            The content encoding header to set on the file.
        if_match : str, optional
            The ETag to match when writing the file.
        if_none_match : str, optional
            The ETag to not match when writing the file.
        if_not_exists : bool, optional
            Whether to fail if the file already exists.
        user_metadata : dict, optional
            The user metadata to set on the file.
        checksum : ChecksumAlgorithm | None, optional
            The algorithm the service uses to verify the full object before
            committing. Omit it or pass `None` to use the service default.

        Raises
        ------
        ValueError
            If the checksum name is unrecognized.
        Unsupported
            If the service cannot verify the write with the selected algorithm,
            including when appending.
        ChecksumMismatch
            If the checksum fails verification.

        Returns
        -------
        coroutine
            An awaitable that completes when the write is finished.
        """

@final
class Operator:
    """
    The blocking equivalent of `AsyncOperator`.

    `Operator` is the entry point for all blocking APIs.

    See Also
    --------
    AsyncOperator
    """

    def __new__(
        cls, /, scheme: str | Scheme, **kwargs: str | PathLike[str]
    ) -> Operator:
        """
        Create a new blocking `Operator`.

        Parameters
        ----------
        scheme : str | Scheme
            The scheme of the service.
        **kwargs : dict
            The options for the service. Path-like values are converted to strings.

        Returns
        -------
        Operator
            The new operator.
        """
    def __reduce__(self, /) -> Any: ...
    def capability(self, /) -> Capability:
        """
        Get all capabilities of this operator.

        Returns
        -------
        Capability
            The capability of the operator.
        """
    def check(self, /) -> None:
        """
        Check if the operator is able to work correctly.

        Raises
        ------
        Exception
            If the operator is not able to work correctly.
        """
    def copy(
        self,
        /,
        source: str | PathLike[str],
        target: str | PathLike[str],
        *,
        if_not_exists: bool | None = None,
    ) -> None:
        """
        Copy a file from one path to another.

        Parameters
        ----------
        source : str
            The path to the source file.
        target : str
            The path to the target file.
        if_not_exists : bool, optional
            If True, require `copy_with_if_not_exists` and raise `Unsupported`
            if the service lacks it. Raise `ConditionNotMatch` if the target
            already exists. Defaults to False, which allows overwriting.
        """
    def create_dir(self, /, path: str | PathLike[str]) -> None:
        """
        Create a directory at the given path.

        Notes
        -----
        To indicate that a path is a directory, it must end with a `/`.
        This operation is always recursive, like `mkdir -p`.

        Parameters
        ----------
        path : str
            The path to the directory.
        """
    def delete(
        self,
        /,
        path: str | PathLike[str],
        *,
        version: str | None = None,
        recursive: bool | None = None,
        if_match: str | None = None,
    ) -> None:
        """
        Delete a file at the given path.

        Notes
        -----
        This operation will not return an error if the path does not exist.

        Parameters
        ----------
        path : str
            The path to the file.
        version : str, optional
            The version of the file to delete. Only supported on version-aware backends.
        recursive : bool, optional
            If True, delete the path recursively.
            Only supported on backends that support recursive delete.
        if_match : str, optional
            If set, only delete when the existing object's ETag matches.
        """
    def delete_many(
        self, /, paths: collections.abc.Iterable[str | PathLike[str]]
    ) -> None:
        """
        Delete an explicit collection of paths with default delete options.

        Notes
        -----
        This method collects the iterable and converts its Python path objects
        before deletion starts, using memory proportional to the number of paths.
        Empty collections do nothing. Paths that do not exist are ignored.
        Storage or path-validation errors can leave some paths deleted; this
        operation is not atomic and does not guarantee a single remote request.
        Use `remove_all` to recursively delete a path prefix.

        Parameters
        ----------
        paths : Iterable[str | PathLike[str]]
            The paths to delete. A single path is not accepted.
        """
    def exists(self, /, path: str | PathLike[str]) -> bool:
        """
        Check if a path exists.

        Parameters
        ----------
        path : str
            The path to check.

        Returns
        -------
        bool
            True if the path exists, False otherwise.
        """
    @classmethod
    def from_config(
        cls,
        /,
        config: ServiceConfig,
        *,
        context: ContextConfig | None = None,
        layers: collections.abc.Sequence[LayerConfig] | None = None,
    ) -> Operator:
        """
        Create a new blocking `Operator` from typed configuration.

        A static type checker validates the service scheme and the options for
        the service, context, and layers. The binding converts non-string values
        such as booleans, integers, finite floats, durations, paths, and flat
        lists or tuples to the strings the registry consumes.
        Service config values such as bytes, mappings, and nested sequences
        raise ``Unsupported``.

        Construction options are stored when pickling the operator and may
        include credentials. Reapply layers added via ``.layer()`` after unpickling.

        Parameters
        ----------
        config : ServiceConfig
            A service configuration such as ``opendal.config.S3Config``.
        context : ContextConfig | None
            Optional per-operator runtime settings, such as an HTTP proxy.
        layers : Sequence[LayerConfig] | None
            Optional declarative layers, applied in sequence order.

        Returns
        -------
        Operator
            The new operator.

        Examples
        --------
        .. code-block:: python

            import opendal
            from opendal.config import S3Config

            op = opendal.Operator.from_config(
                S3Config(scheme="s3", bucket="my-bucket")
            )
        """
    @classmethod
    def from_uri(cls, /, uri: str, **kwargs: str) -> Operator:
        """
        Create a new blocking `Operator` from a URI string.

        The URI encodes the scheme and configuration in a single string, e.g.
        ``memory://`` or ``s3://bucket/path?region=us-east-1``. The scheme must
        belong to a service enabled in this build. Encode service options as
        query parameters; use ``urllib.parse.urlencode`` when building the URI
        dynamically.

        Parameters
        ----------
        uri : str
            The URI of the service, including any options as query parameters.
        **kwargs : dict
            Overrides for URI options. Prefer the URI query string.

        Returns
        -------
        Operator
            The new operator.

        Examples
        --------
        .. code-block:: python

            from urllib.parse import urlencode
            import opendal

            op = opendal.Operator.from_uri("memory://")
            query = urlencode({"region": "us-east-1"})
            op = opendal.Operator.from_uri(f"s3://bucket/path?{query}")
        """
    def layer(self, /, layer: Layer) -> Operator:
        """
        Add a new layer to this operator.

        Parameters
        ----------
        layer : Layer
            The layer to add.

        Returns
        -------
        Operator
            A new operator with the layer added.
        """
    def list(
        self,
        /,
        path: str | PathLike[str],
        *,
        limit: SupportsIndex | None = None,
        start_after: str | None = None,
        recursive: bool | None = None,
        versions: bool | None = None,
        deleted: bool | None = None,
    ) -> collections.abc.Iterable[Entry]:
        """
        List entries in the given directory.

        Parameters
        ----------
        path : str
            The path to the directory.
        limit : int, optional
            The maximum number of entries the service returns per request, as
            a hint. It does not cap the total: iteration continues across
            pages until every entry is returned. Some services, such as fs
            and memory, ignore it.
        start_after : str, optional
            The entry to start after.
        recursive : bool, optional
            Whether to list recursively.
        versions : bool, optional
            Whether to list versions.
        deleted : bool, optional
            Whether to list deleted entries.

        Returns
        -------
        BlockingLister
            An iterator over the entries in the directory.
        """
    def open(
        self, /, path: str | PathLike[str], mode: str, **kwargs: Unpack[OpenKwargs]
    ) -> File:
        """
        Open a file-like object for the given path.

        The returned file is a context manager and reads or writes bytes.
        Use `read()` to supply response header overrides when reading.

        Parameters
        ----------
        path : str
            The path to the file.
        mode : str
            `"r"` to read, `"w"` to write (overwrite by default), `"a"` to append,
            or `"x"` to create a new file. An optional `"b"` can appear before or
            after the letter. All supported modes are binary; text modes such as
            `"rt"` raise `ValueError`. Wrap the returned file with
            `io.TextIOWrapper` to read or write text.
        **kwargs
            Additional options for the underlying reader or writer. In write and
            create modes, `checksum` selects the `ChecksumAlgorithm` the service
            uses to verify the full object before committing. Omit `checksum` or
            pass `None` to use the service default.

        Raises
        ------
        TypeError
            If `**kwargs` contains an unknown keyword argument, an option with a
            non-`None` value that does not apply to `mode`, or a value with an
            incompatible type.
        ValueError
            If the mode is invalid or unsupported, including text (`"t"`) and
            update (`"+"`) modes, or if the mode conflicts with `append` or
            `if_not_exists` in `**kwargs`, or if the checksum name is unrecognized.
        ConditionNotMatch
            If create mode (`"x"`) targets an existing file. The failure can
            surface from `write()`, `flush()`, or `close()`.
        Unsupported
            If the service does not support the requested write options, including
            the selected checksum algorithm. Append writes cannot request a checksum.
        ChecksumMismatch
            If the checksum fails verification. The failure can surface from
            `write()`, `flush()`, or `close()`.

        Returns
        -------
        File
            A file-like object.
        """
    def read(
        self,
        /,
        path: str | PathLike[str],
        *,
        version: str | None = None,
        concurrent: SupportsIndex | None = None,
        chunk: SupportsIndex | None = None,
        gap: SupportsIndex | None = None,
        offset: SupportsIndex | None = None,
        prefetch: SupportsIndex | None = None,
        size: SupportsIndex | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
        if_unmodified_since: datetime | None = None,
        content_type: str | None = None,
        cache_control: str | None = None,
        content_disposition: str | None = None,
    ) -> bytes:
        """
        Read all or a selected range of bytes from a file.

        A positive `prefetch` cannot be combined with `content_type`,
        `cache_control`, or `content_disposition`. Combining them raises `ValueError`.

        Parameters
        ----------
        path : str
            The path to the file.
        version : str, optional
            The version of the file.
        concurrent : int, optional
            The number of concurrent readers.
        chunk : int, optional
            The size of each chunk.
        gap : int, optional
            The gap between each chunk.
        offset : int, optional
            The offset of the file.
        prefetch : int, optional
            The number of completed chunks to buffer during concurrent reads.
        size : int, optional
            The size of the file.
        if_match : str, optional
            The ETag of the file.
        if_none_match : str, optional
            The ETag of the file.
        if_modified_since : datetime, optional
            The last modified time of the file.
        if_unmodified_since : datetime, optional
            The last modified time of the file.
        content_type : str, optional
            The Content-Type response header override, where supported.
        cache_control : str, optional
            The Cache-Control response header override, where supported.
        content_disposition : str, optional
            The Content-Disposition response header override, where supported.

        Returns
        -------
        bytes
            The contents of the file as bytes.
        """
    def remove_all(self, /, path: str | PathLike[str]) -> None:
        """
        Recursively remove all files and directories at the given path.

        Parameters
        ----------
        path : str
            The path to remove.
        """
    def rename(
        self,
        /,
        source: str | PathLike[str],
        target: str | PathLike[str],
        *,
        if_not_exists: bool | None = None,
    ) -> None:
        """
        Rename (move) a file from one path to another.

        Parameters
        ----------
        source : str
            The path to the source file.
        target : str
            The path to the target file.
        if_not_exists : bool, optional
            If True, require `rename_with_if_not_exists` and raise `Unsupported`
            if the service lacks it. Raise `ConditionNotMatch` if the target
            already exists, leaving both files unchanged. Defaults to False,
            which allows overwriting.
        """
    def scan(
        self,
        /,
        path: str | PathLike[str],
        *,
        limit: SupportsIndex | None = None,
        start_after: str | None = None,
        versions: bool | None = None,
        deleted: bool | None = None,
    ) -> collections.abc.Iterable[Entry]:
        """
        Recursively list entries in the given directory.

        Notes
        -----
        This method is deprecated. Use `list()` with `recursive=True` instead.

        Parameters
        ----------
        path : str
            The path to the directory.
        limit : int, optional
            The maximum number of entries the service returns per request, as
            a hint. It does not cap the total: iteration continues across
            pages until every entry is returned. Some services, such as fs
            and memory, ignore it.
        start_after : str, optional
            The entry to start after.
        versions : bool, optional
            Whether to list versions.
        deleted : bool, optional
            Whether to list deleted entries.

        Returns
        -------
        BlockingLister
            An iterator over the entries in the directory.
        """
    def stat(
        self,
        /,
        path: str | PathLike[str],
        *,
        version: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
        if_unmodified_since: datetime | None = None,
        content_type: str | None = None,
        cache_control: str | None = None,
        content_disposition: str | None = None,
        checksum: bool = False,
    ) -> Metadata:
        """
        Get the metadata of a file at the given path.

        Parameters
        ----------
        path : str
            The path to the file.
        version : str, optional
            The version of the file.
        if_match : str, optional
            The ETag of the file.
        if_none_match : str, optional
            The ETag of the file.
        if_modified_since : datetime, optional
            The last modified time of the file.
        if_unmodified_since : datetime, optional
            The last modified time of the file.
        content_type : str, optional
            The content type of the file.
        cache_control : str, optional
            The cache control of the file.
        content_disposition : str, optional
            The content disposition of the file.
        checksum : bool, optional
            Request the service's full-object checksum when available.
            Defaults to False. See `Metadata.checksum` for the returned value.

        Returns
        -------
        Metadata
            The metadata of the file.
        """
    def to_async_operator(self, /) -> AsyncOperator:
        """
        Create a new `AsyncOperator` from this blocking operator.

        Returns
        -------
        AsyncOperator
            The async operator.
        """
    def write(
        self,
        /,
        path: str | PathLike[str],
        bs: bytes,
        *,
        append: bool | None = None,
        chunk: SupportsIndex | None = None,
        concurrent: SupportsIndex | None = None,
        cache_control: str | None = None,
        content_type: str | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        if_not_exists: bool | None = None,
        user_metadata: dict[str, str] | None = None,
        checksum: ChecksumAlgorithm | None = None,
    ) -> None:
        """
        Write bytes to a file at the given path.

        This function will create a file if it does not exist, and will
        overwrite its contents if it does.

        Parameters
        ----------
        path : str
            The path to the file.
        bs : bytes
            The contents to write to the file.
        append : bool, optional
            Whether to append to the file instead of overwriting it.
        chunk : int, optional
            The chunk size to use when writing the file.
        concurrent : int, optional
            The number of concurrent requests to make when writing the file.
        cache_control : str, optional
            The cache control header to set on the file.
        content_type : str, optional
            The content type header to set on the file.
        content_disposition : str, optional
            The content disposition header to set on the file.
        content_encoding : str, optional
            The content encoding header to set on the file.
        if_match : str, optional
            The ETag to match when writing the file.
        if_none_match : str, optional
            The ETag to not match when writing the file.
        if_not_exists : bool, optional
            Whether to fail if the file already exists.
        user_metadata : dict, optional
            The user metadata to set on the file.
        checksum : ChecksumAlgorithm | None, optional
            The algorithm the service uses to verify the full object before
            committing. Omit it or pass `None` to use the service default.

        Raises
        ------
        ValueError
            If the checksum name is unrecognized.
        Unsupported
            If the service cannot verify the write with the selected algorithm,
            including when appending.
        ChecksumMismatch
            If the checksum fails verification.
        """
