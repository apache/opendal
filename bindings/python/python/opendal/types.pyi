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

from datetime import datetime
from typing import Final, final

__all__ = ["Entry", "EntryMode", "Metadata", "PresignedRequest"]

@final
class Entry:
    """
    Entry.

    An entry representing a path and its associated metadata.

    Notes
    -----
    If this entry is a directory, ``path`` **must** end with ``/``.
    Otherwise, ``path`` **must not** end with ``/``.
    """

    @property
    def metadata(self, /) -> Metadata:
        """The metadata of this entry."""
    @property
    def name(self, /) -> str:
        """The name of entry, representing the last segment of the path."""
    @property
    def path(self, /) -> str:
        """The path of entry relative to the operator's root."""

@final
class EntryMode:
    """
    EntryMode.

    The mode of an entry, indicating if it is a file or a directory.
    """

    Dir: Final[EntryMode]
    """
    The entry is a directory and can be listed.
    """
    File: Final[EntryMode]
    """
    The entry is a file and has data to read.
    """
    Unknown: Final[EntryMode]
    """
    The mode of the entry is unknown.
    """
    def __eq__(self, value: object, /) -> bool: ...
    def __hash__(self, /) -> int: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...
    def is_dir(self, /) -> bool:
        """
        Check if the entry mode is `Dir`.

        Returns
        -------
        bool
            True if the entry is a directory.
        """
    def is_file(self, /) -> bool:
        """
        Check if the entry mode is `File`.

        Returns
        -------
        bool
            True if the entry is a file.
        """

@final
class Metadata:
    """
    The metadata of an ``Entry``.

    The metadata is always tied to a specific context and is not a global
    state. For example, two versions of the same path might have different
    content lengths.

    Notes
    -----
    In systems that support versioning, such as AWS S3, the metadata may
    represent a specific version of a file. Use :attr:`version` to get
    the version of a file if it is available.
    """

    @property
    def cache_control(self, /) -> str | None:
        """Return this entry's Cache-Control value."""
    @property
    def content_disposition(self, /) -> str | None:
        """Return this entry's Content-Disposition value."""
    @property
    def content_encoding(self, /) -> str | None:
        """Return this entry's Content-Encoding value."""
    @property
    def content_language(self, /) -> str | None:
        """Return this entry's Content-Language value, if available."""
    @property
    def content_length(self, /) -> int:
        """Return the full content length of this entry."""
    @property
    def content_md5(self, /) -> str | None:
        """Return this entry's Content-MD5 value."""
    @property
    def content_type(self, /) -> str | None:
        """Return this entry's Content-Type value."""
    @property
    def etag(self, /) -> str | None:
        """Return this entry's service-provided ETag as-is."""
    @property
    def is_current(self, /) -> bool | None:
        """Return whether this metadata describes the current object version."""
    @property
    def is_deleted(self, /) -> bool:
        """Return whether this metadata describes a deleted object or delete marker."""
    @property
    def is_dir(self, /) -> bool:
        """Whether this entry is a directory."""
    @property
    def is_file(self, /) -> bool:
        """Whether this entry is a file."""
    @property
    def last_modified(self, /) -> datetime | None:
        """Return this entry's Last-Modified timestamp."""
    @property
    def mode(self, /) -> EntryMode:
        """Return this entry's mode."""
    @property
    def user_metadata(self, /) -> dict[str, str] | None:
        """Return a copy of this entry's user metadata as a dict."""
    @property
    def version(self, /) -> str | None:
        """Return this entry's service-specific version identifier."""

@final
class PresignedRequest:
    """
    A presigned request.

    This contains the information required to make a request to the
    underlying service, including the URL, method, and headers.
    """

    @property
    def headers(self, /) -> dict[str, str]:
        """
        The HTTP headers of this request.

        Returns
        -------
        dict
            The HTTP headers of this request.
        """
    @property
    def method(self, /) -> str:
        """The HTTP method of this request."""
    @property
    def url(self, /) -> str:
        """The URL of this request."""
