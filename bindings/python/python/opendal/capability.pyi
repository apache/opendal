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

from typing import final

__all__ = ["Capability"]

@final
class Capability:
    """
    Capability defines the supported operations and their constraints for an Operator.

    This structure provides a comprehensive description of an Operator's
    capabilities, including:

    - Basic operations support (read, write, delete, etc.)
    - Advanced operation variants (conditional operations, metadata handling)
    - Operational constraints (size limits, batch limitations)

    These flags describe the underlying OpenDAL operator. A flag can be true even
    when the Python binding does not expose the matching method (for example,
    `restore`).
    """

    @property
    def compose(self, /) -> bool:
        """Indicates if the operator supports composing complete source objects."""
    @property
    def compose_with_cache_control(self, /) -> bool:
        """Indicates if Cache-Control can be specified during composition."""
    @property
    def compose_with_content_disposition(self, /) -> bool:
        """Indicates if Content-Disposition can be specified during composition."""
    @property
    def compose_with_content_encoding(self, /) -> bool:
        """Indicates if Content-Encoding can be specified during composition."""
    @property
    def compose_with_content_type(self, /) -> bool:
        """Indicates if Content-Type can be specified during composition."""
    @property
    def compose_with_if_match(self, /) -> bool:
        """Indicates if composition supports a destination ETag match condition."""
    @property
    def compose_with_if_none_match(self, /) -> bool:
        """Indicates if composition supports a destination ETag non-match condition."""
    @property
    def compose_with_if_not_exists(self, /) -> bool:
        """Indicates if composition can require a missing destination."""
    @property
    def compose_with_if_version_match(self, /) -> bool:
        """Indicates if composition supports a destination version match condition."""
    @property
    def compose_with_if_version_not_match(self, /) -> bool:
        """Indicates if composition supports a destination version non-match condition."""
    @property
    def compose_with_source_if_match(self, /) -> bool:
        """Indicates if composition supports a source ETag match condition."""
    @property
    def compose_with_source_version(self, /) -> bool:
        """Indicates if composition can select a specific source version."""
    @property
    def compose_with_user_metadata(self, /) -> bool:
        """Indicates if custom user metadata can be attached during composition."""
    @property
    def copy(self, /) -> bool:
        """Indicates if copy operations are supported."""
    @property
    def copy_can_multi(self, /) -> bool:
        """Indicates if copy operations can be split into multiple server-side tasks."""
    @property
    def copy_multi_max_size(self, /) -> int | None:
        """Maximum size supported for segmented copy tasks."""
    @property
    def copy_multi_min_size(self, /) -> int | None:
        """Minimum size required for segmented copy tasks."""
    @property
    def copy_with_content_language(self, /) -> bool:
        """Indicates if copy operations can replace the destination's Content-Language."""
    @property
    def copy_with_content_type(self, /) -> bool:
        """Indicates if copy operations can replace the destination's Content-Type."""
    @property
    def copy_with_if_match(self, /) -> bool:
        """Indicates if conditional copy operations with if-match are supported."""
    @property
    def copy_with_if_none_match(self, /) -> bool:
        """Indicates if conditional copy operations with if-none-match are supported."""
    @property
    def copy_with_if_not_exists(self, /) -> bool:
        """Indicates if conditional copy operations with if-not-exists are supported."""
    @property
    def copy_with_if_version_match(self, /) -> bool:
        """Indicates if conditional copy operations using version match are supported."""
    @property
    def copy_with_if_version_not_match(self, /) -> bool:
        """Indicates if conditional copy operations using version non-match are supported."""
    @property
    def copy_with_source_version(self, /) -> bool:
        """Indicates if copy operations from a specific source version are supported."""
    @property
    def copy_with_user_metadata(self, /) -> bool:
        """Indicates if copy operations can replace the destination's user metadata."""
    @property
    def create_dir(self, /) -> bool:
        """Indicates if directory creation is supported."""
    @property
    def delete(self, /) -> bool:
        """Indicates if delete operations are supported."""
    @property
    def delete_max_size(self, /) -> int | None:
        """Maximum size supported for single delete operations."""
    @property
    def delete_with_if_match(self, /) -> bool:
        """Indicates if conditional delete operations using If-Match are supported."""
    @property
    def delete_with_if_none_match(self, /) -> bool:
        """Indicates if conditional delete operations using If-None-Match are supported."""
    @property
    def delete_with_if_version_match(self, /) -> bool:
        """Indicates if conditional delete operations using version match are supported."""
    @property
    def delete_with_if_version_not_match(self, /) -> bool:
        """Indicates if conditional delete operations using version non-match are supported."""
    @property
    def delete_with_recursive(self, /) -> bool:
        """Indicates if recursive delete operations are supported."""
    @property
    def delete_with_version(self, /) -> bool:
        """Indicates if versions delete operations are supported."""
    @property
    def list(self, /) -> bool:
        """Indicates if list operations are supported."""
    @property
    def list_with_deleted(self, /) -> bool:
        """Indicates if listing with deleted files included is supported."""
    @property
    def list_with_glob(self, /) -> bool:
        """Indicates if listing with a glob pattern is supported by this service stack."""
    @property
    def list_with_limit(self, /) -> bool:
        """Indicates if list operations support result limiting."""
    @property
    def list_with_recursive(self, /) -> bool:
        """Indicates if recursive listing is supported."""
    @property
    def list_with_start_after(self, /) -> bool:
        """Indicates if list operations support continuation from a specific point."""
    @property
    def list_with_versions(self, /) -> bool:
        """Indicates if listing with versions included is supported."""
    @property
    def presign(self, /) -> bool:
        """Indicates if presigned URL generation is supported."""
    @property
    def presign_delete(self, /) -> bool:
        """Indicates if presigned URLs for delete operations are supported."""
    @property
    def presign_read(self, /) -> bool:
        """Indicates if presigned URLs for read operations are supported."""
    @property
    def presign_stat(self, /) -> bool:
        """Indicates if presigned URLs for stat operations are supported."""
    @property
    def presign_write(self, /) -> bool:
        """Indicates if presigned URLs for write operations are supported."""
    @property
    def read(self, /) -> bool:
        """Indicates if the operator supports read operations."""
    @property
    def read_with_checksum(self, /) -> bool:
        """Indicates if the operator supports requesting stored full-file checksum metadata. Stat and read responses may include that metadata when requested."""
    @property
    def read_with_if_match(self, /) -> bool:
        """Indicates if conditional read operations using If-Match are supported."""
    @property
    def read_with_if_modified_since(self, /) -> bool:
        """Indicates if conditional read operations using If-Modified-Since are supported."""
    @property
    def read_with_if_none_match(self, /) -> bool:
        """Indicates if conditional read operations using If-None-Match are supported."""
    @property
    def read_with_if_unmodified_since(self, /) -> bool:
        """Indicates if conditional read operations using If-Unmodified-Since are supported."""
    @property
    def read_with_if_version_match(self, /) -> bool:
        """Indicates if conditional read operations using version match are supported."""
    @property
    def read_with_if_version_not_match(self, /) -> bool:
        """Indicates if conditional read operations using version non-match are supported."""
    @property
    def read_with_override_cache_control(self, /) -> bool:
        """Indicates if Cache-Control header override is supported during read operations."""
    @property
    def read_with_override_content_disposition(self, /) -> bool:
        """Indicates if Content-Disposition header override is supported during read operations."""
    @property
    def read_with_override_content_type(self, /) -> bool:
        """Indicates if Content-Type header override is supported during read operations."""
    @property
    def read_with_suffix(self, /) -> bool:
        """Indicates if suffix read operations are supported."""
    @property
    def read_with_version(self, /) -> bool:
        """Indicates if versions read operations are supported."""
    @property
    def rename(self, /) -> bool:
        """Indicates if rename operations are supported."""
    @property
    def rename_with_if_not_exists(self, /) -> bool:
        """Indicates if conditional rename operations with if-not-exists are supported."""
    @property
    def restore(self, /) -> bool:
        """Indicates if restore operations are supported."""
    @property
    def restore_with_if_not_exists(self, /) -> bool:
        """Indicates if conditional restore operations using if-not-exists are supported."""
    @property
    def restore_with_version(self, /) -> bool:
        """Indicates if restoring a specific version is supported."""
    @property
    def shared(self, /) -> bool:
        """Indicate if the operator supports shared access."""
    @property
    def stat(self, /) -> bool:
        """Indicates if the operator supports metadata retrieval operations."""
    @property
    def stat_with_if_match(self, /) -> bool:
        """Indicates if conditional stat operations using If-Match are supported."""
    @property
    def stat_with_if_modified_since(self, /) -> bool:
        """Indicates if conditional stat operations using If-Modified-Since are supported."""
    @property
    def stat_with_if_none_match(self, /) -> bool:
        """Indicates if conditional stat operations using If-None-Match are supported."""
    @property
    def stat_with_if_unmodified_since(self, /) -> bool:
        """Indicates if conditional stat operations using If-Unmodified-Since are supported."""
    @property
    def stat_with_if_version_match(self, /) -> bool:
        """Indicates if conditional stat operations using version match are supported."""
    @property
    def stat_with_if_version_not_match(self, /) -> bool:
        """Indicates if conditional stat operations using version non-match are supported."""
    @property
    def stat_with_override_cache_control(self, /) -> bool:
        """Indicates if Cache-Control header override is supported during stat operations."""
    @property
    def stat_with_override_content_disposition(self, /) -> bool:
        """Indicates if Content-Disposition header override is supported during stat operations."""
    @property
    def stat_with_override_content_type(self, /) -> bool:
        """Indicates if Content-Type header override is supported during stat operations."""
    @property
    def stat_with_version(self, /) -> bool:
        """Indicates if versions stat operations are supported."""
    @property
    def write(self, /) -> bool:
        """Indicates if the operator supports write operations."""
    @property
    def write_can_append(self, /) -> bool:
        """Indicates if append operations are supported."""
    @property
    def write_can_copy_from(self, /) -> bool:
        """Indicates if an open, non-append writer can copy byte ranges from existing objects without transferring the copied bytes through the client. Whole-object copy is a separate capability."""
    @property
    def write_can_empty(self, /) -> bool:
        """Indicates if writing empty content is supported."""
    @property
    def write_can_multi(self, /) -> bool:
        """Indicates if multiple write operations can be performed on the same object."""
    @property
    def write_multi_max_size(self, /) -> int | None:
        """Maximum size supported for multipart uploads. For example, AWS S3 supports up to 5GiB per part in multipart uploads."""
    @property
    def write_multi_min_size(self, /) -> int | None:
        """Minimum size required for multipart uploads (except for the last part). For example, AWS S3 requires at least 5MiB per part."""
    @property
    def write_total_max_size(self, /) -> int | None:
        """Maximum total size supported for write operations. For example, Cloudflare D1 has a 1MB total size limit."""
    @property
    def write_with_cache_control(self, /) -> bool:
        """Indicates if Cache-Control can be specified during write operations."""
    @property
    def write_with_checksum(self, /) -> bool:
        """Indicates if non-append writes support native verification with a checksum algorithm selected by the service. Verification may cover only individual upload requests, parts, or blocks. Support for caller-selected algorithms and full-object verification depends on the service and write mode."""
    @property
    def write_with_content_disposition(self, /) -> bool:
        """Indicates if Content-Disposition can be specified during write operations."""
    @property
    def write_with_content_encoding(self, /) -> bool:
        """Indicates if Content-Encoding can be specified during write operations."""
    @property
    def write_with_content_language(self, /) -> bool:
        """Indicates if a write can store Content-Language and a subsequent stat can retrieve it."""
    @property
    def write_with_content_type(self, /) -> bool:
        """Indicates if Content-Type can be specified during write operations."""
    @property
    def write_with_expires(self, /) -> bool:
        """Indicates if writes can set a lifetime for the new file."""
    @property
    def write_with_if_match(self, /) -> bool:
        """Indicates if conditional write operations using If-Match are supported."""
    @property
    def write_with_if_none_match(self, /) -> bool:
        """Indicates if conditional write operations using If-None-Match are supported."""
    @property
    def write_with_if_not_exists(self, /) -> bool:
        """Indicates if write operations can be conditional on object non-existence."""
    @property
    def write_with_if_version_match(self, /) -> bool:
        """Indicates if conditional write operations using version match are supported."""
    @property
    def write_with_if_version_not_match(self, /) -> bool:
        """Indicates if conditional write operations using version non-match are supported."""
    @property
    def write_with_user_metadata(self, /) -> bool:
        """Indicates if custom user metadata can be attached during write operations."""
