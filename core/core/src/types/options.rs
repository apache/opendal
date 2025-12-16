// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Options module provides options definitions for operations.

use crate::raw::{BytesRange, Timestamp};
use std::collections::HashMap;

/// Options for delete operations.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct DeleteOptions {
    /// The version of the file to delete.
    pub version: Option<String>,
    /// Whether to delete the target recursively.
    ///
    /// - If `false`, behaves like the traditional single-object delete.
    /// - If `true`, all entries under the path (or sharing the prefix for file-like paths)
    ///   will be removed.
    pub recursive: bool,
}

/// Options for list operations.
///
/// # Groups
/// - Traversal: `recursive`.
/// - Pagination: `limit`, `start_after`.
/// - Versioning: `versions`, `deleted` (effective on version-aware backends).

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ListOptions {
    /// Maximum results per request (backend hint) to control memory and throttling.
    pub limit: Option<usize>,
    /// The start_after passes to underlying service to specify the specified key
    /// to start listing from.
    pub start_after: Option<String>,
    /// Whether to list recursively under the prefix; default `false`.
    pub recursive: bool,
    /// Include object versions when supported by the backend; default `false`.
    pub versions: bool,
    /// Include delete markers when supported by version-aware backends; default `false`.
    pub deleted: bool,
}

/// Options for read operations.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ReadOptions {
    /// Set `range` for this operation.
    ///
    /// If we have a file with size `n`.
    ///
    /// - `..` means read bytes in range `[0, n)` of file.
    /// - `0..1024` and `..1024` means read bytes in range `[0, 1024)` of file
    /// - `1024..` means read bytes in range `[1024, n)` of file
    ///
    /// The type implements `From<RangeBounds<u64>>`, so users can use `(1024..).into()` instead.
    pub range: BytesRange,
    /// Set `version` for this operation.
    ///
    /// This option can be used to retrieve the data of a specified version of the given path.
    ///
    /// If the version doesn't exist, an error with kind [`ErrorKind::NotFound`] will be returned.
    pub version: Option<String>,

    /// Set `if_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` matches the given `ETag`.
    ///
    /// If file exists and it's etag doesn't match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    pub if_match: Option<String>,
    /// Set `if_none_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` doesn't match the given `ETag`.
    ///
    /// If file exists and it's etag match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    pub if_none_match: Option<String>,
    /// Set `if_modified_since` for this operation.
    ///
    /// This option can be used to check if the file has been modified since the given timestamp.
    ///
    /// If file exists and it hasn't been modified since the specified time, an error with kind
    /// [`ErrorKind::ConditionNotMatch`] will be returned.
    pub if_modified_since: Option<Timestamp>,
    /// Set `if_unmodified_since` for this operation.
    ///
    /// This feature can be used to check if the file hasn't been modified since the given timestamp.
    ///
    /// If file exists and it has been modified since the specified time, an error with kind
    /// [`ErrorKind::ConditionNotMatch`] will be returned.
    pub if_unmodified_since: Option<Timestamp>,

    /// Set `concurrent` for the operation.
    ///
    /// OpenDAL by default to read file without concurrent. This is not efficient for cases when users
    /// read large chunks of data. By setting `concurrent`, opendal will reading files concurrently
    /// on support storage services.
    ///
    /// By setting `concurrent`, opendal will fetch chunks concurrently with
    /// the give chunk size.
    ///
    /// Refer to [`crate::docs::performance`] for more details.
    pub concurrent: usize,
    /// Set `chunk` for the operation.
    ///
    /// OpenDAL will use services' preferred chunk size by default. Users can set chunk based on their own needs.
    ///
    /// Refer to [`crate::docs::performance`] for more details.
    pub chunk: Option<usize>,
    /// Controls the optimization strategy for range reads in [`Reader::fetch`].
    ///
    /// When performing range reads, if the gap between two requested ranges is smaller than
    /// the configured `gap` size, OpenDAL will merge these ranges into a single read request
    /// and discard the unrequested data in between. This helps reduce the number of API calls
    /// to remote storage services.
    ///
    /// This optimization is particularly useful when performing multiple small range reads
    /// that are close to each other, as it reduces the overhead of multiple network requests
    /// at the cost of transferring some additional data.
    ///
    /// Refer to [`crate::docs::performance`] for more details.
    pub gap: Option<usize>,

    /// Specify the content-type header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_content_type: Option<String>,
    /// Specify the `cache-control` header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_cache_control: Option<String>,
    /// Specify the `content-disposition` header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_content_disposition: Option<String>,
}

/// Options for reader operations.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ReaderOptions {
    /// Set `version` for this operation.
    ///
    /// This option can be used to retrieve the data of a specified version of the given path.
    ///
    /// If the version doesn't exist, an error with kind [`ErrorKind::NotFound`] will be returned.
    pub version: Option<String>,

    /// Set `if_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` matches the given `ETag`.
    ///
    /// If file exists and it's etag doesn't match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    pub if_match: Option<String>,
    /// Set `if_none_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` doesn't match the given `ETag`.
    ///
    /// If file exists and it's etag match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    pub if_none_match: Option<String>,
    /// Set `if_modified_since` for this operation.
    ///
    /// This option can be used to check if the file has been modified since the given timestamp.
    ///
    /// If file exists and it hasn't been modified since the specified time, an error with kind
    /// [`ErrorKind::ConditionNotMatch`] will be returned.
    pub if_modified_since: Option<Timestamp>,
    /// Set `if_unmodified_since` for this operation.
    ///
    /// This feature can be used to check if the file hasn't been modified since the given timestamp.
    ///
    /// If file exists and it has been modified since the specified time, an error with kind
    /// [`ErrorKind::ConditionNotMatch`] will be returned.
    pub if_unmodified_since: Option<Timestamp>,

    /// Set `concurrent` for the operation.
    ///
    /// OpenDAL by default to read file without concurrent. This is not efficient for cases when users
    /// read large chunks of data. By setting `concurrent`, opendal will reading files concurrently
    /// on support storage services.
    ///
    /// By setting `concurrent`, opendal will fetch chunks concurrently with
    /// the give chunk size.
    ///
    /// Refer to [`crate::docs::performance`] for more details.
    pub concurrent: usize,
    /// Set `chunk` for the operation.
    ///
    /// OpenDAL will use services' preferred chunk size by default. Users can set chunk based on their own needs.
    ///
    /// Refer to [`crate::docs::performance`] for more details.
    pub chunk: Option<usize>,
    /// Controls the optimization strategy for range reads in [`Reader::fetch`].
    ///
    /// When performing range reads, if the gap between two requested ranges is smaller than
    /// the configured `gap` size, OpenDAL will merge these ranges into a single read request
    /// and discard the unrequested data in between. This helps reduce the number of API calls
    /// to remote storage services.
    ///
    /// This optimization is particularly useful when performing multiple small range reads
    /// that are close to each other, as it reduces the overhead of multiple network requests
    /// at the cost of transferring some additional data.
    ///
    /// Refer to [`crate::docs::performance`] for more details.
    pub gap: Option<usize>,
    /// Controls the number of prefetched bytes ranges that can be buffered in memory
    /// during concurrent reading.
    ///
    /// When performing concurrent reads with `Reader`, this option limits how many
    /// completed-but-not-yet-read chunks can be buffered. Once the number of buffered
    /// chunks reaches this limit, no new read tasks will be spawned until some of the
    /// buffered chunks are consumed.
    ///
    /// - Default value: 0 (no prefetching, strict back-pressure control)
    /// - Set to a higher value to allow more aggressive prefetching at the cost of memory
    ///
    /// This option helps prevent memory exhaustion when reading large files with high
    /// concurrency settings.
    pub prefetch: usize,
}

/// Options for stat operations.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct StatOptions {
    /// Set `version` for this operation.
    ///
    /// This options can be used to retrieve the data of a specified version of the given path.
    ///
    /// If the version doesn't exist, an error with kind [`ErrorKind::NotFound`] will be returned.
    pub version: Option<String>,

    /// Set `if_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` matches the given `ETag`.
    ///
    /// If file exists and it's etag doesn't match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    pub if_match: Option<String>,
    /// Set `if_none_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` doesn't match the given `ETag`.
    ///
    /// If file exists and it's etag match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    pub if_none_match: Option<String>,
    /// Set `if_modified_since` for this operation.
    ///
    /// This option can be used to check if the file has been modified since the given timestamp.
    ///
    /// If file exists and it hasn't been modified since the specified time, an error with kind
    /// [`ErrorKind::ConditionNotMatch`] will be returned.
    pub if_modified_since: Option<Timestamp>,
    /// Set `if_unmodified_since` for this operation.
    ///
    /// This feature can be used to check if the file hasn't been modified since the given timestamp.
    ///
    /// If file exists and it has been modified since the specified time, an error with kind
    /// [`ErrorKind::ConditionNotMatch`] will be returned.
    pub if_unmodified_since: Option<Timestamp>,

    /// Specify the content-type header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_content_type: Option<String>,
    /// Specify the `cache-control` header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_cache_control: Option<String>,
    /// Specify the `content-disposition` header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_content_disposition: Option<String>,
}

/// Options for write operations.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct WriteOptions {
    /// Sets append mode for this operation.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_can_append`] before using this option.
    ///
    /// ### Behavior
    ///
    /// - By default, write operations overwrite existing files
    /// - When append is set to true:
    ///   - New data will be appended to the end of existing file
    ///   - If file doesn't exist, it will be created
    /// - If not supported, will return an error
    ///
    /// This operation allows adding data to existing files instead of overwriting them.
    pub append: bool,

    /// Sets Cache-Control header for this write operation.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_cache_control`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Cache-Control as system metadata on the target file
    /// - The value should follow HTTP Cache-Control header format
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows controlling caching behavior for the written content.
    ///
    /// ### Use Cases
    ///
    /// - Setting browser cache duration
    /// - Configuring CDN behavior
    /// - Optimizing content delivery
    /// - Managing cache invalidation
    ///
    /// ### References
    ///
    /// - [MDN Cache-Control](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control)
    /// - [RFC 7234 Section 5.2](https://tools.ietf.org/html/rfc7234#section-5.2)
    pub cache_control: Option<String>,
    /// Sets `Content-Type` header for this write operation.
    ///
    /// ## Capability
    ///
    /// Check [`Capability::write_with_content_type`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Content-Type as system metadata on the target file
    /// - The value should follow MIME type format (e.g. "text/plain", "image/jpeg")
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows specifying the media type of the content being written.
    pub content_type: Option<String>,
    /// Sets Content-Disposition header for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_content_disposition`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Content-Disposition as system metadata on the target file
    /// - The value should follow HTTP Content-Disposition header format
    /// - Common values include:
    ///   - `inline` - Content displayed within browser
    ///   - `attachment` - Content downloaded as file
    ///   - `attachment; filename="example.jpg"` - Downloaded with specified filename
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows controlling how the content should be displayed or downloaded.
    pub content_disposition: Option<String>,
    /// Sets Content-Encoding header for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_content_encoding`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Content-Encoding as system metadata on the target file
    /// - The value should follow HTTP Content-Encoding header format
    /// - Common values include:
    ///   - `gzip` - Content encoded using gzip compression
    ///   - `deflate` - Content encoded using deflate compression
    ///   - `br` - Content encoded using Brotli compression
    ///   - `identity` - No encoding applied (default value)
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows specifying the encoding applied to the content being written.
    pub content_encoding: Option<String>,
    /// Sets user metadata for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_user_metadata`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, the user metadata will be attached to the object during write
    /// - Accepts key-value pairs where both key and value are strings
    /// - Keys are case-insensitive in most services
    /// - Services may have limitations for user metadata, for example:
    ///   - Key length is typically limited (e.g., 1024 bytes)
    ///   - Value length is typically limited (e.g., 4096 bytes)
    ///   - Total metadata size might be limited
    ///   - Some characters might be forbidden in keys
    /// - If not supported, the metadata will be ignored
    ///
    /// User metadata provides a way to attach custom metadata to objects during write operations.
    /// This metadata can be retrieved later when reading the object.
    pub user_metadata: Option<HashMap<String, String>>,

    /// Sets If-Match header for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_if_match`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, the write operation will only succeed if the target's ETag matches the specified value
    /// - The value should be a valid ETag string
    /// - Common values include:
    ///   - A specific ETag value like `"686897696a7c876b7e"`
    ///   - `*` - Matches any existing resource
    /// - If not supported, the value will be ignored
    ///
    /// This operation provides conditional write functionality based on ETag matching,
    /// helping prevent unintended overwrites in concurrent scenarios.
    pub if_match: Option<String>,
    /// Sets If-None-Match header for this write request.
    ///
    /// Note: Certain services, like `s3`, support `if_not_exists` but not `if_none_match`.
    /// Use `if_not_exists` if you only want to check whether a file exists.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_if_none_match`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, the write operation will only succeed if the target's ETag does not match the specified value
    /// - The value should be a valid ETag string
    /// - Common values include:
    ///   - A specific ETag value like `"686897696a7c876b7e"`
    ///   - `*` - Matches if the resource does not exist
    /// - If not supported, the value will be ignored
    ///
    /// This operation provides conditional write functionality based on ETag non-matching,
    /// useful for preventing overwriting existing resources or ensuring unique writes.
    pub if_none_match: Option<String>,
    /// Sets the condition that write operation will succeed only if target does not exist.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_if_not_exists`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, the write operation will only succeed if the target path does not exist
    /// - Will return error if target already exists
    /// - If not supported, the value will be ignored
    ///
    /// This operation provides a way to ensure write operations only create new resources
    /// without overwriting existing ones, useful for implementing "create if not exists" logic.
    pub if_not_exists: bool,

    /// Sets concurrent write operations for this writer.
    ///
    /// ## Behavior
    ///
    /// - By default, OpenDAL writes files sequentially
    /// - When concurrent is set:
    ///   - Multiple write operations can execute in parallel
    ///   - Write operations return immediately without waiting if tasks space are available
    ///   - Close operation ensures all writes complete in order
    ///   - Memory usage increases with concurrency level
    /// - If not supported, falls back to sequential writes
    ///
    /// This feature significantly improves performance when:
    /// - Writing large files
    /// - Network latency is high
    /// - Storage service supports concurrent uploads like multipart uploads
    ///
    /// ## Performance Impact
    ///
    /// Setting appropriate concurrency can:
    /// - Increase write throughput
    /// - Reduce total write time
    /// - Better utilize available bandwidth
    /// - Trade memory for performance
    pub concurrent: usize,
    /// Sets chunk size for buffered writes.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_multi_min_size`] and [`Capability::write_multi_max_size`] for size limits.
    ///
    /// ### Behavior
    ///
    /// - By default, OpenDAL sets optimal chunk size based on service capabilities
    /// - When chunk size is set:
    ///   - Data will be buffered until reaching chunk size
    ///   - One API call will be made per chunk
    ///   - Last chunk may be smaller than chunk size
    /// - Important considerations:
    ///   - Some services require minimum chunk sizes (e.g. S3's EntityTooSmall error)
    ///   - Smaller chunks increase API calls and costs
    ///   - Larger chunks increase memory usage, but improve performance and reduce costs
    ///
    /// ### Performance Impact
    ///
    /// Setting appropriate chunk size can:
    /// - Reduce number of API calls
    /// - Improve overall throughput
    /// - Lower operation costs
    /// - Better utilize network bandwidth
    pub chunk: Option<usize>,
}

/// Options for copy operations.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct CopyOptions {
    /// Sets the condition that copy operation will succeed only if target does not exist.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::copy_with_if_not_exists`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, the copy operation will only succeed if the target path does not exist
    /// - Will return error if target already exists
    /// - If not supported, the value will be ignored
    ///
    /// This operation provides a way to ensure copy operations only create new resources
    /// without overwriting existing ones, useful for implementing "copy if not exists" logic.
    pub if_not_exists: bool,
}
