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

//! Ops provides the operation args struct like [`OpRead`] for user.
//!
//! By using ops, users can add more context for operation.

use crate::BytesRange;
use crate::options;
use crate::raw::*;

use std::collections::HashMap;

/// Arguments for `create` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpCreateDir {}

impl OpCreateDir {
    /// Create a new `OpCreateDir`.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Arguments for `delete` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default, Eq, Hash, PartialEq)]
pub struct OpDelete {
    /// The version of the object to delete.
    version: Option<String>,

    /// Whether a `delete` is recursive.
    recursive: bool,
}

impl OpDelete {
    /// Create a new `OpDelete`.
    pub fn new() -> Self {
        Self::default()
    }
}

impl OpDelete {
    /// Set the version of the object to delete.
    pub fn with_version(mut self, version: &str) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Change the recursive flag of this delete operation.
    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    /// Return the version of the object to delete.
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Whether this delete should remove objects recursively.
    pub fn recursive(&self) -> bool {
        self.recursive
    }
}

impl From<options::DeleteOptions> for OpDelete {
    fn from(value: options::DeleteOptions) -> Self {
        Self {
            version: value.version,
            recursive: value.recursive,
        }
    }
}

/// Arguments for `delete` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpDeleter {}

impl OpDeleter {
    /// Create a new `OpDelete`.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Arguments for `list` operation.
#[derive(Debug, Clone, Default)]
pub struct OpList {
    /// The maximum number of results that the service should return per request.
    ///
    /// This can be used to control the memory consumption of a list operation.
    limit: Option<usize>,

    /// The key after which the service should start listing.
    start_after: Option<String>,

    /// Whether the list operation is recursive.
    ///
    /// - If `false`, the operation lists only the immediate children of the given
    ///   path.
    /// - If `true`, the operation lists all entries whose paths start with the
    ///   given path.
    ///
    /// Defaults to `false`.
    recursive: bool,

    /// Whether to return object versions.
    ///
    /// - If `false`, the operation does not return object versions.
    /// - If `true`, the operation returns object versions when the service
    ///   supports versioning.
    ///
    /// Defaults to `false`.
    versions: bool,

    /// Whether to return deleted objects.
    ///
    /// - If `false`, the operation does not return deleted objects.
    /// - If `true`, the operation returns deleted objects when the service
    ///   supports versioning.
    ///
    /// Defaults to `false`.
    deleted: bool,
    /// The glob pattern used to filter entries returned by the list operation.
    ///
    /// When set, only entries whose paths (relative to the list root) match the
    /// given Unix shell style glob pattern will be returned.
    ///
    /// Pattern matching is delegated to the underlying service if it advertises
    /// `list_with_glob` capability; otherwise a [`GlobLayer`] (or similar) is
    /// required to provide a client-side fallback.
    glob: Option<String>,
}

impl OpList {
    /// Create a new `OpList`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of results per request.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Return the maximum number of results per request.
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Set the key after which listing should start.
    pub fn with_start_after(mut self, start_after: &str) -> Self {
        self.start_after = Some(start_after.into());
        self
    }

    /// Return the key after which listing should start.
    pub fn start_after(&self) -> Option<&str> {
        self.start_after.as_deref()
    }

    /// Set whether the list operation is recursive.
    ///
    /// - If `false`, the operation lists only the immediate children of the given
    ///   path.
    /// - If `true`, the operation lists all entries whose paths start with the
    ///   given path.
    ///
    /// Defaults to `false`.
    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    /// Return whether the list operation is recursive.
    pub fn recursive(&self) -> bool {
        self.recursive
    }

    /// Change the concurrent of this list operation.
    ///
    /// The default concurrent is 1.
    #[deprecated(since = "0.53.2", note = "concurrent in list is no-op")]
    pub fn with_concurrent(self, concurrent: usize) -> Self {
        let _ = concurrent;
        self
    }

    /// Get the concurrent of list operation.
    #[deprecated(since = "0.53.2", note = "concurrent in list is no-op")]
    pub fn concurrent(&self) -> usize {
        0
    }

    /// Set whether to return object versions.
    pub fn with_versions(mut self, versions: bool) -> Self {
        self.versions = versions;
        self
    }

    /// Return whether the operation includes object versions.
    pub fn versions(&self) -> bool {
        self.versions
    }

    /// Set whether to return deleted objects.
    pub fn with_deleted(mut self, deleted: bool) -> Self {
        self.deleted = deleted;
        self
    }

    /// Return whether the operation includes deleted objects.
    pub fn deleted(&self) -> bool {
        self.deleted
    }

    /// Change the glob pattern of this list operation.
    ///
    /// When set, only entries whose paths match the given Unix shell style glob
    /// pattern will be returned. Patterns support `*`, `?`, `**`, `{a,b}` and
    /// character classes like `[a-z]`.
    pub fn with_glob(mut self, glob: &str) -> Self {
        self.glob = Some(glob.to_string());
        self
    }

    /// Get the glob pattern of this list operation.
    pub fn glob(&self) -> Option<&str> {
        self.glob.as_deref()
    }

    /// Clear the glob pattern. Used by `GlobLayer` when forwarding a
    /// guided-traversal template to the underlying accessor.
    pub(crate) fn clear_glob(&mut self) {
        self.glob = None;
    }

    /// Clear the limit. Used by `GlobLayer`: the caller's limit applies to
    /// filtered results, not raw entries from each downstream list call.
    pub(crate) fn clear_limit(&mut self) {
        self.limit = None;
    }

    /// Clear the `start_after` cursor. Used by `GlobLayer`: pagination is
    /// undefined across the layer's multi-list traversal.
    pub(crate) fn clear_start_after(&mut self) {
        self.start_after = None;
    }
}

impl From<options::ListOptions> for OpList {
    fn from(value: options::ListOptions) -> Self {
        Self {
            limit: value.limit,
            start_after: value.start_after,
            recursive: value.recursive,
            versions: value.versions,
            deleted: value.deleted,
            glob: value.glob,
        }
    }
}

/// Arguments for `presign` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone)]
pub struct OpPresign {
    expire: Duration,

    op: PresignOperation,
}

impl OpPresign {
    /// Create a new `OpPresign`.
    pub fn new(op: impl Into<PresignOperation>, expire: Duration) -> Self {
        Self {
            op: op.into(),
            expire,
        }
    }

    /// Return the operation to presign.
    pub fn operation(&self) -> &PresignOperation {
        &self.op
    }

    /// Return the request expiration duration.
    pub fn expire(&self) -> Duration {
        self.expire
    }

    /// Consume OpPresign into (Duration, PresignOperation)
    pub fn into_parts(self) -> (Duration, PresignOperation) {
        (self.expire, self.op)
    }
}

/// Presign operation used for presign.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PresignOperation {
    /// Presign a stat(head) operation.
    Stat(OpStat),
    /// Presign a read operation.
    Read(BytesRange, OpRead),
    /// Presign a write operation.
    Write(OpWrite),
    /// Presign a delete operation.
    Delete(OpDelete),
}

impl From<OpStat> for PresignOperation {
    fn from(op: OpStat) -> Self {
        Self::Stat(op)
    }
}

impl From<OpRead> for PresignOperation {
    fn from(v: OpRead) -> Self {
        Self::Read(BytesRange::default(), v)
    }
}

impl From<OpWrite> for PresignOperation {
    fn from(v: OpWrite) -> Self {
        Self::Write(v)
    }
}

impl From<OpDelete> for PresignOperation {
    fn from(v: OpDelete) -> Self {
        Self::Delete(v)
    }
}

/// Arguments for `read` operation.
#[derive(Debug, Clone, Default)]
pub struct OpRead {
    if_match: Option<String>,
    if_none_match: Option<String>,
    if_modified_since: Option<Timestamp>,
    if_unmodified_since: Option<Timestamp>,
    override_content_type: Option<String>,
    override_cache_control: Option<String>,
    override_content_disposition: Option<String>,
    version: Option<String>,
    content_length_hint: Option<u64>,
}

impl OpRead {
    /// Create a default `OpRead` which will read whole content of path.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the content-disposition header that should be sent back by the remote read operation.
    pub fn with_override_content_disposition(mut self, content_disposition: &str) -> Self {
        self.override_content_disposition = Some(content_disposition.into());
        self
    }

    /// Returns the content-disposition header that should be sent back by the remote read
    /// operation.
    pub fn override_content_disposition(&self) -> Option<&str> {
        self.override_content_disposition.as_deref()
    }

    /// Sets the cache-control header that should be sent back by the remote read operation.
    pub fn with_override_cache_control(mut self, cache_control: &str) -> Self {
        self.override_cache_control = Some(cache_control.into());
        self
    }

    /// Returns the cache-control header that should be sent back by the remote read operation.
    pub fn override_cache_control(&self) -> Option<&str> {
        self.override_cache_control.as_deref()
    }

    /// Sets the content-type header that should be sent back by the remote read operation.
    pub fn with_override_content_type(mut self, content_type: &str) -> Self {
        self.override_content_type = Some(content_type.into());
        self
    }

    /// Returns the content-type header that should be sent back by the remote read operation.
    pub fn override_content_type(&self) -> Option<&str> {
        self.override_content_type.as_deref()
    }

    /// Set the If-Match of the option
    pub fn with_if_match(mut self, if_match: &str) -> Self {
        self.if_match = Some(if_match.to_string());
        self
    }

    /// Get If-Match from option
    pub fn if_match(&self) -> Option<&str> {
        self.if_match.as_deref()
    }

    /// Set the If-None-Match of the option
    pub fn with_if_none_match(mut self, if_none_match: &str) -> Self {
        self.if_none_match = Some(if_none_match.to_string());
        self
    }

    /// Get If-None-Match from option
    pub fn if_none_match(&self) -> Option<&str> {
        self.if_none_match.as_deref()
    }

    /// Set the If-Modified-Since of the option
    pub fn with_if_modified_since(mut self, v: Timestamp) -> Self {
        self.if_modified_since = Some(v);
        self
    }

    /// Return the If-Modified-Since condition.
    pub fn if_modified_since(&self) -> Option<Timestamp> {
        self.if_modified_since
    }

    /// Set the If-Unmodified-Since of the option
    pub fn with_if_unmodified_since(mut self, v: Timestamp) -> Self {
        self.if_unmodified_since = Some(v);
        self
    }

    /// Get If-Unmodified-Since from option
    pub fn if_unmodified_since(&self) -> Option<Timestamp> {
        self.if_unmodified_since
    }

    /// Set the version of the option
    pub fn with_version(mut self, version: &str) -> Self {
        self.version = Some(version.to_string());
        self
    }

    /// Get version from option
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    pub(crate) fn content_length_hint(&self) -> Option<u64> {
        self.content_length_hint
    }
}

/// Arguments for reader operation.
#[derive(Debug, Clone)]
pub struct OpReader {
    /// The number of concurrent requests that reader can send.
    concurrent: usize,
    /// Request chunk size.
    chunk: Option<usize>,
    /// The gap size of each request.
    gap: Option<usize>,
    /// The maximum number of buffers that can be prefetched.
    prefetch: usize,
}

impl Default for OpReader {
    fn default() -> Self {
        Self {
            concurrent: 1,
            chunk: None,
            gap: None,
            prefetch: 0,
        }
    }
}

impl OpReader {
    /// Create a new `OpReader`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of concurrent requests the reader can send.
    pub fn with_concurrent(mut self, concurrent: usize) -> Self {
        self.concurrent = concurrent.max(1);
        self
    }

    /// Return the number of concurrent requests.
    pub fn concurrent(&self) -> usize {
        self.concurrent
    }

    /// Set the request chunk size.
    pub fn with_chunk(mut self, chunk: usize) -> Self {
        self.chunk = Some(chunk.max(1));
        self
    }

    /// Return the request chunk size.
    pub fn chunk(&self) -> Option<usize> {
        self.chunk
    }

    /// Set the gap size.
    pub fn with_gap(mut self, gap: usize) -> Self {
        self.gap = Some(gap.max(1));
        self
    }

    /// Return the gap size.
    pub fn gap(&self) -> Option<usize> {
        self.gap
    }

    /// Set the number of prefetch requests.
    pub fn with_prefetch(mut self, prefetch: usize) -> Self {
        self.prefetch = prefetch;
        self
    }

    /// Return the number of prefetch requests.
    pub fn prefetch(&self) -> usize {
        self.prefetch
    }
}

impl From<options::ReadOptions> for (BytesRange, OpRead, OpReader) {
    fn from(value: options::ReadOptions) -> Self {
        (
            value.range,
            OpRead {
                if_match: value.if_match,
                if_none_match: value.if_none_match,
                if_modified_since: value.if_modified_since,
                if_unmodified_since: value.if_unmodified_since,
                override_content_type: value.override_content_type,
                override_cache_control: value.override_cache_control,
                override_content_disposition: value.override_content_disposition,
                version: value.version,
                content_length_hint: value.content_length_hint,
            },
            OpReader {
                // Ensure concurrent is at least 1
                concurrent: value.concurrent.max(1),
                chunk: value.chunk,
                gap: value.gap,
                prefetch: 0,
            },
        )
    }
}

impl From<options::ReaderOptions> for (OpRead, OpReader) {
    fn from(value: options::ReaderOptions) -> Self {
        (
            OpRead {
                if_match: value.if_match,
                if_none_match: value.if_none_match,
                if_modified_since: value.if_modified_since,
                if_unmodified_since: value.if_unmodified_since,
                override_content_type: None,
                override_cache_control: None,
                override_content_disposition: None,
                version: value.version,
                content_length_hint: value.content_length_hint,
            },
            OpReader {
                // Ensure concurrent is at least 1
                concurrent: value.concurrent.max(1),
                chunk: value.chunk,
                gap: value.gap,
                prefetch: value.prefetch,
            },
        )
    }
}

/// Arguments for `stat` operation.
#[derive(Debug, Clone, Default)]
pub struct OpStat {
    if_match: Option<String>,
    if_none_match: Option<String>,
    if_modified_since: Option<Timestamp>,
    if_unmodified_since: Option<Timestamp>,
    override_content_type: Option<String>,
    override_cache_control: Option<String>,
    override_content_disposition: Option<String>,
    version: Option<String>,
}

impl OpStat {
    /// Create a new `OpStat`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the If-Match of the option
    pub fn with_if_match(mut self, if_match: &str) -> Self {
        self.if_match = Some(if_match.to_string());
        self
    }

    /// Get If-Match from option
    pub fn if_match(&self) -> Option<&str> {
        self.if_match.as_deref()
    }

    /// Set the If-None-Match of the option
    pub fn with_if_none_match(mut self, if_none_match: &str) -> Self {
        self.if_none_match = Some(if_none_match.to_string());
        self
    }

    /// Get If-None-Match from option
    pub fn if_none_match(&self) -> Option<&str> {
        self.if_none_match.as_deref()
    }

    /// Set the If-Modified-Since of the option
    pub fn with_if_modified_since(mut self, v: Timestamp) -> Self {
        self.if_modified_since = Some(v);
        self
    }

    /// Get If-Modified-Since from option
    pub fn if_modified_since(&self) -> Option<Timestamp> {
        self.if_modified_since
    }

    /// Set the If-Unmodified-Since of the option
    pub fn with_if_unmodified_since(mut self, v: Timestamp) -> Self {
        self.if_unmodified_since = Some(v);
        self
    }

    /// Get If-Unmodified-Since from option
    pub fn if_unmodified_since(&self) -> Option<Timestamp> {
        self.if_unmodified_since
    }

    /// Sets the content-disposition header that should be sent back by the remote read operation.
    pub fn with_override_content_disposition(mut self, content_disposition: &str) -> Self {
        self.override_content_disposition = Some(content_disposition.into());
        self
    }

    /// Returns the content-disposition header that should be sent back by the remote read
    /// operation.
    pub fn override_content_disposition(&self) -> Option<&str> {
        self.override_content_disposition.as_deref()
    }

    /// Sets the cache-control header that should be sent back by the remote read operation.
    pub fn with_override_cache_control(mut self, cache_control: &str) -> Self {
        self.override_cache_control = Some(cache_control.into());
        self
    }

    /// Returns the cache-control header that should be sent back by the remote read operation.
    pub fn override_cache_control(&self) -> Option<&str> {
        self.override_cache_control.as_deref()
    }

    /// Sets the content-type header that should be sent back by the remote read operation.
    pub fn with_override_content_type(mut self, content_type: &str) -> Self {
        self.override_content_type = Some(content_type.into());
        self
    }

    /// Returns the content-type header that should be sent back by the remote read operation.
    pub fn override_content_type(&self) -> Option<&str> {
        self.override_content_type.as_deref()
    }

    /// Set the version of the option
    pub fn with_version(mut self, version: &str) -> Self {
        self.version = Some(version.to_string());
        self
    }

    /// Get version from option
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
}

impl From<options::StatOptions> for OpStat {
    fn from(value: options::StatOptions) -> Self {
        Self {
            if_match: value.if_match,
            if_none_match: value.if_none_match,
            if_modified_since: value.if_modified_since,
            if_unmodified_since: value.if_unmodified_since,
            override_content_type: value.override_content_type,
            override_cache_control: value.override_cache_control,
            override_content_disposition: value.override_content_disposition,
            version: value.version,
        }
    }
}

/// Arguments for `write` operation.
#[derive(Debug, Clone, Default)]
pub struct OpWrite {
    append: bool,
    concurrent: usize,
    content_type: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    cache_control: Option<String>,
    if_match: Option<String>,
    if_none_match: Option<String>,
    if_not_exists: bool,
    user_metadata: Option<HashMap<String, String>>,
}

impl OpWrite {
    /// Create a new `OpWrite`.
    ///
    /// If input path is not a file path, an error will be returned.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the append from op.
    ///
    /// The append is the flag to indicate that this write operation is an append operation.
    pub fn append(&self) -> bool {
        self.append
    }

    /// Set the append mode of op.
    ///
    /// If the append mode is set, the data will be appended to the end of the file.
    ///
    /// # Notes
    ///
    /// Service could return `Unsupported` if the storage does not support append.
    pub fn with_append(mut self, append: bool) -> Self {
        self.append = append;
        self
    }

    /// Get the content type from option
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// Set the content type of option
    pub fn with_content_type(mut self, content_type: &str) -> Self {
        self.content_type = Some(content_type.to_string());
        self
    }

    /// Get the content disposition from option
    pub fn content_disposition(&self) -> Option<&str> {
        self.content_disposition.as_deref()
    }

    /// Set the content disposition of option
    pub fn with_content_disposition(mut self, content_disposition: &str) -> Self {
        self.content_disposition = Some(content_disposition.to_string());
        self
    }

    /// Get the content encoding from option
    pub fn content_encoding(&self) -> Option<&str> {
        self.content_encoding.as_deref()
    }

    /// Set the content encoding of option
    pub fn with_content_encoding(mut self, content_encoding: &str) -> Self {
        self.content_encoding = Some(content_encoding.to_string());
        self
    }

    /// Get the cache control from option
    pub fn cache_control(&self) -> Option<&str> {
        self.cache_control.as_deref()
    }

    /// Set the content type of option
    pub fn with_cache_control(mut self, cache_control: &str) -> Self {
        self.cache_control = Some(cache_control.to_string());
        self
    }

    /// Get the concurrent.
    pub fn concurrent(&self) -> usize {
        self.concurrent
    }

    /// Set the maximum concurrent write task amount.
    pub fn with_concurrent(mut self, concurrent: usize) -> Self {
        self.concurrent = concurrent;
        self
    }

    /// Set the If-Match of the option
    pub fn with_if_match(mut self, s: &str) -> Self {
        self.if_match = Some(s.to_string());
        self
    }

    /// Get If-Match from option
    pub fn if_match(&self) -> Option<&str> {
        self.if_match.as_deref()
    }

    /// Set the If-None-Match of the option
    pub fn with_if_none_match(mut self, s: &str) -> Self {
        self.if_none_match = Some(s.to_string());
        self
    }

    /// Get If-None-Match from option
    pub fn if_none_match(&self) -> Option<&str> {
        self.if_none_match.as_deref()
    }

    /// Set the If-Not-Exist of the option
    pub fn with_if_not_exists(mut self, b: bool) -> Self {
        self.if_not_exists = b;
        self
    }

    /// Get If-Not-Exist from option
    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }

    /// Set the user defined metadata of the op
    pub fn with_user_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.user_metadata = Some(metadata);
        self
    }

    /// Get the user defined metadata from the op
    pub fn user_metadata(&self) -> Option<&HashMap<String, String>> {
        self.user_metadata.as_ref()
    }
}

/// Arguments for `writer` operation.
#[derive(Debug, Clone, Default)]
pub struct OpWriter {
    chunk: Option<usize>,
}

impl OpWriter {
    /// Create a new `OpWriter`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the chunk from op.
    ///
    /// The chunk is used by service to decide the chunk size of the underlying writer.
    pub fn chunk(&self) -> Option<usize> {
        self.chunk
    }

    /// Set the chunk of op.
    ///
    /// If chunk is set, the data will be chunked by the underlying writer.
    ///
    /// ## NOTE
    ///
    /// Service could have their own minimum chunk size while perform write
    /// operations like multipart uploads. So the chunk size may be larger than
    /// the given buffer size.
    pub fn with_chunk(mut self, chunk: usize) -> Self {
        self.chunk = Some(chunk);
        self
    }
}

impl From<options::WriteOptions> for (OpWrite, OpWriter) {
    fn from(value: options::WriteOptions) -> Self {
        (
            OpWrite {
                append: value.append,
                // Ensure concurrent is at least 1
                concurrent: value.concurrent.max(1),
                content_type: value.content_type,
                content_disposition: value.content_disposition,
                content_encoding: value.content_encoding,
                cache_control: value.cache_control,
                if_match: value.if_match,
                if_none_match: value.if_none_match,
                if_not_exists: value.if_not_exists,
                user_metadata: value.user_metadata,
            },
            OpWriter { chunk: value.chunk },
        )
    }
}

/// Arguments for `copy` operation.
#[derive(Debug, Clone, Default)]
pub struct OpCopy {
    if_not_exists: bool,
    if_match: Option<String>,
    source_version: Option<String>,
}

impl OpCopy {
    /// Create a new `OpCopy`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the if_not_exists flag for the operation.
    ///
    /// When set to true, the copy operation will only proceed if the destination
    /// doesn't already exist.
    pub fn with_if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    /// Get if_not_exists flag.
    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }

    /// Set the if_match condition for the operation.
    ///
    /// When set, the copy operation will only proceed if the existing destination
    /// object's ETag matches the given value.
    pub fn with_if_match(mut self, if_match: impl Into<String>) -> Self {
        self.if_match = Some(if_match.into());
        self
    }

    /// Get if_match condition.
    pub fn if_match(&self) -> Option<&str> {
        self.if_match.as_deref()
    }

    /// Set source version for the operation.
    ///
    /// When set, the copy operation will copy from the specified source version.
    pub fn with_source_version(mut self, version: impl Into<String>) -> Self {
        self.source_version = Some(version.into());
        self
    }

    /// Get source version from the operation.
    pub fn source_version(&self) -> Option<&str> {
        self.source_version.as_deref()
    }
}

/// Arguments for `copier` operation.
#[derive(Debug, Clone, Default)]
pub struct OpCopier {
    concurrent: usize,
    chunk: Option<usize>,
    source_content_length_hint: Option<u64>,
}

impl OpCopier {
    /// Create a new `OpCopier`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the concurrent tasks for the copier.
    pub fn with_concurrent(mut self, concurrent: usize) -> Self {
        self.concurrent = concurrent.max(1);
        self
    }

    /// Get the concurrent tasks for the copier.
    pub fn concurrent(&self) -> usize {
        self.concurrent.max(1)
    }

    /// Set the chunk size for the copier.
    pub fn with_chunk(mut self, chunk: usize) -> Self {
        self.chunk = Some(chunk);
        self
    }

    /// Get the chunk size for the copier.
    pub fn chunk(&self) -> Option<usize> {
        self.chunk
    }

    /// Set source content length hint for the copier.
    pub fn with_source_content_length_hint(mut self, content_length: u64) -> Self {
        self.source_content_length_hint = Some(content_length);
        self
    }

    /// Get source content length hint from the copier.
    pub fn source_content_length_hint(&self) -> Option<u64> {
        self.source_content_length_hint
    }
}

impl From<options::CopyOptions> for (OpCopy, OpCopier) {
    fn from(value: options::CopyOptions) -> Self {
        (
            OpCopy {
                if_not_exists: value.if_not_exists,
                if_match: value.if_match,
                source_version: value.source_version,
            },
            OpCopier {
                concurrent: value.concurrent.max(1),
                chunk: value.chunk,
                source_content_length_hint: value.source_content_length_hint,
            },
        )
    }
}

/// Arguments for `rename` operation.
#[derive(Debug, Clone, Default)]
pub struct OpRename {
    /// Whether the rename should fail when the destination already exists.
    ///
    /// If `true`, the rename succeeds only when the destination does not exist.
    /// If `false`, the rename uses OpenDAL's default overwrite behavior.
    if_not_exists: bool,
}

impl OpRename {
    /// Create a new `OpRename`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether the rename should fail when the destination already exists.
    ///
    /// If `true`, the rename succeeds only when the destination does not exist.
    /// If `false`, the rename uses OpenDAL's default overwrite behavior.
    ///
    /// ## Service Implementation
    ///
    /// Check [`Capability::rename_with_if_not_exists`] before setting this to
    /// `true`. A service might return `ErrorKind::Unsupported` if it cannot
    /// enforce the condition.
    pub fn with_if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    /// Return whether the rename should fail when the destination already exists.
    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

impl From<options::RenameOptions> for OpRename {
    fn from(value: options::RenameOptions) -> Self {
        Self {
            if_not_exists: value.if_not_exists,
        }
    }
}
