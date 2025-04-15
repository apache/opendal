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

use crate::raw::*;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::time::Duration;

/// Args for `create` operation.
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

/// Args for `delete` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default, Eq, Hash, PartialEq)]
pub struct OpDelete {
    version: Option<String>,
}

impl OpDelete {
    /// Create a new `OpDelete`.
    pub fn new() -> Self {
        Self::default()
    }
}

impl OpDelete {
    /// Change the version of this delete operation.
    pub fn with_version(mut self, version: &str) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Get the version of this delete operation.
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
}

/// Args for `delete` operation.
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

/// Args for `list` operation.
#[derive(Debug, Clone)]
pub struct OpList {
    /// The limit passed to underlying service to specify the max results
    /// that could return per-request.
    ///
    /// Users could use this to control the memory usage of list operation.
    limit: Option<usize>,
    /// The start_after passes to underlying service to specify the specified key
    /// to start listing from.
    start_after: Option<String>,
    /// The recursive is used to control whether the list operation is recursive.
    ///
    /// - If `false`, list operation will only list the entries under the given path.
    /// - If `true`, list operation will list all entries that starts with given path.
    ///
    /// Default to `false`.
    recursive: bool,
    /// The concurrent of stat operations inside list operation.
    /// Users could use this to control the number of concurrent stat operation when metadata is unknown.
    ///
    /// - If this is set to <= 1, the list operation will be sequential.
    /// - If this is set to > 1, the list operation will be concurrent,
    ///   and the maximum number of concurrent operations will be determined by this value.
    concurrent: usize,
    /// The version is used to control whether the object versions should be returned.
    ///
    /// - If `false`, list operation will not return with object versions
    /// - If `true`, list operation will return with object versions if object versioning is supported
    ///   by the underlying service
    ///
    /// Default to `false`
    versions: bool,
    /// The deleted is used to control whether the deleted objects should be returned.
    ///
    /// - If `false`, list operation will not return with deleted objects
    /// - If `true`, list operation will return with deleted objects if object versioning is supported
    ///   by the underlying service
    ///
    /// Default to `false`
    deleted: bool,
}

impl Default for OpList {
    fn default() -> Self {
        OpList {
            limit: None,
            start_after: None,
            recursive: false,
            concurrent: 1,
            versions: false,
            deleted: false,
        }
    }
}

impl OpList {
    /// Create a new `OpList`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Change the limit of this list operation.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Get the limit of list operation.
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Change the start_after of this list operation.
    pub fn with_start_after(mut self, start_after: &str) -> Self {
        self.start_after = Some(start_after.into());
        self
    }

    /// Get the start_after of list operation.
    pub fn start_after(&self) -> Option<&str> {
        self.start_after.as_deref()
    }

    /// The recursive is used to control whether the list operation is recursive.
    ///
    /// - If `false`, list operation will only list the entries under the given path.
    /// - If `true`, list operation will list all entries that starts with given path.
    ///
    /// Default to `false`.
    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    /// Get the current recursive.
    pub fn recursive(&self) -> bool {
        self.recursive
    }

    /// Change the concurrent of this list operation.
    ///
    /// The default concurrent is 1.
    pub fn with_concurrent(mut self, concurrent: usize) -> Self {
        self.concurrent = concurrent;
        self
    }

    /// Get the concurrent of list operation.
    pub fn concurrent(&self) -> usize {
        self.concurrent
    }

    /// Change the version of this list operation
    #[deprecated(since = "0.51.1", note = "use with_versions instead")]
    pub fn with_version(mut self, version: bool) -> Self {
        self.versions = version;
        self
    }

    /// Change the version of this list operation
    pub fn with_versions(mut self, versions: bool) -> Self {
        self.versions = versions;
        self
    }

    /// Get the version of this list operation
    #[deprecated(since = "0.51.1", note = "use versions instead")]
    pub fn version(&self) -> bool {
        self.versions
    }

    /// Get the version of this list operation
    pub fn versions(&self) -> bool {
        self.versions
    }

    /// Change the deleted of this list operation
    pub fn with_deleted(mut self, deleted: bool) -> Self {
        self.deleted = deleted;
        self
    }

    /// Get the deleted of this list operation
    pub fn deleted(&self) -> bool {
        self.deleted
    }
}

/// Args for `presign` operation.
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

    /// Get operation from op.
    pub fn operation(&self) -> &PresignOperation {
        &self.op
    }

    /// Get expire from op.
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
    Read(OpRead),
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
        Self::Read(v)
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

/// Args for `read` operation.
#[derive(Debug, Clone, Default)]
pub struct OpRead {
    range: BytesRange,
    if_match: Option<String>,
    if_none_match: Option<String>,
    if_modified_since: Option<DateTime<Utc>>,
    if_unmodified_since: Option<DateTime<Utc>>,
    override_content_type: Option<String>,
    override_cache_control: Option<String>,
    override_content_disposition: Option<String>,
    version: Option<String>,
}

impl OpRead {
    /// Create a default `OpRead` which will read whole content of path.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the range of the option
    pub fn with_range(mut self, range: BytesRange) -> Self {
        self.range = range;
        self
    }

    /// Get range from option
    pub fn range(&self) -> BytesRange {
        self.range
    }

    /// Returns a mutable range to allow updating.
    pub(crate) fn range_mut(&mut self) -> &mut BytesRange {
        &mut self.range
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
    pub fn with_if_modified_since(mut self, v: DateTime<Utc>) -> Self {
        self.if_modified_since = Some(v);
        self
    }

    /// Get If-Modified-Since from option
    pub fn if_modified_since(&self) -> Option<DateTime<Utc>> {
        self.if_modified_since
    }

    /// Set the If-Unmodified-Since of the option
    pub fn with_if_unmodified_since(mut self, v: DateTime<Utc>) -> Self {
        self.if_unmodified_since = Some(v);
        self
    }

    /// Get If-Unmodified-Since from option
    pub fn if_unmodified_since(&self) -> Option<DateTime<Utc>> {
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
}

/// Args for reader operation.
#[derive(Debug, Clone)]
pub struct OpReader {
    /// The concurrent requests that reader can send.
    concurrent: usize,
    /// The chunk size of each request.
    chunk: Option<usize>,
    /// The gap size of each request.
    gap: Option<usize>,
}

impl Default for OpReader {
    fn default() -> Self {
        Self {
            concurrent: 1,
            chunk: None,
            gap: None,
        }
    }
}

impl OpReader {
    /// Create a new `OpReader`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the concurrent of the option
    pub fn with_concurrent(mut self, concurrent: usize) -> Self {
        self.concurrent = concurrent.max(1);
        self
    }

    /// Get concurrent from option
    pub fn concurrent(&self) -> usize {
        self.concurrent
    }

    /// Set the chunk of the option
    pub fn with_chunk(mut self, chunk: usize) -> Self {
        self.chunk = Some(chunk.max(1));
        self
    }

    /// Get chunk from option
    pub fn chunk(&self) -> Option<usize> {
        self.chunk
    }

    /// Set the gap of the option
    pub fn with_gap(mut self, gap: usize) -> Self {
        self.gap = Some(gap.max(1));
        self
    }

    /// Get gap from option
    pub fn gap(&self) -> Option<usize> {
        self.gap
    }
}

/// Args for `stat` operation.
#[derive(Debug, Clone, Default)]
pub struct OpStat {
    if_match: Option<String>,
    if_none_match: Option<String>,
    if_modified_since: Option<DateTime<Utc>>,
    if_unmodified_since: Option<DateTime<Utc>>,
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
    pub fn with_if_modified_since(mut self, v: DateTime<Utc>) -> Self {
        self.if_modified_since = Some(v);
        self
    }

    /// Get If-Modified-Since from option
    pub fn if_modified_since(&self) -> Option<DateTime<Utc>> {
        self.if_modified_since
    }

    /// Set the If-Unmodified-Since of the option
    pub fn with_if_unmodified_since(mut self, v: DateTime<Utc>) -> Self {
        self.if_unmodified_since = Some(v);
        self
    }

    /// Get If-Unmodified-Since from option
    pub fn if_unmodified_since(&self) -> Option<DateTime<Utc>> {
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

/// Args for `write` operation.
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
    /// Service could return `Unsupported` if the underlying storage does not support append.
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

/// Args for `writer` operation.
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

/// Args for `copy` operation.
#[derive(Debug, Clone, Default)]
pub struct OpCopy {}

impl OpCopy {
    /// Create a new `OpCopy`.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Args for `rename` operation.
#[derive(Debug, Clone, Default)]
pub struct OpRename {}

impl OpRename {
    /// Create a new `OpMove`.
    pub fn new() -> Self {
        Self::default()
    }
}
