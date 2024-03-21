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

use std::time::Duration;

use flagset::FlagSet;

use crate::raw::*;
use crate::Metakey;

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
#[derive(Debug, Clone, Default)]
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
    /// Metakey is used to control which meta should be returned.
    ///
    /// Lister will make sure the result for specified meta is **known**:
    ///
    /// - `Some(v)` means exist.
    /// - `None` means services doesn't have this meta.
    metakey: FlagSet<Metakey>,
    /// The concurrent of stat operations inside list operation.
    /// Users could use this to control the number of concurrent stat operation when metadata is unknown.
    ///
    /// - If this is set to <= 1, the list operation will be sequential.
    /// - If this is set to > 1, the list operation will be concurrent,
    ///   and the maximum number of concurrent operations will be determined by this value.
    concurrent: usize,
}

impl Default for OpList {
    fn default() -> Self {
        OpList {
            limit: None,
            start_after: None,
            recursive: false,
            // By default, we want to know what's the mode of this entry.
            metakey: Metakey::Mode.into(),
            concurrent: 1,
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

    /// Change the metakey of this list operation.
    ///
    /// The default metakey is `Metakey::Mode`.
    pub fn with_metakey(mut self, metakey: impl Into<FlagSet<Metakey>>) -> Self {
        self.metakey = metakey.into();
        self
    }

    /// Get the current metakey.
    pub fn metakey(&self) -> FlagSet<Metakey> {
        self.metakey
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

/// Args for `batch` operation.
#[derive(Debug, Clone)]
pub struct OpBatch {
    ops: Vec<(String, BatchOperation)>,
}

impl OpBatch {
    /// Create a new batch options.
    pub fn new(ops: Vec<(String, BatchOperation)>) -> Self {
        Self { ops }
    }

    /// Get operation from op.
    pub fn operation(&self) -> &[(String, BatchOperation)] {
        &self.ops
    }

    /// Consume OpBatch into BatchOperation
    pub fn into_operation(self) -> Vec<(String, BatchOperation)> {
        self.ops
    }
}

/// Batch operation used for batch.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BatchOperation {
    /// Batch delete operation.
    Delete(OpDelete),
}

impl From<OpDelete> for BatchOperation {
    fn from(op: OpDelete) -> Self {
        Self::Delete(op)
    }
}

impl BatchOperation {
    /// Return the operation of this batch.
    pub fn operation(&self) -> Operation {
        use BatchOperation::*;
        match self {
            Delete(_) => Operation::Delete,
        }
    }
}

/// Args for `read` operation.
#[derive(Debug, Clone, Default)]
pub struct OpRead {
    if_match: Option<String>,
    if_none_match: Option<String>,
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

    /// Sets the content-disposition header that should be send back by the remote read operation.
    pub fn with_override_content_disposition(mut self, content_disposition: &str) -> Self {
        self.override_content_disposition = Some(content_disposition.into());
        self
    }

    /// Returns the content-disposition header that should be send back by the remote read
    /// operation.
    pub fn override_content_disposition(&self) -> Option<&str> {
        self.override_content_disposition.as_deref()
    }

    /// Sets the cache-control header that should be send back by the remote read operation.
    pub fn with_override_cache_control(mut self, cache_control: &str) -> Self {
        self.override_cache_control = Some(cache_control.into());
        self
    }

    /// Returns the cache-control header that should be send back by the remote read operation.
    pub fn override_cache_control(&self) -> Option<&str> {
        self.override_cache_control.as_deref()
    }

    /// Sets the content-type header that should be send back by the remote read operation.
    pub fn with_override_content_type(mut self, content_type: &str) -> Self {
        self.override_content_type = Some(content_type.into());
        self
    }

    /// Returns the content-type header that should be send back by the remote read operation.
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

/// Args for `stat` operation.
#[derive(Debug, Clone, Default)]
pub struct OpStat {
    if_match: Option<String>,
    if_none_match: Option<String>,
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

    /// Sets the content-disposition header that should be send back by the remote read operation.
    pub fn with_override_content_disposition(mut self, content_disposition: &str) -> Self {
        self.override_content_disposition = Some(content_disposition.into());
        self
    }

    /// Returns the content-disposition header that should be send back by the remote read
    /// operation.
    pub fn override_content_disposition(&self) -> Option<&str> {
        self.override_content_disposition.as_deref()
    }

    /// Sets the cache-control header that should be send back by the remote read operation.
    pub fn with_override_cache_control(mut self, cache_control: &str) -> Self {
        self.override_cache_control = Some(cache_control.into());
        self
    }

    /// Returns the cache-control header that should be send back by the remote read operation.
    pub fn override_cache_control(&self) -> Option<&str> {
        self.override_cache_control.as_deref()
    }

    /// Sets the content-type header that should be send back by the remote read operation.
    pub fn with_override_content_type(mut self, content_type: &str) -> Self {
        self.override_content_type = Some(content_type.into());
        self
    }

    /// Returns the content-type header that should be send back by the remote read operation.
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
    buffer: Option<usize>,
    concurrent: usize,

    content_type: Option<String>,
    content_disposition: Option<String>,
    cache_control: Option<String>,
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

    /// Get the buffer from op.
    ///
    /// The buffer is used by service to decide the buffer size of the underlying writer.
    pub fn buffer(&self) -> Option<usize> {
        self.buffer
    }

    /// Set the buffer of op.
    ///
    /// If buffer is set, the data will be buffered by the underlying writer.
    ///
    /// ## NOTE
    ///
    /// Service could have their own minimum buffer size while perform write operations like
    /// multipart uploads. So the buffer size may be larger than the given buffer size.
    pub fn with_buffer(mut self, buffer: usize) -> Self {
        self.buffer = Some(buffer);
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
