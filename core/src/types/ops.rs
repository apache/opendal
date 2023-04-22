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

use crate::raw::*;

/// Args for `create` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpCreate {}

impl OpCreate {
    /// Create a new `OpCreate`.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Args for `delete` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpDelete {}

impl OpDelete {
    /// Create a new `OpDelete`.
    pub fn new() -> Self {
        Self {}
    }
}

/// Args for `list` operation.
#[derive(Debug, Clone, Default)]
pub struct OpList {
    /// The limit passed to underlying service to specify the max results
    /// that could return.
    limit: Option<usize>,

    /// The start_after passes to underlying service to specify the specified key
    /// to start listing from.
    start_after: Option<String>,
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
}

/// Args for `scan` operation.
#[derive(Debug, Default, Clone)]
pub struct OpScan {
    /// The limit passed to underlying service to specify the max results
    /// that could return.
    limit: Option<usize>,
}

impl OpScan {
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
    br: BytesRange,
    if_match: Option<String>,
    if_none_match: Option<String>,
    override_cache_control: Option<String>,
    override_content_disposition: Option<String>,
}

impl OpRead {
    /// Create a default `OpRead` which will read whole content of path.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new OpRead with range.
    pub fn with_range(mut self, range: BytesRange) -> Self {
        self.br = range;
        self
    }

    /// Get range from OpRead.
    pub fn range(&self) -> BytesRange {
        self.br
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
}

/// Args for `stat` operation.
#[derive(Debug, Clone, Default)]
pub struct OpStat {
    if_match: Option<String>,
    if_none_match: Option<String>,
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
}

/// Args for `write` operation.
#[derive(Debug, Clone, Default)]
pub struct OpWrite {
    content_length: Option<u64>,
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

    /// Get the content length from op.
    ///
    /// The content length is the total length of the data to be written.
    pub fn content_length(&self) -> Option<u64> {
        self.content_length
    }

    /// Set the content length of op.
    ///
    /// If the content length is not set, the content length will be
    /// calculated automatically by buffering part of data.
    pub fn with_content_length(mut self, content_length: u64) -> Self {
        self.content_length = Some(content_length);
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
