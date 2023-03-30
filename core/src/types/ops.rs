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

use time::Duration;

use crate::raw::*;
use crate::*;

/// Args for `create` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpCreate {
    mode: EntryMode,
}

impl OpCreate {
    /// Create a new `OpCreate`.
    pub fn new(mode: EntryMode) -> Self {
        Self { mode }
    }

    /// Get mode from option.
    pub fn mode(&self) -> EntryMode {
        self.mode
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
    ops: BatchOperations,
}

impl OpBatch {
    /// Create a new batch options.
    pub fn new(ops: BatchOperations) -> Self {
        Self { ops }
    }

    /// Get operation from op.
    pub fn operation(&self) -> &BatchOperations {
        &self.ops
    }

    /// Consume OpBatch into BatchOperation
    pub fn into_operation(self) -> BatchOperations {
        self.ops
    }
}

/// Batch operation used for batch.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BatchOperations {
    /// Batch delete operations.
    Delete(Vec<(String, OpDelete)>),
}

impl BatchOperations {
    /// Return the operation of this batch.
    pub fn operation(&self) -> Operation {
        use BatchOperations::*;
        match self {
            Delete(_) => Operation::Delete,
        }
    }

    /// Return the length of given operations.
    pub fn len(&self) -> usize {
        use BatchOperations::*;
        match self {
            Delete(v) => v.len(),
        }
    }

    /// Return if given operations is empty.
    pub fn is_empty(&self) -> bool {
        use BatchOperations::*;
        match self {
            Delete(v) => v.is_empty(),
        }
    }
}

/// Args for `read` operation.
#[derive(Debug, Clone, Default)]
pub struct OpRead {
    br: BytesRange,
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
    pub fn with_override_content_disposition(
        mut self,
        content_disposition: impl Into<String>,
    ) -> Self {
        self.override_content_disposition = Some(content_disposition.into());
        self
    }

    /// Returns the content-disposition header that should be send back by the remote read
    /// operation.
    pub fn override_content_disposition(&self) -> Option<&str> {
        self.override_content_disposition.as_deref()
    }
}

/// Args for `stat` operation.
#[derive(Debug, Clone, Default)]
pub struct OpStat {}

impl OpStat {
    /// Create a new `OpStat`.
    pub fn new() -> Self {
        Self {}
    }
}

/// Args for `write` operation.
#[derive(Debug, Clone, Default)]
pub struct OpWrite {
    append: bool,

    content_type: Option<String>,
    content_disposition: Option<String>,
}

impl OpWrite {
    /// Create a new `OpWrite`.
    ///
    /// If input path is not a file path, an error will be returned.
    pub fn new() -> Self {
        Self {
            append: false,

            content_type: None,
            content_disposition: None,
        }
    }

    pub(crate) fn with_append(mut self) -> Self {
        self.append = true;
        self
    }

    pub(crate) fn append(&self) -> bool {
        self.append
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
}
