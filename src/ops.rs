// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    mode: ObjectMode,
}

impl OpCreate {
    /// Create a new `OpCreate`.
    pub fn new(mode: ObjectMode) -> Self {
        Self { mode }
    }

    /// Get object mode from option.
    pub fn mode(&self) -> ObjectMode {
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

/// Args for `create_multipart` operation.
#[derive(Debug, Clone, Default)]
pub struct OpCreateMultipart {}

impl OpCreateMultipart {
    /// Create a new `OpCreateMultipart`.
    pub fn new() -> Self {
        Self {}
    }
}

/// Args for `write_multipart` operation.
#[derive(Debug, Clone, Default)]
pub struct OpWriteMultipart {
    upload_id: String,
    part_number: usize,
    size: u64,
}

impl OpWriteMultipart {
    /// Create a new `OpWriteMultipart`.
    pub fn new(upload_id: String, part_number: usize, size: u64) -> Self {
        Self {
            upload_id,
            part_number,
            size,
        }
    }

    /// Get upload_id from option.
    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }

    /// Get part_number from option.
    pub fn part_number(&self) -> usize {
        self.part_number
    }

    /// Get size from option.
    pub fn size(&self) -> u64 {
        self.size
    }
}

/// Args for `complete_multipart` operation.
#[derive(Debug, Clone, Default)]
pub struct OpCompleteMultipart {
    upload_id: String,
    parts: Vec<ObjectPart>,
}

impl OpCompleteMultipart {
    /// Create a new `OpCompleteMultipart`.
    pub fn new(upload_id: String, parts: Vec<ObjectPart>) -> Self {
        Self { upload_id, parts }
    }

    /// Get upload_id from option.
    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }

    /// Get parts from option.
    pub fn parts(&self) -> &[ObjectPart] {
        &self.parts
    }
}

/// Args for `abort_multipart` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpAbortMultipart {
    upload_id: String,
}

impl OpAbortMultipart {
    /// Create a new `OpAbortMultipart`.
    ///
    /// If input path is not a file path, an error will be returned.
    pub fn new(upload_id: String) -> Self {
        Self { upload_id }
    }

    /// Get upload_id from option.
    pub fn upload_id(&self) -> &str {
        &self.upload_id
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
    /// Presign a write multipart operation.
    WriteMultipart(OpWriteMultipart),
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

impl From<OpWriteMultipart> for PresignOperation {
    fn from(v: OpWriteMultipart) -> Self {
        Self::WriteMultipart(v)
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
}

impl OpRead {
    /// Create a default `OpRead` which will read whole content of object.
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
    size: u64,
    content_type: Option<String>,
    content_disposition: Option<String>,
}

impl OpWrite {
    /// Create a new `OpWrite`.
    ///
    /// If input path is not a file path, an error will be returned.
    pub fn new(size: u64) -> Self {
        Self {
            size,
            content_type: None,
            content_disposition: None,
        }
    }

    /// Set the content type of option
    pub fn with_content_type(self, content_type: &str) -> Self {
        Self {
            size: self.size(),
            content_type: Some(content_type.to_string()),
            content_disposition: self.content_disposition,
        }
    }

    /// Set the content disposition of option
    pub fn with_content_disposition(self, content_disposition: &str) -> Self {
        Self {
            size: self.size(),
            content_type: self.content_type,
            content_disposition: Some(content_disposition.to_string()),
        }
    }

    /// Get size from option.
    pub fn size(&self) -> u64 {
        self.size
    }
    /// Get the content type from option
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// Get the content disposition from option
    pub fn content_disposition(&self) -> Option<&str> {
        self.content_disposition.as_deref()
    }
}
