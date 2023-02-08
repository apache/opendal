// Copyright 2022 Datafuse Labs.
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
pub struct OpList {}

impl OpList {
    /// Create a new `OpList`.
    pub fn new() -> Self {
        Self {}
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
}

impl OpWrite {
    /// Create a new `OpWrite`.
    ///
    /// If input path is not a file path, an error will be returned.
    pub fn new(size: u64) -> Self {
        Self {
            size,
            content_type: None,
        }
    }

    /// Set the content type of option
    pub fn with_content_type(self, content_type: &str) -> Self {
        Self {
            size: self.size(),
            content_type: Some(content_type.to_string()),
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
}
