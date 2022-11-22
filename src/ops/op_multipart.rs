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

use crate::ObjectPart;

/// Args for `create_multipart` operation.
#[derive(Debug, Clone, Default)]
pub struct OpCreateMultipart {}

impl OpCreateMultipart {
    /// Create a new `OpCreateMultipart`.
    pub fn new() -> Self {
        Self {}
    }
}

/// Reply for `create_multipart` operation.
#[derive(Debug, Clone, Default)]
pub struct RpCreateMultipart {
    upload_id: String,
}

impl RpCreateMultipart {
    /// Create a new reply for create_multipart.
    pub fn new(upload_id: &str) -> Self {
        Self {
            upload_id: upload_id.to_string(),
        }
    }

    /// Get the upload_id.
    pub fn upload_id(&self) -> &str {
        &self.upload_id
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
