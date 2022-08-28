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

use std::io::Result;
use std::ops::RangeBounds;

use anyhow::anyhow;

use super::BytesRange;
use crate::error::other;
use crate::error::ObjectError;
use crate::ops::Operation;

/// Args for `read` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpRead {
    path: String,
    offset: Option<u64>,
    size: Option<u64>,
}

impl OpRead {
    /// Create a new `OpRead`.
    ///
    /// If input path is not a file path, an error will be returned.
    pub fn new(path: &str, range: impl RangeBounds<u64>) -> Result<Self> {
        if path.ends_with('/') {
            return Err(other(ObjectError::new(
                Operation::Read,
                path,
                anyhow!("Is a directory"),
            )));
        }

        let br = BytesRange::from(range);

        Ok(Self {
            path: path.to_string(),
            offset: br.offset(),
            size: br.size(),
        })
    }

    pub(crate) fn new_with_offset(
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<Self> {
        if path.ends_with('/') {
            return Err(other(ObjectError::new(
                Operation::Read,
                path,
                anyhow!("Is a directory"),
            )));
        }

        Ok(Self {
            path: path.to_string(),
            offset,
            size,
        })
    }

    /// Get path from option.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get offset from option.
    pub fn offset(&self) -> Option<u64> {
        self.offset
    }

    /// Get size from option.
    pub fn size(&self) -> Option<u64> {
        self.size
    }
}
