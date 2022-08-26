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

use anyhow::anyhow;

use crate::error::other;
use crate::error::ObjectError;
use crate::ops::Operation;

/// Args for `write` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpWrite {
    path: String,
    size: u64,
}

impl OpWrite {
    /// Create a new `OpWrite`.
    ///
    /// If input path is not a file path, an error will be returned.
    pub fn new(path: &str, size: u64) -> Result<Self> {
        if path.ends_with('/') {
            return Err(other(ObjectError::new(
                Operation::Write,
                path,
                anyhow!("Is a directory"),
            )));
        }

        Ok(Self {
            path: path.to_string(),
            size,
        })
    }

    /// Get path from option.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get size from option.
    pub fn size(&self) -> u64 {
        self.size
    }
}
