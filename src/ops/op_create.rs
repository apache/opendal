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
use crate::ObjectMode;

/// Args for `create` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpCreate {
    path: String,
    mode: ObjectMode,
}

impl OpCreate {
    /// Create a new `OpCreate`.
    ///
    /// If input path is not match with object mode, an error will be returned.
    pub fn new(path: &str, mode: ObjectMode) -> Result<Self> {
        match mode {
            ObjectMode::FILE => {
                if path.ends_with('/') {
                    return Err(other(ObjectError::new(
                        "create",
                        path,
                        anyhow!("Is a directory"),
                    )));
                }
                Ok(Self {
                    path: path.to_string(),
                    mode,
                })
            }
            ObjectMode::DIR => {
                if !path.ends_with('/') {
                    return Err(other(ObjectError::new(
                        "create",
                        path,
                        anyhow!("Not a directory"),
                    )));
                }

                Ok(Self {
                    path: path.to_string(),
                    mode,
                })
            }
            ObjectMode::Unknown => Err(other(ObjectError::new(
                "create",
                path,
                anyhow!("create unknown object mode is not supported"),
            ))),
        }
    }

    /// Get path from option.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get object mode from option.
    pub fn mode(&self) -> ObjectMode {
        self.mode
    }
}
