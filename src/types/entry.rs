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

use crate::raw::*;
use crate::*;

/// Entry is the file/dir entry returned by `Lister`.
#[derive(Clone, Debug)]
pub struct Entry {
    /// Path of the entry.
    path: String,

    /// Optional cached metadata
    metadata: Option<Metadata>,
}

impl Entry {
    /// Create an entry with .
    ///
    /// # Notes
    ///
    /// This function is crate internal only. Users don't have public
    /// methods to construct an entry with arbitrary cached metadata.
    ///
    /// The only way to get an entry with associated cached metadata
    /// is `Operator::list` or `Operator::scan`.
    pub(crate) fn new_with(path: String, metadata: Metadata) -> Self {
        Self {
            path,
            metadata: Some(metadata),
        }
    }

    /// Create an [`Entry`] with empty cached metadata.
    pub fn new(path: &str) -> Self {
        Self {
            path: normalize_path(path),
            metadata: None,
        }
    }

    /// Path of entry. Path is relative to operator's root.
    /// Only valid in current operator.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Name of entry. Name is the last segment of path.
    ///
    /// If this entry is a dir, `Name` MUST endswith `/`
    /// Otherwise, `Name` MUST NOT endswith `/`.
    pub fn name(&self) -> &str {
        get_basename(&self.path)
    }

    /// Get the cached metadata of entry.
    ///
    /// # Notes
    ///
    /// This function is crate internal only. Because the returning
    /// metadata could be incomplete. Users must use `Operator::metadata`
    /// to query the cached metadata instead.
    pub(crate) fn metadata(&self) -> &Option<Metadata> {
        &self.metadata
    }
}
