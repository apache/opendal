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

use crate::raw::*;
use crate::*;

/// Entry is the file/dir entry returned by `Lister`.
#[derive(Clone, Debug)]
pub struct Entry {
    path: String,
    meta: Metadata,
}

impl Entry {
    /// Create an entry.
    ///
    /// # Notes
    ///
    /// This function is crate internal only. Users don't have public
    /// methods to construct an entry. The only way to get an entry
    /// is `Operator::list` or `Operator::scan`.
    pub(crate) fn new(path: String, meta: Metadata) -> Self {
        Self { path, meta }
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

    /// Mode of this entry.
    pub fn mode(&self) -> EntryMode {
        self.meta.mode()
    }

    /// Returns `true` if this entry is for a directory.
    pub fn is_dir(&self) -> bool {
        self.meta.is_dir()
    }

    /// Returns `true` if this entry is for a file.
    pub fn is_file(&self) -> bool {
        self.meta.is_file()
    }

    /// Get the metadata of entry.
    ///
    /// # Notes
    ///
    /// This function is crate internal only. Because the returning
    /// metadata could be incomplete. Users must use `Operator::metadata`
    /// to query the cached metadata instead.
    pub(crate) fn metadata(&self) -> &Metadata {
        &self.meta
    }
}
