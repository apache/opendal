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

/// Entry returned by [`Lister`] or [`BlockingLister`] to represent a path and it's relative metadata.
#[derive(Clone, Debug)]
pub struct Entry {
    /// Path of this entry.
    path: String,

    /// Metadata of this entry.
    metadata: Metadata,
}

impl Entry {
    /// Create an entry with metadata.
    ///
    /// # Notes
    ///
    /// The only way to get an entry with associated cached metadata
    /// is `Operator::list`.
    pub(crate) fn new(path: String, metadata: Metadata) -> Self {
        Self { path, metadata }
    }

    /// Path of entry. Path is relative to operator's root.
    ///
    /// Only valid in current operator.
    ///
    /// If this entry is a dir, `path` MUST end with `/`
    /// Otherwise, `path` MUST NOT end with `/`.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Name of entry. Name is the last segment of path.
    ///
    /// If this entry is a dir, `name` MUST end with `/`
    /// Otherwise, `name` MUST NOT end with `/`.
    pub fn name(&self) -> &str {
        get_basename(&self.path)
    }

    /// Fetch metadata of this entry.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Consume this entry to get its path and metadata.
    pub fn into_parts(self) -> (String, Metadata) {
        (self.path, self.metadata)
    }
}
