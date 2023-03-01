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

use crate::Object;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::Operator;

/// Entry is returned by `Page` or `BlockingPage`
/// during list operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    path: String,
    meta: ObjectMetadata,
}

impl Entry {
    /// Create a new object entry by its corresponding underlying storage.
    pub fn new(path: &str, meta: ObjectMetadata) -> Entry {
        Self::with(path.to_string(), meta)
    }

    /// Create a new object entry with given value.
    pub fn with(path: String, meta: ObjectMetadata) -> Entry {
        debug_assert!(
            meta.mode().is_dir() == path.ends_with('/'),
            "mode {:?} not match with path {}",
            meta.mode(),
            path
        );

        Entry { path, meta }
    }

    /// Set path for object entry.
    pub fn set_path(&mut self, path: &str) -> &mut Self {
        self.path = path.to_string();
        self
    }

    /// Get the path of object entry.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Set mode for object entry.
    ///
    /// # Note
    ///
    /// Please use this function carefully.
    pub fn set_mode(&mut self, mode: ObjectMode) -> &mut Self {
        self.meta.set_mode(mode);
        self
    }

    /// Get entry's object mode.
    pub fn mode(&self) -> ObjectMode {
        self.meta.mode()
    }

    /// Consume to convert into an object.
    pub fn into_object(self, op: Operator) -> Object {
        Object::with(op, &self.path, Some(self.meta))
    }
}
