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

use std::sync::Arc;

use crate::Accessor;
use crate::Object;
use crate::ObjectMetadata;
use crate::ObjectMode;

/// ObjectEntry is returned by `ObjectStream` or `ObjectIterate` during object list.
///
/// Users can check returning object entry's mode or convert into an object without overhead.
#[derive(Debug, Clone)]
pub struct ObjectEntry {
    path: String,
    meta: ObjectMetadata,
}

impl ObjectEntry {
    /// Create a new object entry by its corresponding underlying storage.
    pub fn new(path: &str, meta: ObjectMetadata) -> ObjectEntry {
        debug_assert!(
            meta.mode().is_dir() == path.ends_with('/'),
            "mode {:?} not match with path {}",
            meta.mode(),
            path
        );

        ObjectEntry {
            path: path.to_string(),
            meta,
        }
    }

    pub fn set_path(&mut self, path: &str) -> &mut Self {
        self.path = path.to_string();
        self
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn mode(&self) -> ObjectMode {
        self.meta.mode()
    }

    pub fn into_object(self, acc: Arc<dyn Accessor>) -> Object {
        Object::new(acc, &self.path).with_metadata(self.meta)
    }
}
