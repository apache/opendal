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

use crate::*;

use super::BytesRange;

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

/// Reply for `read` operation.
#[derive(Debug, Clone)]
pub struct RpRead {
    meta: ObjectMetadata,
}

impl RpRead {
    /// Create a new reply.
    pub fn new(content_length: u64) -> Self {
        RpRead {
            meta: ObjectMetadata::new(ObjectMode::FILE).with_content_length(content_length),
        }
    }

    /// Create reply read with existing object metadata.
    pub fn with_metadata(meta: ObjectMetadata) -> Self {
        RpRead { meta }
    }

    /// Consume reply to get the object meta.
    pub fn into_metadata(self) -> ObjectMetadata {
        self.meta
    }
}
