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

    /// Create a new OpRead with offset.
    pub fn with_offset(mut self, offset: Option<u64>) -> Self {
        self.br = self.br.with_offset(offset);
        self
    }

    /// Create a new OpRead with offset.
    pub fn with_size(mut self, size: Option<u64>) -> Self {
        self.br = self.br.with_size(size);
        self
    }

    /// Get range from OpRead.
    pub fn range(&self) -> BytesRange {
        self.br
    }
}
