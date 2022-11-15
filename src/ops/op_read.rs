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

use std::ops::RangeBounds;

use super::BytesRange;

/// Args for `read` operation.
#[derive(Debug, Clone, Default)]
pub struct OpRead {
    br: BytesRange,
}

impl OpRead {
    /// Create a new `OpRead`.
    ///
    /// If input path is not a file path, an error will be returned.
    pub fn new(range: impl RangeBounds<u64>) -> Self {
        Self { br: range.into() }
    }

    /// Get range from OpRead.
    pub fn range(&self) -> BytesRange {
        self.br
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
}
