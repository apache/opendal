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

/// Args for `write` operation.
#[derive(Debug, Clone, Default)]
pub struct OpWrite {
    size: u64,
    mime: String,
}

impl OpWrite {
    /// Create a new `OpWrite`.
    ///
    /// If input path is not a file path, an error will be returned.
    pub fn new(size: u64, mime: &str) -> Self {
        Self {
            size,
            mime: mime.to_string(),
        }
    }

    /// Get size from option.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Get MIME type from option.
    pub fn mime_type(&self) -> String {
        self.mime.clone()
    }
}
