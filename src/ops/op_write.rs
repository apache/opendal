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

use mime::Mime;

/// Args for `write` operation.
#[derive(Debug, Clone)]
pub struct OpWrite {
    size: u64,
    content_type: Mime,
}

impl Default for OpWrite {
    fn default() -> Self {
        Self {
            size: 0,
            /// MIME type of content
            ///
            /// # NOTE
            /// > Generic binary data (or binary data whose true type is unknown) is application/octet-stream
            /// --- [MDN Reference](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types)
            content_type: mime::APPLICATION_OCTET_STREAM,
        }
    }
}

impl OpWrite {
    /// Create a new `OpWrite`.
    ///
    /// If input path is not a file path, an error will be returned.
    pub fn new(size: u64) -> Self {
        Self {
            size,
            /// MIME type of content
            ///
            /// # NOTE
            /// > Generic binary data (or binary data whose true type is unknown) is application/octet-stream
            /// --- [MDN Reference](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types)
            content_type: mime::APPLICATION_OCTET_STREAM,
        }
    }

    /// Set the content type of option
    pub fn with_content_type(self, content_type: Mime) -> Self {
        Self {
            size: self.size(),
            content_type,
        }
    }
}

impl OpWrite {
    /// Get size from option.
    pub fn size(&self) -> u64 {
        self.size
    }
    /// Get the content type from option
    pub fn content_type(&self) -> Mime {
        self.content_type.clone()
    }
}
