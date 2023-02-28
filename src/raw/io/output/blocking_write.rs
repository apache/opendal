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

use std::io::Result;

use bytes::Bytes;

/// BlockingWriter is a type erased [`BlockingWrite`]
pub type BlockingWriter = Box<dyn BlockingWrite>;

/// BlockingWrite is the trait that OpenDAL returns to callers.
pub trait BlockingWrite: Send + Sync + 'static {
    /// Write whole content at once.
    ///
    /// We consume the writer here to indicate that users should
    /// write all content. To append multiple bytes together, use
    /// `append` instead.
    fn write(self, bs: Bytes) -> Result<()>;
}

impl BlockingWrite for () {
    fn write(self, bs: Bytes) -> Result<()> {
        let _ = bs;

        unimplemented!("write is required to be implemented for output::BlockingWrite")
    }
}
