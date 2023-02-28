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

use bytes::Bytes;

use crate::*;

/// BlockingWriter is a type erased [`BlockingWrite`]
pub type BlockingWriter = Box<dyn BlockingWrite>;

/// BlockingWrite is the trait that OpenDAL returns to callers.
pub trait BlockingWrite: Send + Sync + 'static {
    /// Write whole content at once.
    ///
    /// We consume the writer here to indicate that users should
    /// write all content. To append multiple bytes together, use
    /// `append` instead.
    fn write(&mut self, bs: Bytes) -> Result<()>;

    /// Close the writer and make sure all data has been flushed.
    fn close(&mut self) -> Result<()>;
}

impl BlockingWrite for () {
    fn write(&mut self, bs: Bytes) -> Result<()> {
        let _ = bs;

        unimplemented!("write is required to be implemented for output::BlockingWrite")
    }

    fn close(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support close",
        ))
    }
}

/// `Box<dyn BlockingWrite>` won't implement `BlockingWrite` automanticly.
/// To make BlockingWriter work as expected, we must add this impl.
impl<T: BlockingWrite + ?Sized> BlockingWrite for Box<T> {
    fn write(&mut self, bs: Bytes) -> Result<()> {
        (**self).write(bs)
    }

    fn close(&mut self) -> Result<()> {
        (**self).close()
    }
}
