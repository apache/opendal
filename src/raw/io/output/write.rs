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

use async_trait::async_trait;
use bytes::Bytes;

use crate::*;

/// Writer is a type erased [`Write`]
pub type Writer = Box<dyn Write>;

/// Write is the trait that OpenDAL returns to callers.
#[async_trait]
pub trait Write: Unpin + Send + Sync {
    /// Write whole content at once.
    ///
    /// To append multiple bytes together, use `append` instead.
    async fn write(&mut self, bs: Bytes) -> Result<()>;

    /// Append bytes to the writer.
    ///
    /// It is highly recommended to align the length of the input bytes
    /// into blocks of 4MiB (except the last block) for better performance
    /// and compatibility.
    async fn append(&mut self, bs: Bytes) -> Result<()>;

    /// Close the writer and make sure all data has been flushed.
    async fn close(&mut self) -> Result<()>;
}

#[async_trait]
impl Write for () {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let _ = bs;

        unimplemented!("write is required to be implemented for output::Write")
    }

    async fn append(&mut self, bs: Bytes) -> Result<()> {
        let _ = bs;

        Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support append",
        ))
    }

    async fn close(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support close",
        ))
    }
}

/// `Box<dyn Write>` won't implement `Write` automanticly. To make Writer
/// work as expected, we must add this impl.
#[async_trait]
impl<T: Write + ?Sized> Write for Box<T> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        (**self).write(bs).await
    }

    async fn append(&mut self, bs: Bytes) -> Result<()> {
        (**self).append(bs).await
    }

    async fn close(&mut self) -> Result<()> {
        (**self).close().await
    }
}
