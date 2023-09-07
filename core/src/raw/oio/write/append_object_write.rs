// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use async_trait::async_trait;

use crate::raw::*;
use crate::*;

/// AppendObjectWrite is used to implement [`Write`] based on append
/// object. By implementing AppendObjectWrite, services don't need to
/// care about the details of buffering and uploading parts.
///
/// The layout after adopting [`AppendObjectWrite`]:
///
/// - Services impl `AppendObjectWrite`
/// - `AppendObjectWriter` impl `Write`
/// - Expose `AppendObjectWriter` as `Accessor::Writer`
#[async_trait]
pub trait AppendObjectWrite: Send + Sync + Unpin {
    /// Get the current offset of the append object.
    ///
    /// Returns `0` if the object is not exist.
    async fn offset(&self) -> Result<u64>;

    /// Append the data to the end of this object.
    async fn append(&self, offset: u64, size: u64, body: AsyncBody) -> Result<()>;
}

/// AppendObjectWriter will implements [`Write`] based on append object.
///
/// ## TODO
///
/// - Allow users to switch to un-buffered mode if users write 16MiB every time.
pub struct AppendObjectWriter<W: AppendObjectWrite> {
    inner: W,

    offset: Option<u64>,
}

impl<W: AppendObjectWrite> AppendObjectWriter<W> {
    /// Create a new AppendObjectWriter.
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            offset: None,
        }
    }

    async fn offset(&mut self) -> Result<u64> {
        if let Some(offset) = self.offset {
            return Ok(offset);
        }

        let offset = self.inner.offset().await?;
        self.offset = Some(offset);

        Ok(offset)
    }
}

#[async_trait]
impl<W> oio::Write for AppendObjectWriter<W>
where
    W: AppendObjectWrite,
{
    async fn write(&mut self, bs: &dyn Buf) -> Result<usize> {
        let offset = self.offset().await?;

        let size = bs.remaining();

        self.inner
            .append(
                offset,
                size as u64,
                AsyncBody::Bytes(bs.copy_to_bytes(size)),
            )
            .await
            .map(|_| self.offset = Some(offset + size as u64))?;

        Ok(size)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}
