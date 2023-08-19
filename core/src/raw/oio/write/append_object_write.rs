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
use bytes::Bytes;

use crate::raw::oio::Streamer;
use crate::raw::*;
use crate::*;

const DEFAULT_WRITE_MIN_SIZE: usize = 8 * 1024 * 1024;

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
    buffer: oio::VectorCursor,
    buffer_size: usize,
}

impl<W: AppendObjectWrite> AppendObjectWriter<W> {
    /// Create a new AppendObjectWriter.
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            offset: None,
            buffer: oio::VectorCursor::new(),
            buffer_size: DEFAULT_WRITE_MIN_SIZE,
        }
    }

    /// Configure the write_min_size.
    ///
    /// write_min_size is used to control the size of internal buffer.
    ///
    /// AppendObjectWriter will flush the buffer to storage when
    /// the size of buffer is larger than write_min_size.
    ///
    /// This value is default to 8 MiB.
    pub fn with_write_min_size(mut self, v: usize) -> Self {
        self.buffer_size = v;
        self
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
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let offset = self.offset().await?;

        // Ignore empty bytes
        if bs.is_empty() {
            return Ok(());
        }

        self.buffer.push(bs);
        // Return directly if the buffer is not full
        if self.buffer.len() <= self.buffer_size {
            return Ok(());
        }

        let bs = self.buffer.peak_all();
        let size = bs.len();

        match self
            .inner
            .append(offset, size as u64, AsyncBody::Bytes(bs))
            .await
        {
            Ok(_) => {
                self.buffer.take(size);
                self.offset = Some(offset + size as u64);
                Ok(())
            }
            Err(e) => {
                // If the upload fails, we should pop the given bs to make sure
                // write is re-enter safe.
                self.buffer.pop();
                Err(e)
            }
        }
    }

    async fn sink(&mut self, size: u64, s: Streamer) -> Result<()> {
        if !self.buffer.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Writer::sink should not be used mixed with existing buffer",
            ));
        }

        let offset = self.offset().await?;

        self.inner
            .append(offset, size, AsyncBody::Stream(s))
            .await?;
        self.offset = Some(offset + size);

        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
