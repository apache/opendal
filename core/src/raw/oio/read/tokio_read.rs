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

use std::io::SeekFrom;

use bytes::Bytes;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeek;
use tokio::io::AsyncSeekExt;
use tokio::io::ReadBuf;

use crate::raw::*;
use crate::*;

/// FuturesReader implements [`oio::Read`] via [`AsyncRead`] + [`AsyncSeek`].
pub struct TokioReader<R: AsyncRead + AsyncSeek> {
    inner: R,
    buf: Vec<u8>,
}

impl<R: AsyncRead + AsyncSeek> TokioReader<R> {
    /// Create a new tokio reader.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(64 * 1024),
        }
    }
}

impl<R> oio::Read for TokioReader<R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send + Sync,
{
    async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.inner.seek(pos).await.map_err(|err| {
            new_std_io_error(err)
                .with_operation(oio::ReadOperation::Seek)
                .with_context("source", "TokioReader")
        })
    }

    async fn read(&mut self, size: usize) -> Result<Bytes> {
        // Make sure buf has enough space.
        if self.buf.capacity() < size {
            self.buf.reserve(size - self.buf.capacity());
        }
        let buf = self.buf.spare_capacity_mut();
        let mut read_buf: ReadBuf = ReadBuf::uninit(buf);

        // SAFETY: Read at most `size` bytes into `read_buf`.
        unsafe {
            read_buf.assume_init(size);
        }

        let n = self
            .inner
            .read(read_buf.initialize_unfilled())
            .await
            .map_err(|err| {
                new_std_io_error(err)
                    .with_operation(oio::ReadOperation::Read)
                    .with_context("source", "FuturesReader")
            })?;
        read_buf.set_filled(n);

        Ok(Bytes::copy_from_slice(read_buf.filled()))
    }
}
