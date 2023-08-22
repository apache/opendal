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

use crate::raw::oio::{StreamExt, Streamer};
use crate::raw::*;
use crate::*;
use async_trait::async_trait;
use bytes::Bytes;

/// AtLeastBufWrite is used to implement [`Write`] based on at least buffer.
///
/// Users can wrap a writer and a buffer together.
pub struct AtLeastBufWriter<W: oio::Write> {
    inner: W,

    size: usize,
    buf: oio::ChunkedCursor,
}

impl<W: oio::Write> AtLeastBufWriter<W> {
    /// Create a new at least buf writer.
    pub fn new(inner: W, size: usize) -> Self {
        Self {
            inner,
            size,
            buf: oio::ChunkedCursor::new(),
        }
    }
}

#[async_trait]
impl<W: oio::Write> oio::Write for AtLeastBufWriter<W> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        // Push the bytes into the buffer if the buffer is not full.
        if self.buf.len() + bs.len() <= self.size {
            self.buf.push(bs);
            return Ok(());
        }

        let mut buf = self.buf.clone();
        buf.push(bs);

        self.inner
            .sink(buf.len() as u64, Box::new(buf))
            .await
            // Clear buffer if the write is successful.
            .map(|_| self.buf.clear())
    }

    async fn sink(&mut self, size: u64, s: Streamer) -> Result<()> {
        // Push the bytes into the buffer if the buffer is not full.
        if self.buf.len() as u64 + size <= self.size as u64 {
            self.buf.push(s.collect().await?);
            return Ok(());
        }

        let buf = self.buf.clone();
        let buffer_size = buf.len() as u64;
        let stream = buf.chain(s);

        self.inner
            .sink(buffer_size + size, Box::new(stream))
            .await
            // Clear buffer if the write is successful.
            .map(|_| self.buf.clear())
    }

    async fn abort(&mut self) -> Result<()> {
        self.buf.clear();
        self.inner.abort().await
    }

    async fn close(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.inner
                .sink(self.buf.len() as u64, Box::new(self.buf.clone()))
                .await?;
            self.buf.clear();
        }

        self.inner.close().await?;

        Ok(())
    }
}
