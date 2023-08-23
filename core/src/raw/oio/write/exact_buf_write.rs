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
use std::cmp::min;

/// ExactBufWriter is used to implement [`oio::Write`] based on exact buffer strategy: flush the
/// underlying storage when the buffered size is exactly the same as the buffer size.
///
/// ExactBufWriter makes sure that the size of the data written to the underlying storage is exactly
/// `buffer_size` bytes. It's useful when the underlying storage requires the size to be written.
///
/// For example, R2 requires all parts must be the same size except the last part.
///
/// ## Notes
///
/// ExactBufWriter is not a good choice for most cases, because it will cause more network requests.
pub struct ExactBufWriter<W: oio::Write> {
    inner: W,

    /// The size for buffer, we will flush the underlying storage at the size of this buffer.
    buffer_size: usize,
    buffer: oio::ChunkedCursor,

    buffer_stream: Option<Streamer>,
}

impl<W: oio::Write> ExactBufWriter<W> {
    /// Create a new exact buf writer.
    pub fn new(inner: W, buffer_size: usize) -> Self {
        Self {
            inner,
            buffer_size,
            buffer: oio::ChunkedCursor::new(),
            buffer_stream: None,
        }
    }

    /// Next bytes is used to fetch bytes from buffer or input streamer.
    ///
    /// We need this function because we need to make sure our write is reentrant.
    /// We can't mutate state unless we are sure that the write is successful.
    async fn next_bytes(&mut self, s: &mut Streamer) -> Option<Result<Bytes>> {
        match self.buffer_stream.as_mut() {
            None => s.next().await,
            Some(bs) => match bs.next().await {
                None => {
                    self.buffer_stream = None;
                    s.next().await
                }
                Some(v) => Some(v),
            },
        }
    }

    fn chain_stream(&mut self, s: Streamer) {
        self.buffer_stream = match self.buffer_stream.take() {
            Some(stream) => Some(Box::new(stream.chain(s))),
            None => Some(s),
        }
    }
}

#[async_trait]
impl<W: oio::Write> oio::Write for ExactBufWriter<W> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.sink(bs.len() as u64, Box::new(oio::Cursor::from(bs)))
            .await
    }

    async fn sink(&mut self, size: u64, mut s: Streamer) -> Result<()> {
        // Collect the stream into buffer directly if the buffet is not full.
        if self.buffer.len() as u64 + size < self.buffer_size as u64 {
            self.buffer.push(s.collect().await?);
            return Ok(());
        }

        if self.buffer.len() > self.buffer_size {
            let buf = self.buffer.clone();
            let to_write = self.buffer.split_to(self.buffer_size);
            return self
                .inner
                .sink(to_write.len() as u64, Box::new(to_write))
                .await
                // Replace buffer with remaining if the write is successful.
                .map(|_| {
                    self.buffer = buf;
                    self.chain_stream(s);
                });
        }

        let mut buf = self.buffer.clone();
        while buf.len() < self.buffer_size {
            let bs = self.next_bytes(&mut s).await.transpose()?;
            match bs {
                None => break,
                Some(bs) => buf.push(bs),
            }
        }

        // Return directly if the buffer is not full.
        //
        // We don't need to chain stream here because it must be consumed.
        if buf.len() < self.buffer_size {
            self.buffer = buf;
            return Ok(());
        }

        let to_write = buf.split_to(self.buffer_size);
        self.inner
            .sink(to_write.len() as u64, Box::new(to_write))
            .await
            // Replace buffer with remaining if the write is successful.
            .map(|_| {
                self.buffer = buf;
                self.chain_stream(s);
            })
    }

    async fn abort(&mut self) -> Result<()> {
        self.buffer.clear();
        self.buffer_stream = None;

        self.inner.abort().await
    }

    async fn close(&mut self) -> Result<()> {
        loop {
            if let Some(stream) = self.buffer_stream.as_mut() {
                let bs = stream.next().await.transpose()?;
                match bs {
                    None => {
                        self.buffer_stream = None;
                        break;
                    }
                    Some(bs) => self.buffer.push(bs),
                }
            }

            let mut buf = self.buffer.clone();
            if buf.len() >= self.buffer_size {
                let to_write = buf.split_to(self.buffer_size);
                self.inner
                    .sink(to_write.len() as u64, Box::new(to_write))
                    .await
                    // Replace buffer with remaining if the write is successful.
                    .map(|_| {
                        self.buffer = buf;
                    })?
            }
        }

        while !self.buffer.is_empty() {
            let mut buf = self.buffer.clone();
            let to_write = buf.split_to(min(self.buffer_size, buf.len()));

            self.inner
                .sink(to_write.len() as u64, Box::new(to_write))
                .await
                // Replace buffer with remaining if the write is successful.
                .map(|_| self.buffer = buf)?;
        }

        self.inner.close().await
    }
}
