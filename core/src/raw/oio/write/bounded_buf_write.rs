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

use std::cmp::min;

use async_trait::async_trait;
use bytes::Bytes;

use crate::raw::*;
use crate::*;

/// BoundedBufWriter is used to implement [`oio::Write`] based on bounded buffer strategy: flush the
/// underlying storage when the buffered size is between the range of [min_buffer..max_buffer]
pub struct BoundedBufWriter<W: oio::Write> {
    inner: W,

    min_buffer: usize,
    max_buffer: usize,
    buffer: oio::ChunkedCursor,
}

impl<W: oio::Write> BoundedBufWriter<W> {
    /// Create a new exact buf writer.
    pub fn new(inner: W, min_buffer: usize) -> Self {
        Self {
            inner,
            min_buffer,
            max_buffer: usize::MAX,
            buffer: oio::ChunkedCursor::new(),
        }
    }

    /// Configure the max buffer size for writer.
    ///
    /// # Panics
    ///
    /// Panic if max_buffer is smaller than min_buffer.
    pub fn with_max_buffer(mut self, max_buffer: usize) -> Self {
        assert!(
            max_buffer >= self.min_buffer,
            "input max buffer is smaller than min buffer"
        );

        self.max_buffer = max_buffer;
        self
    }
}

#[async_trait]
impl<W: oio::Write> oio::Write for BoundedBufWriter<W> {
    /// # TODO
    ///
    /// - Use copy_from_slice if given bytes is smaller than 4KiB.
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        // Make sure the existing buffer has been flushed.
        while self.buffer.len() >= self.max_buffer {
            let mut buf = self.buffer.clone();
            let to_write = buf.split_to(self.max_buffer);

            if let Some(bs) = to_write.try_single() {
                self.inner.write(bs).await?;
            } else {
                self.inner
                    .sink(to_write.len() as u64, Box::new(to_write))
                    .await?;
            }
            // input bytes is not handled yet, go on.
        }

        let current_size = self.buffer.len() + bs.len();

        if current_size >= self.max_buffer {
            let mut buf = self.buffer.clone();
            buf.push(bs);

            let to_write = buf.split_to(self.max_buffer);

            if let Some(bs) = to_write.try_single() {
                self.inner.write(bs).await?;
            } else {
                self.inner
                    .sink(to_write.len() as u64, Box::new(to_write))
                    .await?;
            }
            // Replace buffer since there are bytes not consumed.
            self.buffer = buf;
            return Ok(());
        }

        if current_size >= self.min_buffer {
            let mut buf = self.buffer.clone();
            buf.push(bs);

            if let Some(bs) = buf.try_single() {
                self.inner.write(bs).await?;
            } else {
                self.inner.sink(buf.len() as u64, Box::new(buf)).await?;
            }
            // Clean buffer, since it has been consumed all.
            self.buffer.clear();
            return Ok(());
        }

        // Push the bytes into the buffer since the buffer is not full.
        self.buffer.push(bs);
        Ok(())
    }

    /// Sink will always bypass the buffer logic.
    ///
    /// `CompleteLayer` will make sure that users can't mix `write` and `sink` together.
    async fn sink(&mut self, size: u64, s: oio::Streamer) -> Result<()> {
        self.inner.sink(size, s).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.buffer.clear();
        self.inner.abort().await
    }

    async fn close(&mut self) -> Result<()> {
        while !self.buffer.is_empty() {
            let mut buf = self.buffer.clone();
            let to_write = buf.split_to(min(self.max_buffer, buf.len()));

            if let Some(bs) = to_write.try_single() {
                self.inner.write(bs).await?;
            } else {
                self.inner
                    .sink(to_write.len() as u64, Box::new(to_write))
                    .await?;
            }
            self.buffer = buf;
        }

        self.inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use log::debug;
    use pretty_assertions::assert_eq;
    use rand::thread_rng;
    use rand::Rng;
    use rand::RngCore;
    use sha2::Digest;
    use sha2::Sha256;

    use super::*;
    use crate::raw::oio::StreamExt;
    use crate::raw::oio::Write;

    struct MockWriter {
        buf: Vec<u8>,
    }

    #[async_trait]
    impl Write for MockWriter {
        async fn write(&mut self, bs: Bytes) -> Result<()> {
            debug!("test_fuzz_exact_buf_writer: flush size: {}", bs.len());

            self.buf.extend_from_slice(&bs);
            Ok(())
        }

        async fn sink(&mut self, size: u64, s: oio::Streamer) -> Result<()> {
            let bs = s.collect().await?;
            assert_eq!(bs.len() as u64, size);
            self.write(bs).await
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_exact_buf_writer_short_write() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .pretty()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        let mut rng = thread_rng();
        let mut expected = vec![0; 5];
        rng.fill_bytes(&mut expected);

        let mut w = BoundedBufWriter::new(MockWriter { buf: vec![] }, 10);

        w.write(Bytes::from(expected.clone())).await?;
        w.close().await?;

        assert_eq!(w.inner.buf.len(), expected.len());
        assert_eq!(
            format!("{:x}", Sha256::digest(&w.inner.buf)),
            format!("{:x}", Sha256::digest(&expected))
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_fuzz_exact_buf_writer() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .pretty()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        let mut rng = thread_rng();
        let mut expected = vec![];

        let buffer_size = rng.gen_range(1..10);
        let mut writer = BoundedBufWriter::new(MockWriter { buf: vec![] }, buffer_size);
        debug!("test_fuzz_exact_buf_writer: buffer size: {buffer_size}");

        for _ in 0..1000 {
            let size = rng.gen_range(1..20);
            debug!("test_fuzz_exact_buf_writer: write size: {size}");
            let mut content = vec![0; size];
            rng.fill_bytes(&mut content);

            expected.extend_from_slice(&content);
            writer.write(Bytes::from(content)).await?;
        }
        writer.close().await?;

        assert_eq!(writer.inner.buf.len(), expected.len());
        assert_eq!(
            format!("{:x}", Sha256::digest(&writer.inner.buf)),
            format!("{:x}", Sha256::digest(&expected))
        );
        Ok(())
    }
}
