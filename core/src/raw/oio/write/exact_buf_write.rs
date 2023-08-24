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

use crate::raw::oio::StreamExt;
use crate::raw::oio::Streamer;
use crate::raw::*;
use crate::*;

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

    /// # TODO
    ///
    /// We know every stream size, we can collect them into a buffer without chain them every time.
    async fn sink(&mut self, _: u64, mut s: Streamer) -> Result<()> {
        if self.buffer.len() >= self.buffer_size {
            let mut buf = self.buffer.clone();
            let to_write = buf.split_to(self.buffer_size);
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
        while let Some(stream) = self.buffer_stream.as_mut() {
            let bs = stream.next().await.transpose()?;
            match bs {
                None => {
                    self.buffer_stream = None;
                }
                Some(bs) => {
                    self.buffer.push(bs);
                }
            }

            if self.buffer.len() >= self.buffer_size {
                let mut buf = self.buffer.clone();
                let to_write = buf.split_to(self.buffer_size);
                self.inner
                    .sink(to_write.len() as u64, Box::new(to_write))
                    .await
                    // Replace buffer with remaining if the write is successful.
                    .map(|_| {
                        self.buffer = buf;
                    })?;
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

        async fn sink(&mut self, size: u64, s: Streamer) -> Result<()> {
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

        let mut w = ExactBufWriter::new(MockWriter { buf: vec![] }, 10);

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
        let mut writer = ExactBufWriter::new(MockWriter { buf: vec![] }, buffer_size);
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
