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

use crate::raw::*;
use crate::*;

/// ChunkedWriter is used to implement [`oio::Write`] based on chunk:
/// flush the underlying storage at the `chunk`` size.
///
/// ChunkedWriter makes sure that the size of the data written to the
/// underlying storage is
/// - exactly `chunk` bytes if `exact` is true
/// - at least `chunk` bytes if `exact` is false
pub struct ChunkedWriter<W: oio::Write> {
    inner: W,

    /// The size for buffer, we will flush the underlying storage at the size of this buffer.
    chunk_size: usize,
    /// If `exact` is true, the size of the data written to the underlying storage is
    /// exactly `chunk_size` bytes.
    exact: bool,
    buffer: oio::QueueBuf,
}

impl<W: oio::Write> ChunkedWriter<W> {
    /// Create a new exact buf writer.
    pub fn new(inner: W, chunk_size: usize, exact: bool) -> Self {
        Self {
            inner,
            chunk_size,
            exact,
            buffer: oio::QueueBuf::new(),
        }
    }
}

impl<W: oio::Write> oio::Write for ChunkedWriter<W> {
    async fn write(&mut self, mut bs: Buffer) -> Result<usize> {
        if self.buffer.len() >= self.chunk_size {
            let written = self.inner.write(self.buffer.clone().collect()).await?;
            self.buffer.advance(written);
        }

        if !self.exact && bs.len() >= self.chunk_size && self.buffer.is_empty() {
            // Sends the buffer directly if the buffer queue is empty and we are in
            // inexact mode.
            return self.inner.write(bs.collect()).await;
        }

        let remaining = self.chunk_size - self.buffer.len();
        bs.truncate(remaining);
        let n = bs.len();
        self.buffer.push(bs);
        Ok(n)
    }

    async fn close(&mut self) -> Result<()> {
        loop {
            if self.buffer.is_empty() {
                break;
            }

            let written = self.inner.write(self.buffer.clone().collect()).await?;
            self.buffer.advance(written);
        }

        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.buffer.clear();
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use bytes::Bytes;
    use log::debug;
    use pretty_assertions::assert_eq;
    use rand::thread_rng;
    use rand::Rng;
    use rand::RngCore;
    use sha2::Digest;
    use sha2::Sha256;

    use super::*;
    use crate::raw::oio::Write;

    struct MockWriter {
        buf: Vec<u8>,
    }

    impl Write for MockWriter {
        async fn write(&mut self, bs: Buffer) -> Result<usize> {
            debug!("test_fuzz_exact_buf_writer: flush size: {}", &bs.len());

            let chunk = bs.chunk();
            self.buf.extend_from_slice(chunk);
            Ok(chunk.len())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }

        async fn abort(&mut self) -> Result<()> {
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

        let mut w = ChunkedWriter::new(MockWriter { buf: vec![] }, 10, true);

        let mut bs = Bytes::from(expected.clone());
        while !bs.is_empty() {
            let n = w.write(bs.clone().into()).await?;
            bs.advance(n);
        }

        w.close().await?;

        assert_eq!(w.inner.buf.len(), expected.len());
        assert_eq!(
            format!("{:x}", Sha256::digest(&w.inner.buf)),
            format!("{:x}", Sha256::digest(&expected))
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_inexact_buf_writer_large_write() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .pretty()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        let mut w = ChunkedWriter::new(MockWriter { buf: vec![] }, 10, false);

        let mut rng = thread_rng();
        let mut expected = vec![0; 15];
        rng.fill_bytes(&mut expected);

        let bs = Bytes::from(expected.clone());
        // The MockWriter always returns the first chunk size.
        let n = w.write(bs.into()).await?;
        assert_eq!(expected.len(), n);

        w.close().await?;

        assert_eq!(w.inner.buf.len(), expected.len());
        assert_eq!(
            format!("{:x}", Sha256::digest(&w.inner.buf)),
            format!("{:x}", Sha256::digest(&expected))
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_inexact_buf_writer_mix_write() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .pretty()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        let mut w = ChunkedWriter::new(MockWriter { buf: vec![] }, 10, false);

        let mut rng = thread_rng();
        let mut expected = vec![];

        let mut new_content = |size| {
            let mut content = vec![0; size];
            rng.fill_bytes(&mut content);
            expected.extend_from_slice(&content);
            Bytes::from(content)
        };

        // content > chunk size.
        let content = new_content(15);
        assert_eq!(15, w.write(content.into()).await?);
        // content < chunk size.
        let content = new_content(5);
        assert_eq!(5, w.write(content.into()).await?);
        // content > chunk size.
        let mut content = new_content(15);
        while !content.is_empty() {
            assert_eq!(5, w.write(content.clone().into()).await?);
            content.advance(5);
        }

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
        let mut writer = ChunkedWriter::new(MockWriter { buf: vec![] }, buffer_size, true);
        debug!("test_fuzz_exact_buf_writer: buffer size: {buffer_size}");

        for _ in 0..1000 {
            let size = rng.gen_range(1..20);
            debug!("test_fuzz_exact_buf_writer: write size: {size}");
            let mut content = vec![0; size];
            rng.fill_bytes(&mut content);

            expected.extend_from_slice(&content);

            let mut bs = Bytes::from(content.clone());
            while !bs.is_empty() {
                let n = writer.write(bs.clone().into()).await?;
                bs.advance(n);
            }
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
