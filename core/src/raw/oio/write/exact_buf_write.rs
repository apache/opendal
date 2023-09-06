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
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::ReadBuf;

use crate::raw::oio::ReadExt;
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
    buffer: Buffer,
}

impl<W: oio::Write> ExactBufWriter<W> {
    /// Create a new exact buf writer.
    pub fn new(inner: W, buffer_size: usize) -> Self {
        Self {
            inner,
            buffer_size,
            buffer: Buffer::Filling(BytesMut::new()),
        }
    }
}

enum Buffer {
    Filling(BytesMut),
    Consuming(Bytes),
}

#[async_trait]
impl<W: oio::Write> oio::Write for ExactBufWriter<W> {
    async fn write(&mut self, mut bs: Bytes) -> Result<u64> {
        loop {
            match &mut self.buffer {
                Buffer::Filling(fill) => {
                    if fill.len() >= self.buffer_size {
                        self.buffer = Buffer::Consuming(fill.split().freeze());
                        continue;
                    }

                    let size = min(self.buffer_size - fill.len(), bs.len());
                    fill.extend_from_slice(&bs[..size]);
                    bs.advance(size);
                    return Ok(size as u64);
                }
                Buffer::Consuming(consume) => {
                    // Make sure filled buffer has been flushed.
                    //
                    // TODO: maybe we can re-fill it after a successful write.
                    while !consume.is_empty() {
                        let n = self.inner.write(consume.clone()).await?;
                        consume.advance(n as usize);
                    }
                    self.buffer = Buffer::Filling(BytesMut::new());
                }
            }
        }
    }

    async fn copy_from(&mut self, _: u64, mut s: oio::Reader) -> Result<u64> {
        loop {
            match &mut self.buffer {
                Buffer::Filling(fill) => {
                    if fill.len() >= self.buffer_size {
                        self.buffer = Buffer::Consuming(fill.split().freeze());
                        continue;
                    }

                    // Reserve to buffer size.
                    fill.reserve(self.buffer_size - fill.len());
                    let dst = fill.spare_capacity_mut();
                    let dst_len = dst.len();
                    let mut buf = ReadBuf::uninit(dst);

                    // Safety: the input buffer is created with_capacity(length).
                    unsafe { buf.assume_init(dst_len) };

                    let n = s.read(buf.initialize_unfilled()).await?;

                    // Safety: read makes sure this buffer has been filled.
                    unsafe { fill.advance_mut(n) };

                    return Ok(n as u64);
                }
                Buffer::Consuming(consume) => {
                    // Make sure filled buffer has been flushed.
                    //
                    // TODO: maybe we can re-fill it after a successful write.
                    while !consume.is_empty() {
                        let n = self.inner.write(consume.clone()).await?;
                        consume.advance(n as usize);
                    }
                    self.buffer = Buffer::Filling(BytesMut::new());
                }
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        self.buffer = Buffer::Filling(BytesMut::new());
        self.inner.abort().await
    }

    async fn close(&mut self) -> Result<()> {
        loop {
            match &mut self.buffer {
                Buffer::Filling(fill) => {
                    self.buffer = Buffer::Consuming(fill.split().freeze());
                    continue;
                }
                Buffer::Consuming(consume) => {
                    // Make sure filled buffer has been flushed.
                    //
                    // TODO: maybe we can re-fill it after a successful write.
                    while !consume.is_empty() {
                        let n = self.inner.write(consume.clone()).await?;
                        consume.advance(n as usize);
                    }
                    break;
                }
            }
        }

        self.inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use futures::AsyncReadExt;
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

    #[async_trait]
    impl Write for MockWriter {
        async fn write(&mut self, bs: Bytes) -> Result<u64> {
            debug!("test_fuzz_exact_buf_writer: flush size: {}", bs.len());

            self.buf.extend_from_slice(&bs);
            Ok(bs.len() as u64)
        }

        async fn copy_from(&mut self, size: u64, mut s: oio::Reader) -> Result<u64> {
            let mut bs = vec![];
            s.read_to_end(&mut bs).await.unwrap();
            assert_eq!(bs.len() as u64, size);
            self.write(bs.into()).await
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

        let mut bs = Bytes::from(expected.clone());
        while !bs.is_empty() {
            let n = w.write(bs.clone()).await?;
            bs.advance(n as usize);
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
        let mut writer = ExactBufWriter::new(MockWriter { buf: vec![] }, buffer_size);
        debug!("test_fuzz_exact_buf_writer: buffer size: {buffer_size}");

        for _ in 0..1000 {
            let size = rng.gen_range(1..20);
            debug!("test_fuzz_exact_buf_writer: write size: {size}");
            let mut content = vec![0; size];
            rng.fill_bytes(&mut content);

            expected.extend_from_slice(&content);

            let mut bs = Bytes::from(content.clone());
            while !bs.is_empty() {
                let n = writer.write(bs.clone()).await?;
                bs.advance(n as usize);
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
