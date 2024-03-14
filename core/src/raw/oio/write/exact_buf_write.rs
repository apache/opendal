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

use bytes::{Buf, BufMut, Bytes};
use std::mem;

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
    buffer: Vec<u8>,
    frozen: Option<Bytes>,
}

impl<W: oio::Write> ExactBufWriter<W> {
    /// Create a new exact buf writer.
    pub fn new(inner: W, buffer_size: usize) -> Self {
        Self {
            inner,
            buffer_size,
            buffer: Vec::new(),
            frozen: None,
        }
    }
}

impl<W: oio::Write> oio::Write for ExactBufWriter<W> {
    async fn write(&mut self, bs: Bytes) -> Result<usize> {
        if let Some(bs) = self.frozen.as_mut() {
            let written = self.inner.write(bs.clone()).await?;
            bs.advance(written);
            if bs.is_empty() {
                self.frozen = None;
            } else {
                // Return remaining bytes back to buffer
                self.buffer = bs.to_vec();
            }
        }

        // Quick Path
        //
        // if buffer is empty and bs is larger than buffer_size, we can directly
        // freeze the first buffer_size bytes.
        if self.buffer.is_empty() {
            if bs.len() >= self.buffer_size {
                self.frozen = Some(bs.slice(0..self.buffer_size));
                return Ok(self.buffer_size);
            }
        }

        let remaining = self.buffer_size - self.buffer.len();
        if bs.len() >= remaining {
            self.buffer.put_slice(&bs[0..remaining]);
            self.frozen = Some(Bytes::from(mem::take(&mut self.buffer)));
            Ok(remaining)
        } else {
            self.buffer.put_slice(&bs);
            Ok(bs.len())
        }
    }

    async fn close(&mut self) -> Result<()> {
        loop {
            if let Some(bs) = self.frozen.as_mut() {
                let written = self.inner.write(bs.clone()).await?;
                bs.advance(written);
                if bs.is_empty() {
                    self.frozen = None;
                }
            } else if !self.buffer.is_empty() {
                // freeze the remaining buffer
                self.frozen = Some(Bytes::from(mem::take(&mut self.buffer)));
            } else {
                break;
            }
        }

        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.frozen = None;
        self.buffer.clear();
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
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
        async fn write(&mut self, bs: Bytes) -> Result<usize> {
            debug!("test_fuzz_exact_buf_writer: flush size: {}", &bs.len());

            self.buf.extend_from_slice(&bs);
            Ok(bs.len())
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

        let mut w = ExactBufWriter::new(MockWriter { buf: vec![] }, 10);

        let mut bs = Bytes::from(expected.clone());
        while !bs.is_empty() {
            let n = w.write(bs.clone()).await?;
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
