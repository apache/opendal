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

use std::pin::pin;

use bytes::Buf;
use bytes::Bytes;
use futures::TryStreamExt;

use crate::raw::oio::Write;
use crate::raw::*;
use crate::*;

/// Writer is designed to write data into given path in an asynchronous
/// manner.
///
/// ## Notes
///
/// Please make sure either `close` or `abort` has been called before
/// dropping the writer otherwise the data could be lost.
///
/// ## Usage
///
/// ### Write Multiple Chunks
///
/// Some services support to write multiple chunks of data into given path. Services that doesn't
/// support write multiple chunks will return [`ErrorKind::Unsupported`] error when calling `write`
/// at the second time.
///
/// ```no_build
/// let mut w = op.writer("path/to/file").await?;
/// w.write(bs).await?;
/// w.write(bs).await?;
/// w.close().await?
/// ```
///
/// Our writer also provides [`Writer::sink`] and [`Writer::copy`] support.
///
/// Besides, our writer implements [`AsyncWrite`] and [`tokio::io::AsyncWrite`].
///
/// ### Write with append enabled
///
/// Writer also supports to write with append enabled. This is useful when users want to append
/// some data to the end of the file.
///
/// - If file doesn't exist, it will be created and just like calling `write`.
/// - If file exists, data will be appended to the end of the file.
///
/// Possible Errors:
///
/// - Some services store normal file and appendable file in different way. Trying to append
///   on non-appendable file could return [`ErrorKind::ConditionNotMatch`] error.
/// - Services that doesn't support append will return [`ErrorKind::Unsupported`] error when
///   creating writer with `append` enabled.
pub struct Writer {
    inner: oio::Writer,
}

impl Writer {
    /// Create a new writer from an `oio::Writer`.
    pub(crate) fn new(w: oio::Writer) -> Self {
        Writer { inner: w }
    }

    /// Create a new writer.
    ///
    /// Create will use internal information to decide the most suitable
    /// implementation for users.
    ///
    /// We don't want to expose those details to users so keep this function
    /// in crate only.
    pub(crate) async fn create(acc: FusedAccessor, path: &str, op: OpWrite) -> Result<Self> {
        let (_, w) = acc.write(path, op).await?;

        Ok(Writer { inner: w })
    }

    /// Write Buffer into inner writer.
    pub async fn write(&mut self, bs: impl Into<Buffer>) -> Result<()> {
        let mut bs = bs.into();
        while !bs.is_empty() {
            let n = self.inner.write_dyn(bs.clone()).await?;
            bs.advance(n);
        }
        Ok(())
    }

    /// Write bytes::Buf into inner writer.
    pub async fn write_from(&mut self, bs: impl Buf) -> Result<()> {
        let mut bs = bs;
        let mut bs = Buffer::from(bs.copy_to_bytes(bs.remaining()));
        while !bs.is_empty() {
            let n = self.inner.write_dyn(bs.clone()).await?;
            bs.advance(n);
        }
        Ok(())
    }

    /// Abort the writer and clean up all written data.
    ///
    /// ## Notes
    ///
    /// Abort should only be called when the writer is not closed or
    /// aborted, otherwise an unexpected error could be returned.
    pub async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }

    /// Close the writer and make sure all data have been committed.
    ///
    /// ## Notes
    ///
    /// Close should only be called when the writer is not closed or
    /// aborted, otherwise an unexpected error could be returned.
    pub async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }

    /// Sink into writer.
    ///
    /// sink will read data from given streamer and write them into writer
    /// directly without extra in-memory buffer.
    ///
    /// # Notes
    ///
    /// - Sink doesn't support to be used with write concurrently.
    /// - Sink doesn't support to be used without content length now.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use bytes::Bytes;
    /// use futures::stream;
    /// use futures::StreamExt;
    /// use opendal::Operator;
    /// use opendal::Result;
    ///
    /// async fn sink_example(op: Operator) -> Result<()> {
    ///     let mut w = op.writer_with("path/to/file").await?;
    ///     let stream = stream::iter(vec![vec![0; 4096], vec![1; 4096]]).map(Ok);
    ///     w.sink(stream).await?;
    ///     w.close().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn sink<S, T>(&mut self, sink_from: S) -> Result<u64>
    where
        S: futures::Stream<Item = Result<T>>,
        T: Into<Bytes>,
    {
        let mut sink_from = pin!(sink_from);
        let mut written = 0;
        while let Some(bs) = sink_from.try_next().await? {
            let mut bs = bs.into();
            while !bs.is_empty() {
                let n = self.inner.write(bs.clone().into()).await?;
                bs.advance(n);
                written += n as u64;
            }
        }
        Ok(written)
    }

    /// Convert writer into [`FuturesAsyncWriter`] which implements [`futures::AsyncWrite`],
    pub fn into_futures_async_write(self) -> FuturesAsyncWriter {
        FuturesAsyncWriter::new(self.inner)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use rand::RngCore;

    use crate::services;
    use crate::Operator;

    fn gen_random_bytes() -> Vec<u8> {
        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);
        content
    }

    #[tokio::test]
    async fn test_writer_write() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let path = "test_file";

        let content = gen_random_bytes();
        let mut writer = op.writer(path).await.unwrap();
        writer
            .write(content.clone())
            .await
            .expect("write must succeed");
        writer.close().await.expect("close must succeed");

        let buf = op.read(path).await.expect("read to end mut succeed");

        assert_eq!(buf.to_bytes(), content);
    }

    #[tokio::test]
    async fn test_writer_write_from() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let path = "test_file";

        let content = gen_random_bytes();
        let mut writer = op.writer(path).await.unwrap();
        writer
            .write_from(Bytes::from(content.clone()))
            .await
            .expect("write must succeed");
        writer.close().await.expect("close must succeed");

        let buf = op.read(path).await.expect("read to end mut succeed");

        assert_eq!(buf.to_bytes(), content);
    }
}
