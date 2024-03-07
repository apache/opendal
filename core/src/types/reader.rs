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

use std::io;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::AsyncRead;
use futures::AsyncSeek;
use futures::Stream;

use crate::raw::*;
use crate::*;

/// Reader is designed to read data from given path in an asynchronous
/// manner.
///
/// # Usage
///
/// Reader implements the following APIs:
///
/// - `AsyncRead`
/// - `AsyncSeek`
/// - `Stream<Item = <io::Result<Bytes>>>`
///
/// For reading data, we can use `AsyncRead` and `Stream`. The mainly
/// different is where the `copy` happens.
///
/// `AsyncRead` requires user to prepare a buffer for `Reader` to fill.
/// And `Stream` will stream out a `Bytes` for user to decide when to copy
/// it's content.
///
/// For example, users may have their only CPU/IO bound workers and don't
/// want to do copy inside IO workers. They can use `Stream` to get a `Bytes`
/// and consume it in side CPU workers inside.
///
/// Besides, `Stream` **COULD** reduce an extra copy if underlying reader is
/// stream based (like services s3, azure which based on HTTP).
pub struct Reader {
    inner: oio::Reader,
    seek_state: SeekState,
}

impl Reader {
    /// Create a new reader.
    ///
    /// Create will use internal information to decide the most suitable
    /// implementation for users.
    ///
    /// We don't want to expose those details to users so keep this function
    /// in crate only.
    pub(crate) async fn create(acc: FusedAccessor, path: &str, op: OpRead) -> Result<Self> {
        let (_, r) = acc.read(path, op).await?;

        Ok(Reader {
            inner: r,
            seek_state: SeekState::Init,
        })
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        todo!()
        // Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncSeek for Reader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: io::SeekFrom,
    ) -> Poll<io::Result<u64>> {
        todo!()
        // Pin::new(&mut self.inner).poll_seek(cx, pos)
    }
}

impl tokio::io::AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        todo!()

        // // Safety: We make sure that we will set filled correctly.
        // unsafe { buf.assume_init(buf.remaining()) }
        // let n = ready!(self.inner.poll_read(cx, buf.initialize_unfilled()))?;
        // buf.advance(n);
        // Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncSeek for Reader {
    fn start_seek(self: Pin<&mut Self>, pos: io::SeekFrom) -> io::Result<()> {
        todo!()

        // let this = self.get_mut();
        // if let SeekState::Start(_) = this.seek_state {
        //     return Err(io::Error::new(
        //         io::ErrorKind::Other,
        //         "another search is in progress.",
        //     ));
        // }
        // this.seek_state = SeekState::Start(pos);
        // Ok(())
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        todo!()

        // let state = self.seek_state;
        // match state {
        //     SeekState::Init => {
        //         // AsyncSeek recommends calling poll_complete before start_seek.
        //         // We don't have to guarantee that the value returned by
        //         // poll_complete called without start_seek is correct,
        //         // so we'll return 0.
        //         Poll::Ready(Ok(0))
        //     }
        //     SeekState::Start(pos) => {
        //         let n = ready!(self.inner.poll_seek(cx, pos))?;
        //         self.get_mut().seek_state = SeekState::Init;
        //         Poll::Ready(Ok(n))
        //     }
        // }
    }
}

#[derive(Debug, Clone, Copy)]
/// SeekState is used to track the tokio seek state of Reader.
enum SeekState {
    /// start_seek has not been called.
    Init,
    /// start_seek has been called, but poll_complete has not yet been called.
    Start(io::SeekFrom),
}

impl Stream for Reader {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
        // Pin::new(&mut self.inner)
        //     .poll_next(cx)
        //     .map_err(|err| io::Error::new(io::ErrorKind::Interrupted, err))
    }
}

/// BlockingReader is designed to read data from given path in an blocking
/// manner.
pub struct BlockingReader {
    pub(crate) inner: oio::BlockingReader,
}

impl BlockingReader {
    /// Create a new blocking reader.
    ///
    /// Create will use internal information to decide the most suitable
    /// implementation for users.
    ///
    /// We don't want to expose those details to users so keep this function
    /// in crate only.
    pub(crate) fn create(acc: FusedAccessor, path: &str, op: OpRead) -> Result<Self> {
        let (_, r) = acc.blocking_read(path, op)?;

        Ok(BlockingReader { inner: r })
    }
}

impl oio::BlockingRead for BlockingReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf)
    }

    #[inline]
    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        self.inner.seek(pos)
    }

    #[inline]
    fn next(&mut self) -> Option<Result<Bytes>> {
        oio::BlockingRead::next(&mut self.inner)
    }
}

impl io::Read for BlockingReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl io::Seek for BlockingReader {
    #[inline]
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl Iterator for BlockingReader {
    type Item = io::Result<Bytes>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|v| v.map_err(|err| io::Error::new(io::ErrorKind::Interrupted, err)))
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use rand::RngCore;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncSeekExt;

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
    async fn test_reader_async_read() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let path = "test_file";

        let content = gen_random_bytes();
        op.write(path, content.clone())
            .await
            .expect("write must succeed");

        let mut reader = op.reader(path).await.unwrap();
        let mut buf = Vec::new();
        reader
            .read_to_end(&mut buf)
            .await
            .expect("read to end must succeed");

        assert_eq!(buf, content);
    }

    #[tokio::test]
    async fn test_reader_async_seek() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let path = "test_file";

        let content = gen_random_bytes();
        op.write(path, content.clone())
            .await
            .expect("write must succeed");

        let mut reader = op.reader(path).await.unwrap();
        let mut buf = Vec::new();
        reader
            .read_to_end(&mut buf)
            .await
            .expect("read to end must succeed");
        assert_eq!(buf, content);

        let n = reader.seek(tokio::io::SeekFrom::Start(0)).await.unwrap();
        assert_eq!(n, 0, "seek position must be 0");

        let mut buf = Vec::new();
        reader
            .read_to_end(&mut buf)
            .await
            .expect("read to end must succeed");
        assert_eq!(buf, content);
    }
}
