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
use std::io::SeekFrom;
use std::ops::{Bound, Range, RangeBounds};
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::Stream;
use tokio::io::ReadBuf;

use crate::raw::oio::BlockingRead;
use crate::raw::*;
use crate::*;

/// Reader is designed to read data from given path in an asynchronous
/// manner.
///
/// # Usage
///
/// [`Reader`] provides multiple ways to read data from given reader. Please note that it's
/// undefined behavior to use `Reader` in different ways.
///
/// ## Direct
///
/// [`Reader`] provides public API including [`Reader::read`], [`Reader::seek`] and
/// [`Reader::read_to_end`]. You can use those APIs directly without extra copy.
///
/// ## Bytes Stream
///
/// [`Reader`] can be used as `Stream<Item = <io::Result<Bytes>>>`.
///
/// It also implements [`Send`], [`Sync`] and [`Unpin`].
///
/// ## Futures AsyncRead
///
/// [`Reader`] can be used as [`futures::AsyncRead`] and [`futures::AsyncSeek`].
///
/// It also implements [`Send`], [`Sync`] and [`Unpin`].
///
/// [`Reader`] provides [`Reader::into_futures_read`] to remove extra APIs upon self.
///
/// ## Tokio AsyncRead
///
/// [`Reader`] can be used as [`tokio::io::AsyncRead`] and [`tokio::io::AsyncSeek`].
///
/// It also implements [`Send`], [`Sync`] and [`Unpin`].
///
/// [`Reader`] provides [`Reader::into_tokio_read`] to remove extra APIs upon self.
pub struct Reader {
    acc: FusedAccessor,
    path: String,
    op: OpRead,

    inner: oio::Reader,
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
        let (_, r) = acc.read(path, op.clone()).await?;

        Ok(Reader {
            acc,
            path: path.to_string(),
            op,
            inner: r,
        })
    }

    /// Convert [`Reader`] into an [`futures::AsyncRead`] and [`futures::AsyncSeek`]
    ///
    /// `Reader` itself implements [`futures::AsyncRead`], this function is used to
    /// make sure that `Reader` is used as an `AsyncRead` only.
    ///
    /// The returning type also implements `Send`, `Sync` and `Unpin`, so users can use it
    /// as `Box<dyn futures::AsyncRead>` and calling `poll_read_unpin` on it.
    #[inline]
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_futures_read(
        self,
    ) -> impl futures::AsyncRead + futures::AsyncSeek + Send + Sync + Unpin {
        futures_io_adapter::FuturesReader::new(self.acc, self.path, self.op)
    }

    /// Convert [`Reader`] into an [`tokio::io::AsyncRead`] and [`tokio::io::AsyncSeek`]
    ///
    /// `Reader` itself implements [`tokio::io::AsyncRead`], this function is used to
    /// make sure that `Reader` is used as an [`tokio::io::AsyncRead`] only.
    ///
    /// The returning type also implements `Send`, `Sync` and `Unpin`, so users can use it
    /// as `Box<dyn tokio::io::AsyncRead>` and calling `poll_read_unpin` on it.
    #[inline]
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_tokio_read(
        self,
    ) -> impl tokio::io::AsyncRead + tokio::io::AsyncSeek + Send + Sync + Unpin {
        tokio_io_adapter::TokioReader::new(self.acc, self.path, self.op)
    }

    #[inline]
    pub async fn read(&self, buf: &mut impl BufMut, offset: u64, limit: usize) -> Result<usize> {
        let bs = self.inner.read_at_dyn(offset, limit).await?;
        let n = bs.remaining();
        buf.put(bs);
        Ok(n)
    }

    /// Read given range bytes of data from reader.
    pub async fn read_range(
        &self,
        buf: &mut impl BufMut,
        range: impl RangeBounds<u64>,
    ) -> Result<usize> {
        let start = match range.start_bound().cloned() {
            Bound::Included(start) => start,
            Bound::Excluded(start) => start + 1,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound().cloned() {
            Bound::Included(end) => Some(end + 1),
            Bound::Excluded(end) => Some(end),
            Bound::Unbounded => None,
        };

        // If range is empty, return Ok(0) directly.
        if let Some(end) = end {
            if end <= start {
                return Ok(0);
            }
        }

        let mut offset = start;
        let mut size = match end {
            Some(end) => Some(end - start),
            None => None,
        };

        let mut read = 0;
        loop {
            let bs = self
                .inner
                // TODO: use service preferred io size instead.
                .read_at_dyn(offset, size.unwrap_or(4 * 1024 * 1024) as usize)
                .await?;
            let n = bs.remaining();
            read += n;
            buf.put(bs);
            if n == 0 {
                return Ok(read);
            }

            offset += n as u64;

            size = size.map(|v| v - n as u64);
            if size == Some(0) {
                return Ok(read);
            }
        }
    }

    #[inline]
    pub async fn read_to_end(&self, buf: &mut impl BufMut) -> Result<usize> {
        self.read_range(buf, ..).await
    }
}

mod futures_io_adapter {
    use super::*;
    use futures::io::{AsyncRead, AsyncSeek};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// TODO: we can implement async buf read.
    pub struct FuturesReader {
        pub acc: FusedAccessor,
        pub path: String,
        pub op: OpRead,

        pub state: State,
        pub offset: u64,
    }

    enum State {
        Idle(Option<oio::Reader>),
        Stating(BoxedStaticFuture<Result<RpStat>>),
        Reading(BoxedStaticFuture<(oio::Reader, Result<oio::Buffer>)>),
    }

    /// # Safety
    ///
    /// Reader will only be used with `&mut self`.
    unsafe impl Sync for State {}

    impl FuturesReader {
        pub fn new(acc: FusedAccessor, path: String, op: OpRead) -> Self {
            Self {
                acc,
                path,
                op,
                state: State::Idle(None),
                offset: 0,
            }
        }
    }

    impl AsyncRead for FuturesReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            mut buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            use oio::Read;

            match &mut self.state {
                State::Idle(r) => {
                    let mut r = r.take().expect("reader must be valid");
                    let size = buf.len();
                    let offset = self.offset;
                    let fut = async move {
                        let res = r.read_at(offset, size).await;
                        (r, res)
                    };
                    self.state = State::Reading(Box::pin(fut));
                    self.poll_read(cx, buf)
                }
                State::Reading(fut) => {
                    let (r, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(r));
                    let bs = res.map_err(format_std_io_error)?;
                    let n = bs.remaining();
                    buf.put(bs);
                    self.offset += n as u64;
                    Poll::Ready(Ok(n))
                }
                State::Stating(_) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "another io operation is in progress",
                ))),
            }
        }
    }

    impl AsyncSeek for FuturesReader {
        fn poll_seek(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            pos: io::SeekFrom,
        ) -> Poll<io::Result<u64>> {
            use oio::Read;

            match &mut self.state {
                State::Idle(_) => match pos {
                    SeekFrom::Start(n) => {
                        self.offset = n;
                        Poll::Ready(Ok(n))
                    }
                    SeekFrom::End(_) => todo!(),
                    SeekFrom::Current(amt) => {
                        let offset = self.offset as i64 + amt;
                        if offset < 0 {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "invalid seek to a negative position",
                            )));
                        }
                        self.offset = offset as u64;
                        Poll::Ready(Ok(self.offset))
                    }
                },
                State::Stating(fut) => {
                    todo!()
                }
                State::Reading(_) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "another io operation is in progress",
                ))),
            }
        }
    }
}

mod tokio_io_adapter {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncSeek};

    /// TODO: we can implement async buf read.
    pub struct TokioReader {
        acc: FusedAccessor,
        path: String,
        op: OpRead,

        state: State,
        offset: u64,
    }

    enum State {
        Idle(Option<oio::Reader>),
        Stating(BoxedStaticFuture<Result<RpStat>>),
        Reading(BoxedStaticFuture<(oio::Reader, Result<oio::Buffer>)>),
    }

    /// # Safety
    ///
    /// Reader will only be used with `&mut self`.
    unsafe impl Sync for State {}

    impl TokioReader {
        pub fn new(acc: FusedAccessor, path: String, op: OpRead) -> Self {
            Self {
                acc,
                path,
                op,
                state: State::Idle(None),
                offset: 0,
            }
        }
    }

    impl AsyncRead for TokioReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            use oio::Read;

            match &mut self.state {
                State::Idle(r) => {
                    let mut r = r.take().expect("reader must be valid");
                    let size = buf.remaining_mut();
                    let offset = self.offset;
                    let fut = async move {
                        let res = r.read_at(offset, size).await;
                        (r, res)
                    };
                    self.state = State::Reading(Box::pin(fut));
                    self.poll_read(cx, buf)
                }
                State::Reading(fut) => {
                    let (r, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(r));
                    let bs = res.map_err(format_std_io_error)?;
                    let n = bs.remaining();
                    buf.put(bs);
                    self.offset += n as u64;
                    Poll::Ready(Ok(()))
                }
                State::Stating(_) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "another io operation is in progress",
                ))),
            }
        }
    }

    impl AsyncSeek for TokioReader {
        fn start_seek(mut self: Pin<&mut Self>, pos: io::SeekFrom) -> io::Result<()> {
            match &mut self.state {
                State::Idle(_) => match pos {
                    SeekFrom::Start(n) => {
                        self.offset = n;
                        Ok(())
                    }
                    SeekFrom::End(_) => todo!(),
                    SeekFrom::Current(amt) => {
                        let offset = self.offset as i64 + amt;
                        if offset < 0 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "invalid seek to a negative position",
                            ));
                        }
                        self.offset = offset as u64;
                        Ok(())
                    }
                },
                State::Stating(fut) => {
                    todo!()
                }
                State::Reading(_) => Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "another io operation is in progress",
                )),
            }
        }

        fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
            Poll::Ready(Ok(self.offset))
        }
    }
}

mod stream_adapter {
    use super::*;
    use crate::raw::{oio, BoxedStaticFuture, RpStat};
    use bytes::Bytes;
    use futures::Stream;
    use std::io;
    use std::pin::Pin;

    pub struct StreamReader {
        state: State,

        buffer: oio::Buffer,
        offset: u64,
    }

    enum State {
        Idle(Option<oio::Reader>),
        Reading(BoxedStaticFuture<(oio::Reader, crate::Result<oio::Buffer>)>),
    }

    impl Stream for StreamReader {
        type Item = io::Result<Bytes>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.buffer.has_remaining() {
                let n = self.buffer.chunk().len();
                let bs = self.buffer.copy_to_bytes(n);
                return Poll::Ready(Some(Ok(bs)));
            }

            let offset = self.offset;

            match &mut self.state {
                State::Idle(r) => {
                    let mut r = r.take().expect("reader must be valid");
                    let fut = async move {
                        // TODO: should allow user to tune this value.
                        let res = r.read_at_dyn(offset, 4 * 1024 * 1024).await;
                        (r, res)
                    };
                    self.state = State::Reading(Box::pin(fut));
                    self.poll_next(cx)
                }
                State::Reading(fut) => {
                    let (r, res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some(r));
                    let bs = res.map_err(format_std_io_error)?;
                    if bs.has_remaining() {
                        self.offset += bs.remaining() as u64;
                        self.buffer = bs;
                        self.poll_next(cx)
                    } else {
                        Poll::Ready(None)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
    }
}
