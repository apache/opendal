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
use std::ops::{Range, RangeBounds};
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
    inner: oio::Reader,
}

impl Reader {
    /// Create a new reader from an `oio::Reader`.
    pub(crate) fn new(r: oio::Reader) -> Self {
        Reader { inner: r }
    }

    /// Create a new reader.
    ///
    /// Create will use internal information to decide the most suitable
    /// implementation for users.
    ///
    /// We don't want to expose those details to users so keep this function
    /// in crate only.
    pub(crate) async fn create(acc: FusedAccessor, path: &str, op: OpRead) -> Result<Self> {
        let (_, r) = acc.read(path, op).await?;

        Ok(Reader { inner: r })
    }

    /// Convert [`Reader`] into an [`futures::AsyncRead`] and [`futures::AsyncSeek`]
    ///
    /// `Reader` itself implements [`futures::AsyncRead`], this function is used to
    /// make sure that `Reader` is used as an `AsyncRead` only.
    ///
    /// The returning type also implements `Send`, `Sync` and `Unpin`, so users can use it
    /// as `Box<dyn futures::AsyncRead>` and calling `poll_read_unpin` on it.
    // #[inline]
    // #[cfg(not(target_arch = "wasm32"))]
    // pub fn into_futures_read(
    //     self,
    // ) -> impl futures::AsyncRead + futures::AsyncSeek + Send + Sync + Unpin {
    //     // self
    //     todo!()
    // }

    /// Convert [`Reader`] into an [`tokio::io::AsyncRead`] and [`tokio::io::AsyncSeek`]
    ///
    /// `Reader` itself implements [`tokio::io::AsyncRead`], this function is used to
    /// make sure that `Reader` is used as an [`tokio::io::AsyncRead`] only.
    ///
    /// The returning type also implements `Send`, `Sync` and `Unpin`, so users can use it
    /// as `Box<dyn tokio::io::AsyncRead>` and calling `poll_read_unpin` on it.
    // #[inline]
    // #[cfg(not(target_arch = "wasm32"))]
    // pub fn into_tokio_read(
    //     self,
    // ) -> impl tokio::io::AsyncRead + tokio::io::AsyncSeek + Send + Sync + Unpin {
    //     // self
    //     todo!()
    // }

    pub async fn read_at(&self, buf: &mut impl BufMut, offset: u64) -> Result<usize> {
        let bs = self.inner.read_at_dyn(offset, buf.remaining_mut()).await?;
        let n = bs.remaining();
        buf.put(bs);
        Ok(n)
    }

    /// Read given range bytes of data from reader.
    pub async fn read_range(&self, buf: &mut impl BufMut, range: Range<u64>) -> Result<usize> {
        if range.is_empty() {
            return Ok(0);
        }
        let (mut offset, mut size) = (range.start, range.end - range.start);

        let mut read = 0;

        loop {
            let bs = self.inner.read_at_dyn(offset, size as usize).await?;
            let n = bs.remaining();
            read += n;
            buf.put(bs);
            if n == 0 {
                return Ok(read);
            }

            offset += n as u64;

            debug_assert!(
                size >= n as u64,
                "read should not return more bytes than expected"
            );
            size -= n as u64;
            if size == 0 {
                return Ok(read);
            }
        }
    }

    pub async fn read_to_end(&self, buf: &mut impl BufMut) -> Result<usize> {
        self.read_to_end_at(buf, 0).await
    }

    pub async fn read_to_end_at(&self, buf: &mut imp                l BufMut, mut offset: u64) -> Result<usize> {
        let mut size = 0;
        loop {
            // TODO: io size should be tuned based on storage
            let bs = self.inner.read_at_dyn(offset, 4 * 1024 * 1024).await?;
            let n = bs.remaining();
            size += n;

            buf.put(bs);
            if n == 0 {
                return Ok(size);
            }

            offset += n as u64;
        }
    }
}

enum State {
    Idle(Option<oio::Reader>),
    Reading(BoxedStaticFuture<(oio::Reader, Result<Bytes>)>),
    Seeking(BoxedStaticFuture<(oio::Reader, Result<u64>)>),
}

/// # Safety
///
/// Reader will only be used with `&mut self`.
unsafe impl Sync for State {}

// impl futures::AsyncRead for Reader {
//     fn poll_read(
//         mut self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//         buf: &mut [u8],
//     ) -> Poll<io::Result<usize>> {
//         use oio::Read;
//
//         match &mut self.state {
//             State::Idle(r) => {
//                 let mut r = r.take().expect("reader must be valid");
//                 let size = buf.len();
//                 let fut = async move {
//                     let res = r.read(size).await;
//                     (r, res)
//                 };
//                 self.state = State::Reading(Box::pin(fut));
//                 self.poll_read(cx, buf)
//             }
//             State::Reading(fut) => {
//                 let (r, res) = ready!(fut.as_mut().poll(cx));
//                 self.state = State::Idle(Some(r));
//                 let bs = res.map_err(format_std_io_error)?;
//                 let n = bs.len();
//                 buf[..n].copy_from_slice(&bs);
//                 Poll::Ready(Ok(n))
//             }
//             State::Seeking(_) => Poll::Ready(Err(io::Error::new(
//                 io::ErrorKind::Interrupted,
//                 "another io operation is  in progress",
//             ))),
//         }
//     }
// }

// impl futures::AsyncSeek for Reader {
//     fn poll_seek(
//         mut self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//         pos: io::SeekFrom,
//     ) -> Poll<io::Result<u64>> {
//         use oio::Read;
//
//         match &mut self.state {
//             State::Idle(r) => {
//                 let mut r = r.take().expect("reader must be valid");
//                 let fut = async move {
//                     let res = r.seek(pos).await;
//                     (r, res)
//                 };
//                 self.state = State::Seeking(Box::pin(fut));
//                 self.poll_seek(cx, pos)
//             }
//             State::Seeking(fut) => {
//                 let (r, res) = ready!(fut.as_mut().poll(cx));
//                 self.state = State::Idle(Some(r));
//                 Poll::Ready(res.map_err(format_std_io_error))
//             }
//             State::Reading(_) => Poll::Ready(Err(io::Error::new(
//                 io::ErrorKind::Interrupted,
//                 "another io operation is in progress",
//             ))),
//         }
//     }
// }
//
// impl tokio::io::AsyncRead for Reader {
//     fn poll_read(
//         mut self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//         buf: &mut tokio::io::ReadBuf<'_>,
//     ) -> Poll<io::Result<()>> {
//         use oio::Read;
//
//         loop {
//             match &mut self.state {
//                 State::Idle(r) => {
//                     // Safety: We make sure that we will set filled correctly.
//                     unsafe { buf.assume_init(buf.remaining()) }
//                     let size = buf.initialize_unfilled().len();
//
//                     let mut r = r.take().expect("reader must be valid");
//                     let fut = async move {
//                         let res = r.read(size).await;
//                         (r, res)
//                     };
//                     self.state = State::Reading(Box::pin(fut));
//                 }
//                 State::Reading(fut) => {
//                     let (r, res) = ready!(fut.as_mut().poll(cx));
//                     self.state = State::Idle(Some(r));
//                     let bs = res.map_err(format_std_io_error)?;
//                     let n = bs.len();
//                     buf.initialize_unfilled()[..n].copy_from_slice(&bs);
//                     buf.advance(n);
//                     return Poll::Ready(Ok(()));
//                 }
//                 State::Seeking(_) => {
//                     return Poll::Ready(Err(io::Error::new(
//                         io::ErrorKind::Interrupted,
//                         "another io operation is in progress",
//                     )))
//                 }
//             }
//         }
//     }
// }

// impl tokio::io::AsyncSeek for Reader {
//     fn start_seek(mut self: Pin<&mut Self>, pos: io::SeekFrom) -> io::Result<()> {
//         use oio::Read;
//
//         match &mut self.state {
//             State::Idle(r) => {
//                 let mut r = r.take().expect("reader must be valid");
//                 let fut = async move {
//                     let res = r.seek(pos).await;
//                     (r, res)
//                 };
//                 self.state = State::Seeking(Box::pin(fut));
//                 Ok(())
//             }
//             State::Seeking(_) | State::Reading(_) => Err(io::Error::new(
//                 io::ErrorKind::Interrupted,
//                 "another io operation is in progress",
//             )),
//         }
//     }
//
//     fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
//         match &mut self.state {
//             State::Idle(_) => {
//                 // AsyncSeek recommends calling poll_complete before start_seek.
//                 // We don't have to guarantee that the value returned by
//                 // poll_complete called without start_seek is correct,
//                 // so we'll return 0.
//                 Poll::Ready(Ok(0))
//             }
//             State::Seeking(fut) => {
//                 let (r, res) = ready!(fut.as_mut().poll(cx));
//                 self.state = State::Idle(Some(r));
//                 Poll::Ready(res.map_err(format_std_io_error))
//             }
//             State::Reading(_) => Poll::Ready(Err(io::Error::new(
//                 io::ErrorKind::Interrupted,
//                 "another io operation is in progress",
//             ))),
//         }
//     }
// }

// impl Stream for Reader {
//     type Item = io::Result<Bytes>;
//
//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         use oio::Read;
//
//         match &mut self.state {
//             State::Idle(r) => {
//                 let mut r = r.take().expect("reader must be valid");
//                 let fut = async move {
//                     // TODO: should allow user to tune this value.
//                     let res = r.read(4 * 1024 * 1024).await;
//                     (r, res)
//                 };
//                 self.state = State::Reading(Box::pin(fut));
//                 self.poll_next(cx)
//             }
//             State::Reading(fut) => {
//                 let (r, res) = ready!(fut.as_mut().poll(cx));
//                 self.state = State::Idle(Some(r));
//                 let bs = res.map_err(format_std_io_error)?;
//                 if bs.is_empty() {
//                     Poll::Ready(None)
//                 } else {
//                     Poll::Ready(Some(Ok(bs)))
//                 }
//             }
//             State::Seeking(_) => Poll::Ready(Some(Err(io::Error::new(
//                 io::ErrorKind::Interrupted,
//                 "another io operation is in progress",
//             )))),
//         }
//     }
// }

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

    /// Create a new reader from an `oio::BlockingReader`.
    pub(crate) fn new(r: oio::BlockingReader) -> Self {
        BlockingReader { inner: r }
    }

    /// Read given range bytes of data from reader.
    pub fn read_at(&self, buf: &mut impl BufMut, offset: u64) -> Result<usize> {
        let bs = self.inner.read_at(offset, buf.remaining_mut())?;
        let n = bs.remaining();
        buf.put(bs);
        Ok(n)
    }

    /// Read given range bytes of data from reader.
    pub fn read_range(&self, buf: &mut impl BufMut, range: Range<u64>) -> Result<usize> {
        if range.is_empty() {
            return Ok(0);
        }
        let (mut offset, mut size) = (range.start, range.end - range.start);

        let mut read = 0;

        loop {
            let bs = self.inner.read_at(offset, size as usize)?;
            let n = bs.remaining();
            read += n;
            buf.put(bs);
            if n == 0 {
                return Ok(read);
            }

            offset += n as u64;

            debug_assert!(
                size >= n as u64,
                "read should not return more bytes than expected"
            );
            size -= n as u64;
            if size == 0 {
                return Ok(read);
            }
        }
    }

    pub fn read_to_end(&self, buf: &mut impl BufMut) -> Result<usize> {
        self.read_to_end_at(buf, 0)
    }

    pub fn read_to_end_at(&self, buf: &mut impl BufMut, mut offset: u64) -> Result<usize> {
        let mut size = 0;
        loop {
            // TODO: io size should be tuned based on storage
            let bs = self.inner.read_at(offset, 4 * 1024 * 1024)?;
            let n = bs.remaining();
            size += n;

            buf.put(bs);
            if n == 0 {
                return Ok(size);
            }

            offset += n as u64;
        }
    }
}

// impl io::Read for BlockingReader {
//     #[inline]
//     fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//         let bs = self.inner.read(buf.len()).map_err(format_std_io_error)?;
//         buf[..bs.len()].copy_from_slice(&bs);
//         Ok(bs.len())
//     }
// }
//
// impl io::Seek for BlockingReader {
//     #[inline]
//     fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
//         self.inner.seek(pos).map_err(format_std_io_error)
//     }
// }
//
// impl Iterator for BlockingReader {
//     type Item = io::Result<Bytes>;
//
//     #[inline]
//     fn next(&mut self) -> Option<Self::Item> {
//         match self
//             .inner
//             .read(4 * 1024 * 1024)
//             .map_err(format_std_io_error)
//         {
//             Ok(bs) if bs.is_empty() => None,
//             Ok(bs) => Some(Ok(bs)),
//             Err(err) => Some(Err(err)),
//         }
//     }
// }

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
            .read_to_end_at(&mut buf, 0)
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
            .read_to_end_at(&mut buf, 0)
            .await
            .expect("read to end must succeed");
        assert_eq!(buf, content);
    }
}
