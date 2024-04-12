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

use std::ops::Range;
use std::ops::RangeBounds;

use bytes::BufMut;
use futures::stream;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

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
/// [`Reader`] provides public API including [`Reader::read`], [`Reader:read_range`], and [`Reader::read_to_end`]. You can use those APIs directly without extra copy.
#[derive(Clone)]
pub struct Reader {
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
        let (_, r) = acc.read(path, op).await?;

        Ok(Reader { inner: r })
    }

    /// Read give range from reader into [`Buffer`].
    ///
    /// This operation is zero-copy, which means it keeps the [`Bytes`] returned by underlying
    /// storage services without any extra copy or intensive memory allocations.
    ///
    /// # Notes
    ///
    /// - Buffer length smaller than range means we have reached the end of file.
    pub async fn read(&self, range: impl RangeBounds<u64>) -> Result<Buffer> {
        let bufs: Vec<_> = self.into_stream(range).try_collect().await?;
        Ok(bufs.into_iter().flatten().collect())
    }

    /// Read all data from reader into given [`BufMut`].
    ///
    /// This operation will copy and write bytes into given [`BufMut`]. Allocation happens while
    /// [`BufMut`] doesn't have enough space.
    ///
    /// # Notes
    ///
    /// - Returning length smaller than range means we have reached the end of file.
    pub async fn read_into(
        &self,
        buf: &mut impl BufMut,
        range: impl RangeBounds<u64>,
    ) -> Result<usize> {
        let mut stream = self.into_stream(range);

        let mut read = 0;
        loop {
            let Some(bs) = stream.try_next().await? else {
                return Ok(read);
            };
            read += bs.len();
            buf.put(bs);
        }
    }

    /// Create a buffer stream to read specific range from given reader.
    pub fn into_stream(
        &self,
        range: impl RangeBounds<u64>,
    ) -> impl Stream<Item = Result<Buffer>> + Unpin + MaybeSend + 'static {
        let futs = into_stream::ReadFutureIterator::new(self.inner.clone(), range);

        stream::iter(futs).then(|f| f)
    }

    /// Convert reader into [`FuturesIoAsyncReader`] which implements [`futures::AsyncRead`],
    /// [`futures::AsyncSeek`] and [`futures::AsyncBufRead`].
    #[inline]
    pub fn into_futures_io_async_read(self, range: Range<u64>) -> FuturesIoAsyncReader {
        FuturesIoAsyncReader::new(self.inner, range)
    }

    /// Convert reader into [`FuturesBytesStream`] which implements [`futures::Stream`],
    /// [`futures::AsyncSeek`] and [`futures::AsyncBufRead`].
    #[inline]
    pub fn into_futures_bytes_stream(self, range: Range<u64>) -> FuturesBytesStream {
        FuturesBytesStream::new(self.inner, range)
    }
}

pub mod into_stream {
    use std::sync::atomic::Ordering;
    use std::{
        ops::{Bound, RangeBounds},
        sync::{atomic::AtomicBool, Arc},
    };

    use crate::raw::oio::ReadDyn;
    use crate::raw::*;
    use crate::*;

    pub struct ReadFutureIterator {
        r: oio::Reader,

        offset: u64,
        end: Option<u64>,
        finished: Arc<AtomicBool>,
    }

    impl ReadFutureIterator {
        pub fn new(r: oio::Reader, range: impl RangeBounds<u64>) -> Self {
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

            ReadFutureIterator {
                r,
                offset: start,
                end,
                finished: Arc::default(),
            }
        }
    }

    impl Iterator for ReadFutureIterator {
        type Item = BoxedFuture<'static, Result<Buffer>>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.offset >= self.end.unwrap_or(u64::MAX) {
                return None;
            }
            if self.finished.load(Ordering::Relaxed) {
                return None;
            }

            let offset = self.offset;
            let limit = self
                .end
                .map(|end| end - self.offset)
                .unwrap_or(4 * 1024 * 1024) as usize;
            let finished = self.finished.clone();
            let r = self.r.clone();

            // Update self.offset before building future.
            self.offset += limit as u64;
            let fut = async move {
                let buf = r.read_at_static(offset, limit).await?;
                if buf.len() < limit || limit == 0 {
                    // Update finished marked if buf is less than limit.
                    finished.store(true, Ordering::Relaxed);
                }
                Ok(buf)
            };

            Some(Box::pin(fut))
        }
    }
}

pub mod into_futures_async_read {
    use std::io;
    use std::io::SeekFrom;
    use std::ops::Range;
    use std::pin::Pin;
    use std::task::ready;
    use std::task::Context;
    use std::task::Poll;

    use bytes::Buf;
    use futures::AsyncBufRead;
    use futures::AsyncRead;
    use futures::AsyncSeek;

    use crate::raw::*;
    use crate::*;

    /// FuturesAsyncReader is the adapter of [`AsyncRead`], [`AsyncBufRead`] and [`AsyncSeek`]
    /// for [`Reader`].
    ///
    /// Users can use this adapter in cases where they need to use [`AsyncRead`] related trait.
    ///
    /// FuturesAsyncReader also implements [`Unpin`], [`Send`] and [`Sync`]
    pub struct FuturesIoAsyncReader {
        state: State,
        offset: u64,
        size: u64,
        cap: usize,

        cur: u64,
        buf: Buffer,
    }

    enum State {
        Idle(Option<oio::Reader>),
        Fill(BoxedStaticFuture<(oio::Reader, Result<Buffer>)>),
    }

    /// # Safety
    ///
    /// FuturesReader only exposes `&mut self` to the outside world, so it's safe to be `Sync`.
    unsafe impl Sync for State {}

    impl FuturesIoAsyncReader {
        /// NOTE: don't allow users to create FuturesAsyncReader directly.
        #[inline]
        pub(super) fn new(r: oio::Reader, range: Range<u64>) -> Self {
            FuturesIoAsyncReader {
                state: State::Idle(Some(r)),
                offset: range.start,
                size: range.end - range.start,
                // TODO: should use services preferred io size.
                cap: 8 * 1024 * 1024,

                cur: 0,
                buf: Buffer::new(),
            }
        }

        /// Set the capacity of this reader to control the IO size.
        pub fn with_capacity(mut self, cap: usize) -> Self {
            self.cap = cap;
            self
        }
    }

    impl AsyncBufRead for FuturesIoAsyncReader {
        fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
            let this = self.get_mut();
            loop {
                if this.buf.has_remaining() {
                    return Poll::Ready(Ok(this.buf.chunk()));
                }

                match &mut this.state {
                    State::Idle(r) => {
                        // Make sure cur didn't exceed size.
                        if this.cur >= this.size {
                            return Poll::Ready(Ok(&[]));
                        }

                        let r = r.take().expect("reader must be present");
                        let next_offset = this.offset + this.cur;
                        let next_size = (this.size - this.cur).min(this.cap as u64) as usize;
                        let fut = async move {
                            let res = r.read_at_dyn(next_offset, next_size).await;
                            (r, res)
                        };
                        this.state = State::Fill(Box::pin(fut));
                    }
                    State::Fill(fut) => {
                        let (r, res) = ready!(fut.as_mut().poll(cx));
                        this.state = State::Idle(Some(r));
                        this.buf = res?;
                    }
                }
            }
        }

        fn consume(mut self: Pin<&mut Self>, amt: usize) {
            self.buf.advance(amt);
            // Make sure buf has been dropped before starting new request.
            // Otherwise, we will hold those bytes in memory until next
            // buffer reaching.
            if self.buf.is_empty() {
                self.buf = Buffer::new();
            }
            self.cur += amt as u64;
        }
    }

    impl AsyncRead for FuturesIoAsyncReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            let bs = ready!(self.as_mut().poll_fill_buf(cx))?;
            let n = bs.len().min(buf.len());
            buf[..n].copy_from_slice(&bs[..n]);
            self.as_mut().consume(n);
            Poll::Ready(Ok(n))
        }
    }

    impl AsyncSeek for FuturesIoAsyncReader {
        fn poll_seek(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            pos: SeekFrom,
        ) -> Poll<io::Result<u64>> {
            let new_pos = match pos {
                SeekFrom::Start(pos) => pos as i64,
                SeekFrom::End(pos) => self.size as i64 + pos,
                SeekFrom::Current(pos) => self.cur as i64 + pos,
            };

            if new_pos < 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid seek to a negative position",
                )));
            }

            let new_pos = new_pos as u64;

            if (self.cur..self.cur + self.buf.remaining() as u64).contains(&new_pos) {
                let cnt = new_pos - self.cur;
                self.buf.advance(cnt as _);
            } else {
                self.buf = Buffer::new()
            }

            self.cur = new_pos;
            Poll::Ready(Ok(self.cur))
        }
    }
}

pub mod into_futures_stream {
    use std::io;
    use std::ops::Range;
    use std::pin::Pin;
    use std::task::ready;
    use std::task::Context;
    use std::task::Poll;

    use bytes::Bytes;
    use futures::Stream;

    use crate::raw::oio::ReadDyn;
    use crate::raw::*;
    use crate::*;

    /// FuturesStream is the adapter of [`Stream`] for [`Reader`].
    ///
    /// Users can use this adapter in cases where they need to use [`Stream`] trait.
    ///
    /// FuturesStream also implements [`Unpin`], [`Send`] and [`Sync`].
    pub struct FuturesBytesStream {
        r: oio::Reader,
        state: State,
        offset: u64,
        size: u64,
        cap: usize,

        cur: u64,
    }

    enum State {
        Idle(Buffer),
        Next(BoxedStaticFuture<Result<Buffer>>),
    }

    /// # Safety
    ///
    /// FuturesReader only exposes `&mut self` to the outside world, so it's safe to be `Sync`.
    unsafe impl Sync for State {}

    impl FuturesBytesStream {
        /// NOTE: don't allow users to create FuturesStream directly.
        #[inline]
        pub(crate) fn new(r: oio::Reader, range: Range<u64>) -> Self {
            FuturesBytesStream {
                r,
                state: State::Idle(Buffer::new()),
                offset: range.start,
                size: range.end - range.start,
                // TODO: should use services preferred io size.
                cap: 4 * 1024 * 1024,

                cur: 0,
            }
        }

        /// Set the capacity of this reader to control the IO size.
        pub fn with_capacity(mut self, cap: usize) -> Self {
            self.cap = cap;
            self
        }
    }

    impl Stream for FuturesBytesStream {
        type Item = io::Result<Bytes>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();

            loop {
                match &mut this.state {
                    State::Idle(buf) => {
                        // Consume current buffer
                        if let Some(bs) = buf.next() {
                            return Poll::Ready(Some(Ok(bs)));
                        }

                        // Make sure cur didn't exceed size.
                        if this.cur >= this.size {
                            return Poll::Ready(None);
                        }

                        let r = this.r.clone();
                        let next_offset = this.offset + this.cur;
                        let next_size = (this.size - this.cur).min(this.cap as u64) as usize;
                        let fut = async move { r.read_at_static(next_offset, next_size).await };
                        this.state = State::Next(Box::pin(fut));
                    }
                    State::Next(fut) => {
                        let res = ready!(fut.as_mut().poll(cx));
                        match res {
                            Ok(buf) => {
                                this.state = State::Idle(buf);
                            }
                            Err(err) => return Poll::Ready(Some(Err(format_std_io_error(err)))),
                        };
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
    async fn test_reader_read() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let path = "test_file";

        let content = gen_random_bytes();
        op.write(path, content.clone())
            .await
            .expect("write must succeed");

        let reader = op.reader(path).await.unwrap();
        let buf = reader.read(..).await.expect("read to end must succeed");

        assert_eq!(buf.to_bytes(), content);
    }

    #[tokio::test]
    async fn test_reader_read_into() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let path = "test_file";

        let content = gen_random_bytes();
        op.write(path, content.clone())
            .await
            .expect("write must succeed");

        let reader = op.reader(path).await.unwrap();
        let mut buf = Vec::new();
        reader
            .read_into(&mut buf, ..)
            .await
            .expect("read to end must succeed");

        assert_eq!(buf, content);
    }
}
