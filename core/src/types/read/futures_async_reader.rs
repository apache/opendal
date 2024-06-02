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
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::Buf;
use futures::AsyncBufRead;
use futures::AsyncRead;
use futures::AsyncSeek;
use futures::StreamExt;

use crate::raw::*;
use crate::*;

/// FuturesAsyncReader is the adapter of [`AsyncRead`], [`AsyncBufRead`]
/// and [`AsyncSeek`] generated by [`Reader::into_futures_async_read`].
///
/// Users can use this adapter in cases where they need to use [`AsyncRead`]
/// related trait. FuturesAsyncReader reuses the same concurrent and chunk
/// settings from [`Reader`].
///
/// FuturesAsyncReader also implements [`Unpin`], [`Send`] and [`Sync`]
pub struct FuturesAsyncReader {
    ctx: Arc<ReadContext>,

    stream: BufferStream,
    buf: Buffer,
    start: u64,
    end: u64,
    pos: u64,
}

/// Safety: FuturesAsyncReader only exposes `&mut self` to the outside world,
unsafe impl Sync for FuturesAsyncReader {}

impl FuturesAsyncReader {
    /// NOTE: don't allow users to create FuturesAsyncReader directly.
    ///
    /// # TODO
    ///
    /// Extend this API to accept `impl RangeBounds`.
    #[inline]
    pub(super) fn new(ctx: Arc<ReadContext>, range: Range<u64>) -> Self {
        let (start, end) = (range.start, range.end);
        let stream = BufferStream::new(ctx.clone(), range);

        FuturesAsyncReader {
            ctx,
            stream,
            buf: Buffer::new(),
            start,
            end,
            pos: 0,
        }
    }
}

impl AsyncBufRead for FuturesAsyncReader {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        loop {
            if this.buf.has_remaining() {
                return Poll::Ready(Ok(this.buf.chunk()));
            }

            this.buf = match ready!(this.stream.poll_next_unpin(cx)) {
                Some(Ok(buf)) => buf,
                Some(Err(err)) => return Poll::Ready(Err(format_std_io_error(err))),
                None => return Poll::Ready(Ok(&[])),
            };
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
        self.pos += amt as u64;
    }
}

/// TODO: implement vectored read.
impl AsyncRead for FuturesAsyncReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        loop {
            if this.buf.remaining() > 0 {
                let size = this.buf.remaining().min(buf.len());
                this.buf.copy_to_slice(&mut buf[..size]);
                this.pos += size as u64;
                return Poll::Ready(Ok(size));
            }

            this.buf = match ready!(this.stream.poll_next_unpin(cx)) {
                Some(Ok(buf)) => buf,
                Some(Err(err)) => return Poll::Ready(Err(format_std_io_error(err))),
                None => return Poll::Ready(Ok(0)),
            };
        }
    }
}

impl AsyncSeek for FuturesAsyncReader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let new_pos = match pos {
            SeekFrom::Start(pos) => pos as i64,
            SeekFrom::End(pos) => self.end as i64 - self.start as i64 + pos,
            SeekFrom::Current(pos) => self.pos as i64 + pos,
        };

        // Check if new_pos is negative.
        if new_pos < 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid seek to a negative position",
            )));
        }

        let new_pos = new_pos as u64;

        if (self.pos..self.pos + self.buf.remaining() as u64).contains(&new_pos) {
            let cnt = new_pos - self.pos;
            self.buf.advance(cnt as _);
        } else {
            self.buf = Buffer::new();
            self.stream = BufferStream::new(self.ctx.clone(), new_pos + self.start..self.end);
        }

        self.pos = new_pos;
        Poll::Ready(Ok(self.pos))
    }
}

#[cfg(test_xx)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::AsyncBufReadExt;
    use futures::AsyncReadExt;
    use futures::AsyncSeekExt;
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_trait() {
        let v = FuturesAsyncReader::new(Arc::new(Buffer::new()), OpReader::new(), 4..8);

        let _: Box<dyn Unpin + MaybeSend + Sync + 'static> = Box::new(v);
    }

    #[tokio::test]
    async fn test_futures_async_read() {
        let r: oio::Reader = Box::new(Buffer::from(vec![
            Bytes::from("Hello"),
            Bytes::from("World"),
        ]));

        let mut fr = FuturesAsyncReader::new(r, OpReader::new(), 4..8);
        let mut bs = vec![];
        fr.read_to_end(&mut bs).await.unwrap();
        assert_eq!(&bs, "oWor".as_bytes());

        let pos = fr.seek(SeekFrom::Current(-2)).await.unwrap();
        assert_eq!(pos, 2);
        let mut bs = vec![];
        fr.read_to_end(&mut bs).await.unwrap();
        assert_eq!(&bs, "or".as_bytes());
    }

    #[tokio::test]
    async fn test_futures_async_read_with_concurrent() {
        let r: oio::Reader = Arc::new(Buffer::from(vec![
            Bytes::from("Hello"),
            Bytes::from("World"),
        ]));

        let mut fr =
            FuturesAsyncReader::new(r, OpReader::new().with_concurrent(3).with_chunk(1), 4..8);
        let mut bs = vec![];
        fr.read_to_end(&mut bs).await.unwrap();
        assert_eq!(&bs, "oWor".as_bytes());

        let pos = fr.seek(SeekFrom::Current(-2)).await.unwrap();
        assert_eq!(pos, 2);
        let mut bs = vec![];
        fr.read_to_end(&mut bs).await.unwrap();
        assert_eq!(&bs, "or".as_bytes());
    }

    #[tokio::test]
    async fn test_futures_async_buf_read() {
        let r: oio::Reader = Arc::new(Buffer::from(vec![
            Bytes::from("Hello"),
            Bytes::from("World"),
        ]));

        let mut fr = FuturesAsyncReader::new(r, OpReader::new(), 4..8);
        let chunk = fr.fill_buf().await.unwrap();
        assert_eq!(chunk, "o".as_bytes());

        fr.consume_unpin(1);
        let chunk = fr.fill_buf().await.unwrap();
        assert_eq!(chunk, "Wor".as_bytes());
    }
}
