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
use std::ops::RangeBounds;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;

use crate::raw::*;
use crate::*;

/// FuturesBytesStream is the adapter of [`Stream`] generated by [`Reader::into_bytes_stream`].
///
/// Users can use this adapter in cases where they need to use [`Stream`] trait. FuturesBytesStream reuses the same concurrent and chunk
/// settings from [`Reader`].
///
/// FuturesStream also implements [`Unpin`], [`Send`] and [`Sync`].
pub struct FuturesBytesStream {
    stream: BufferStream,
    buf: Buffer,
}

impl FuturesBytesStream {
    /// NOTE: don't allow users to create FuturesStream directly.
    #[inline]
    pub(crate) fn new(r: oio::Reader, options: OpReader, range: impl RangeBounds<u64>) -> Self {
        let stream = BufferStream::new(r, options, range);

        FuturesBytesStream {
            stream,
            buf: Buffer::new(),
        }
    }
}

impl Stream for FuturesBytesStream {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // Consume current buffer
            if let Some(bs) = Iterator::next(&mut this.buf) {
                return Poll::Ready(Some(Ok(bs)));
            }

            this.buf = match ready!(this.stream.poll_next_unpin(cx)) {
                Some(Ok(buf)) => buf,
                Some(Err(err)) => return Poll::Ready(Some(Err(format_std_io_error(err)))),
                None => return Poll::Ready(None),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use super::*;

    #[tokio::test]
    async fn test_futures_bytes_stream() {
        let r: oio::Reader = Arc::new(Buffer::from(vec![
            Bytes::from("Hello"),
            Bytes::from("World"),
        ]));

        let s = FuturesBytesStream::new(r, OpReader::new(), 4..8);
        let bufs: Vec<Bytes> = s.try_collect().await.unwrap();
        assert_eq!(&bufs[0], "o".as_bytes());
        assert_eq!(&bufs[1], "Wor".as_bytes());
    }

    #[tokio::test]
    async fn test_futures_bytes_stream_with_concurrent() {
        let r: oio::Reader = Arc::new(Buffer::from(vec![
            Bytes::from("Hello"),
            Bytes::from("World"),
        ]));

        let s = FuturesBytesStream::new(r, OpReader::new().with_concurrent(3).with_chunk(1), 4..8);
        let bufs: Vec<Bytes> = s.try_collect().await.unwrap();
        assert_eq!(&bufs[0], "o".as_bytes());
        assert_eq!(&bufs[1], "W".as_bytes());
        assert_eq!(&bufs[2], "o".as_bytes());
        assert_eq!(&bufs[3], "r".as_bytes());
    }
}
