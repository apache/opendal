// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::min;
use std::cmp::Ordering;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use futures::AsyncBufRead;
use futures::AsyncRead;
use futures::Stream;
use futures::StreamExt;
use pin_project::pin_project;

use crate::raw::*;

/// Convert [`input::Stream`] into [`input::Read`].
///
/// # Note
///
/// This conversion is **zero cost**.
///
/// # Example
///
/// ```rust
/// use opendal::raw::input::into_reader;
/// # use anyhow::Result;
/// # use std::io::Error;
/// # use futures::io;
/// # use bytes::Bytes;
/// # use futures::StreamExt;
/// # use futures::SinkExt;
/// # use futures::stream;
/// # use futures::AsyncReadExt;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// # let stream = Box::pin(stream::once(async {
/// #     Ok::<_, Error>(Bytes::from(vec![0; 1024]))
/// # }));
/// let mut s = into_reader::from_stream(stream, Some(1024));
/// let mut bs = Vec::new();
/// s.read_to_end(&mut bs).await;
/// # Ok(())
/// # }
/// ```
pub fn from_stream<S: input::Stream>(stream: S, size: Option<u64>) -> IntoReader<S> {
    IntoReader {
        stream,
        size,
        read: 0,
        chunk: None,
    }
}

#[pin_project]
pub struct IntoReader<S: input::Stream> {
    #[pin]
    stream: S,
    size: Option<u64>,
    read: u64,
    chunk: Option<Bytes>,
}

impl<S> IntoReader<S>
where
    S: input::Stream,
{
    /// Do we have a chunk and is it non-empty?
    #[inline]
    fn has_chunk(&self) -> bool {
        if let Some(ref chunk) = self.chunk {
            chunk.remaining() > 0
        } else {
            false
        }
    }

    #[inline]
    fn check(expect: u64, actual: u64) -> Result<()> {
        match actual.cmp(&expect) {
            Ordering::Equal => Ok(()),
            Ordering::Less => Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!("reader got too less data, expect: {expect}, actual: {actual}"),
            )),
            Ordering::Greater => Err(Error::new(
                ErrorKind::Other,
                format!("reader got too much data, expect: {expect}, actual: {actual}"),
            )),
        }
    }
}

impl<S> Stream for IntoReader<S>
where
    S: input::Stream,
{
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl<S> AsyncRead for IntoReader<S>
where
    S: input::Stream,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let inner_buf = match self.as_mut().poll_fill_buf(cx) {
            Poll::Ready(Ok(buf)) => buf,
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        };
        let len = min(inner_buf.len(), buf.len());
        buf.put_slice(&inner_buf[..len]);

        // Make read has been updated.
        self.read += len as u64;
        // len == 0 means we are reaching the EOF, let go check it.
        if len == 0 {
            if let Some(size) = self.size {
                Self::check(size, self.read)?;
            }
        }

        self.consume(len);
        Poll::Ready(Ok(len))
    }
}

impl<S> AsyncBufRead for IntoReader<S>
where
    S: input::Stream,
{
    fn poll_fill_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        loop {
            if self.as_mut().has_chunk() {
                let buf = self.project().chunk.as_ref().unwrap().chunk();
                return Poll::Ready(Ok(buf));
            } else {
                match self.as_mut().project().stream.poll_next(cx) {
                    Poll::Ready(Some(Ok(chunk))) => {
                        *self.as_mut().project().chunk = Some(chunk);
                    }
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                    Poll::Ready(None) => return Poll::Ready(Ok(&[])),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        if amt == 0 {
            return;
        }

        self.project()
            .chunk
            .as_mut()
            .expect("No chunk present")
            .advance(amt);
    }
}

#[cfg(test)]
mod tests {
    use std::io::Error;

    use futures::stream;
    use futures::AsyncReadExt;
    use rand::prelude::*;

    use super::*;

    #[tokio::test]
    async fn test_into_reader() {
        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        let stream_content = content.clone();
        let s = Box::pin(stream::once(async {
            Ok::<_, Error>(Bytes::from(stream_content))
        }));
        let mut r = from_stream(s, Some(size as u64));

        let mut bs = Vec::new();
        r.read_to_end(&mut bs).await.expect("read must succeed");

        assert_eq!(bs, content)
    }

    #[tokio::test]
    async fn test_into_reader_unexpected_eof() {
        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        let stream_content = content.clone();
        let s = Box::pin(stream::once(async {
            Ok::<_, Error>(Bytes::from(stream_content))
        }));
        // Size is larger then expected.
        let mut r = from_stream(s, Some(size as u64 + 1));

        let mut bs = Vec::new();
        let err = r
            .read_to_end(&mut bs)
            .await
            .expect_err("must returning error");

        assert_eq!(err.kind(), ErrorKind::UnexpectedEof)
    }
}
