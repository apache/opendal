// Copyright 2022 Datafuse Labs.
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
use std::io;
use std::io::Read;
use std::io::Write;
use std::task::Context;
use std::task::Poll;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use futures::ready;
use futures::StreamExt;

use crate::raw::*;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

/// Body used in blocking HTTP requests.
pub enum Body {
    /// An empty body.
    Empty,
    /// Body with bytes.
    Bytes(Bytes),
    /// Body with a Reader.
    Reader(input::BlockingReader),
}

impl Default for Body {
    fn default() -> Self {
        Body::Empty
    }
}

impl Body {
    /// Consume the entire body.
    pub fn consume(self) -> Result<()> {
        if let Body::Reader(mut r) = self {
            std::io::copy(&mut r, &mut std::io::sink()).map_err(|err| {
                Error::new(ErrorKind::Unexpected, "consuming response")
                    .with_operation("http_util::Body::consume")
                    .set_source(err)
            })?;
        }

        Ok(())
    }
}

impl Read for Body {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Body::Empty => Ok(0),
            Body::Bytes(bs) => {
                let size = min(bs.len(), buf.len());
                let rbs = bs.split_to(size);
                bs.advance(size);

                buf.write_all(&rbs).expect("write all must succeed");
                Ok(size)
            }
            Body::Reader(r) => r.read(buf),
        }
    }
}

/// Body used in async HTTP requests.
pub enum AsyncBody {
    /// An empty body.
    Empty,
    /// Body with bytes.
    Bytes(Bytes),
    /// Body with a Reader.
    Reader(input::Reader),
    /// Body with a multipart field.
    ///
    /// If input with this field, we will goto the internal multipart
    /// handle logic.
    Multipart(String, input::Reader),
}

impl Default for AsyncBody {
    fn default() -> Self {
        AsyncBody::Empty
    }
}

impl From<AsyncBody> for reqwest::Body {
    fn from(v: AsyncBody) -> Self {
        match v {
            AsyncBody::Empty => reqwest::Body::from(""),
            AsyncBody::Bytes(bs) => reqwest::Body::from(bs),
            AsyncBody::Reader(r) => reqwest::Body::wrap_stream(into_stream(r, 16 * 1024)),
            AsyncBody::Multipart(_, _) => {
                unreachable!("reqwest multipart should not be constructed by body")
            }
        }
    }
}

/// IncomingAsyncBody carries the content returned by remote servers.
///
/// # Notes
///
/// Client SHOULD NEVER construct this body.
pub struct IncomingAsyncBody {
    inner: BytesStreamer,
    size: Option<u64>,
    read: u64,
    chunk: Option<Bytes>,
}

impl IncomingAsyncBody {
    /// Construct a new incoming async body
    pub fn new(s: BytesStreamer, size: Option<u64>) -> Self {
        Self {
            inner: s,
            size,
            read: 0,
            chunk: None,
        }
    }

    /// Consume the entire body.
    pub async fn consume(mut self) -> Result<()> {
        use output::ReadExt;

        while let Some(bs) = self.next().await {
            bs.map_err(|err| {
                Error::new(ErrorKind::Unexpected, "fetch bytes from stream")
                    .with_operation("http_util::IncomingAsyncBody::consume")
                    .set_source(err)
            })?;
        }

        Ok(())
    }

    /// Consume the response to bytes.
    ///
    /// Borrowed from hyper's [`to_bytes`](https://docs.rs/hyper/latest/hyper/body/fn.to_bytes.html).
    pub async fn bytes(mut self) -> Result<Bytes> {
        use output::ReadExt;

        let poll_next_error = |err: io::Error| {
            Error::new(ErrorKind::Unexpected, "fetch bytes from stream")
                .with_operation("http_util::IncomingAsyncBody::bytes")
                .set_source(err)
        };

        // If there's only 1 chunk, we can just return Buf::to_bytes()
        let mut first = if let Some(buf) = self.next().await {
            buf.map_err(poll_next_error)?
        } else {
            return Ok(Bytes::new());
        };

        let second = if let Some(buf) = self.next().await {
            buf.map_err(poll_next_error)?
        } else {
            return Ok(first.copy_to_bytes(first.remaining()));
        };

        // With more than 1 buf, we gotta flatten into a Vec first.
        let cap = first.remaining() + second.remaining() + self.size.unwrap_or_default() as usize;
        let mut vec = Vec::with_capacity(cap);
        vec.put(first);
        vec.put(second);

        while let Some(buf) = self.next().await {
            vec.put(buf.map_err(poll_next_error)?);
        }

        Ok(vec.into())
    }

    /// Consume the response to build a reader.
    pub fn reader(self) -> output::Reader {
        Box::new(self)
    }

    #[inline]
    fn check(expect: u64, actual: u64) -> io::Result<()> {
        match actual.cmp(&expect) {
            Ordering::Equal => Ok(()),
            Ordering::Less => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("reader got too less data, expect: {expect}, actual: {actual}"),
            )),
            Ordering::Greater => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("reader got too much data, expect: {expect}, actual: {actual}"),
            )),
        }
    }
}

impl output::Read for IncomingAsyncBody {
    fn poll_read(&mut self, cx: &mut Context<'_>, mut buf: &mut [u8]) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let mut bs = match self.chunk.take() {
            Some(bs) if !bs.is_empty() => bs,
            _ => match ready!(self.poll_next(cx)) {
                Some(Ok(bs)) => bs,
                Some(Err(err)) => return Poll::Ready(Err(err)),
                None => return Poll::Ready(Ok(0)),
            },
        };

        let amt = min(bs.len(), buf.len());
        buf.put_slice(&bs[..amt]);
        bs.advance(amt);
        if !bs.is_empty() {
            self.chunk = Some(bs);
        }

        Poll::Ready(Ok(amt))
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Bytes>>> {
        if let Some(bs) = self.chunk.take() {
            self.read += bs.len() as u64;
            return Poll::Ready(Some(Ok(bs)));
        }

        let res = match ready!(self.inner.poll_next_unpin(cx)) {
            Some(Ok(bs)) => {
                self.read += bs.len() as u64;
                Some(Ok(bs))
            }
            Some(Err(err)) => Some(Err(err)),
            None => {
                if let Some(size) = self.size {
                    Self::check(size, self.read)?;
                }

                None
            }
        };

        Poll::Ready(res)
    }
}
