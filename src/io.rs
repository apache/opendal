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

use std::future::Future;
use std::io;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use bytes::Buf;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::ready;
use futures::AsyncRead;
use futures::AsyncSeek;
use futures::AsyncWrite;
use futures::Sink;
use futures::SinkExt;
use futures::Stream;
use futures::TryStreamExt;

use crate::error::Error;
use crate::error::Result;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::Metadata;

/// BoxedAsyncReader is a boxed AsyncRead.
pub type BoxedAsyncReader = Box<dyn AsyncRead + Unpin + Send>;
/// BytesStream represents a stream of bytes.
pub type BytesStream = Box<dyn Stream<Item = Result<Bytes>> + Unpin + Send>;
/// BytesSink represents a sink of bytes.
pub type BytesSink = Box<dyn Sink<Bytes, Error = Error> + Unpin + Send>;

/// Reader is used for reading data from underlying backend.
///
/// # Lazy Stat
///
/// We will fetch the object's content-length while the first time
/// caller try to seek with `SeekFrom::End(pos)` and the total size is `None`.
pub struct Reader {
    acc: Arc<dyn Accessor>,
    path: String,
    offset: Option<u64>,
    size: Option<u64>,

    pos: u64,
    state: ReadState,
}

enum ReadState {
    Idle,
    Sending(BoxFuture<'static, Result<BytesStream>>),
    Seeking(BoxFuture<'static, Result<Metadata>>),
    Reading(BoxedAsyncReader),
}

impl Reader {
    pub fn new(acc: Arc<dyn Accessor>, path: &str, offset: Option<u64>, size: Option<u64>) -> Self {
        Self {
            acc,
            path: path.to_string(),
            offset,
            size,

            pos: 0,
            state: ReadState::Idle,
        }
    }

    fn current_offset(&self) -> u64 {
        self.offset.unwrap_or_default() + self.pos
    }

    fn current_size(&self) -> Option<u64> {
        self.size.map(|v| v - self.pos)
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut self.state {
            ReadState::Idle => {
                let acc = self.acc.clone();
                let op = OpRead {
                    path: self.path.to_string(),
                    offset: Some(self.current_offset()),
                    size: self.current_size(),
                };

                let future = async move { acc.read(&op).await };

                self.state = ReadState::Sending(Box::pin(future));
                self.poll_read(cx, buf)
            }
            ReadState::Sending(future) => match ready!(Pin::new(future).poll(cx)) {
                Ok(r) => {
                    self.state = ReadState::Reading(Box::new(
                        r.map_err(std::io::Error::from).into_async_read(),
                    ));
                    self.poll_read(cx, buf)
                }
                Err(e) => Poll::Ready(Err(io::Error::from(e))),
            },
            ReadState::Reading(r) => match ready!(Pin::new(r).poll_read(cx, buf)) {
                Ok(n) => {
                    self.pos += n as u64;
                    Poll::Ready(Ok(n))
                }
                Err(e) => Poll::Ready(Err(e)),
            },
            _ => unreachable!("read while seeking is invalid"),
        }
    }
}

impl AsyncSeek for Reader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        if let ReadState::Seeking(future) = &mut self.state {
            match ready!(Pin::new(future).poll(cx)) {
                Ok(meta) => {
                    self.size = Some(meta.content_length() - self.offset.unwrap_or_default())
                }
                Err(e) => return Poll::Ready(Err(io::Error::from(e))),
            }
        }

        let cur = self.pos as i64;
        let cur = match pos {
            SeekFrom::Start(off) => off as i64,
            SeekFrom::Current(off) => cur + off,
            SeekFrom::End(off) => {
                // Stat the object to get it's content-length.
                if self.size.is_none() {
                    let acc = self.acc.clone();
                    let op = OpStat::new(&self.path);

                    let future = async move { acc.stat(&op).await };

                    self.state = ReadState::Seeking(Box::pin(future));
                    return self.poll_seek(cx, pos);
                }

                let total_size = self.size.expect("must have valid total_size");

                total_size as i64 + off
            }
        };

        self.pos = cur as u64;

        self.state = ReadState::Idle;
        Poll::Ready(Ok(self.pos))
    }
}

/// Writer is used to write data into underlying backend.
///
/// # TODO
///
/// maybe we can implement `AsyncWrite` for `Writer`
pub struct Writer {
    acc: Arc<dyn Accessor>,
    path: String,
}

impl Writer {
    pub fn new(acc: Arc<dyn Accessor>, path: &str) -> Self {
        Self {
            acc,
            path: path.to_string(),
        }
    }

    pub async fn write_bytes(self, bs: Vec<u8>) -> Result<usize> {
        let op = &OpWrite {
            path: self.path.clone(),
            size: bs.len() as u64,
        };
        let mut s = self.acc.write(op).await?;
        s.feed(Bytes::from(bs)).await?;
        s.close().await?;

        Ok(op.size as usize)
    }
    pub async fn write_reader(self, mut r: BoxedAsyncReader, size: u64) -> Result<usize> {
        let op = &OpWrite {
            path: self.path.clone(),
            size,
        };
        let s = self.acc.write(op).await?;
        let mut w = into_write(s);
        Ok(futures::io::copy(&mut r, &mut w)
            .await
            .map_err(|e| Error::Unexpected(anyhow!(e)))? as usize)
    }
}

pub fn into_sink<W: AsyncWrite + Send + Unpin>(w: W) -> IntoSink<W> {
    IntoSink {
        w,
        b: bytes::Bytes::new(),
    }
}

pub struct IntoSink<W: AsyncWrite + Send + Unpin> {
    w: W,
    b: bytes::Bytes,
}

impl<W> Sink<Bytes> for IntoSink<W>
where
    W: AsyncWrite + Send + Unpin,
{
    type Error = Error;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        while !self.b.is_empty() {
            let b = &self.b.clone();
            let n = ready!(Pin::new(&mut self.w).poll_write(cx, b))
                .map_err(|e| Error::Unexpected(anyhow!(e)))?;
            self.b.advance(n);
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> std::result::Result<(), Self::Error> {
        self.b = item;
        Ok(())
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        while !self.b.is_empty() {
            let b = &self.b.clone();
            let n = ready!(Pin::new(&mut self.w).poll_write(cx, b))
                .map_err(|e| Error::Unexpected(anyhow!(e)))?;
            self.b.advance(n);
        }

        Pin::new(&mut self.w)
            .poll_flush(cx)
            .map_err(|e| Error::Unexpected(anyhow!(e)))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

pub fn into_write<S: Sink<Bytes, Error = Error> + Send + Unpin>(s: S) -> IntoWrite<S> {
    IntoWrite { s }
}

pub struct IntoWrite<S: Sink<Bytes, Error = Error> + Send + Unpin> {
    s: S,
}

impl<S> AsyncWrite for IntoWrite<S>
where
    S: Sink<Bytes, Error = Error> + Send + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        ready!(Pin::new(&mut self.s).poll_ready(cx))?;

        let size = buf.len();
        Pin::new(&mut self.s).start_send(Bytes::copy_from_slice(buf))?;
        Poll::Ready(Ok(size))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.s)
            .poll_flush(cx)
            .map_err(io::Error::from)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.s)
            .poll_close(cx)
            .map_err(io::Error::from)
    }
}
