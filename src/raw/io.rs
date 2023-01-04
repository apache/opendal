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

use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Result;
use std::io::Seek;
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::AsyncRead;
use futures::AsyncSeek;
use futures::AsyncWrite;
use futures::Sink;
use futures::Stream;

/// BytesRead represents a reader of bytes.
pub trait BytesRead: AsyncRead + Unpin + Send {}
impl<T> BytesRead for T where T: AsyncRead + Unpin + Send {}

/// BytesReader is a boxed dyn [`BytesRead`].
pub type BytesReader = Box<dyn BytesRead>;

/// OutputBytesRead is the output version of bytes returned by OpenDAL.
///
/// OutputBytesRead is compose of the following trait
///
/// - `AsyncRead`
/// - `AsyncSeek`
/// - `Stream<Item = Result<Bytes>>`
///
/// `AsyncRead` is required to be implemented, `AsyncSeek` and `Stream`
/// is optional. We use `OutputBytesRead` to make users life easier.
pub trait OutputBytesRead: Unpin + Send + Sync {
    /// Return the inner output bytes reader if there is one.
    fn inner(&mut self) -> Option<&mut OutputBytesReader> {
        None
    }

    /// Read bytes asynchronously.
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        match self.inner() {
            Some(v) => v.poll_read(cx, buf),
            None => unimplemented!("poll_read is required to be implemented for OutputBytesRead"),
        }
    }

    /// Check if this reader is seekable.
    fn is_seekable(&mut self) -> bool {
        match self.inner() {
            Some(v) => v.is_seekable(),
            None => false,
        }
    }

    /// Seek asynchronously.
    ///
    /// Returns `Unsupported` error if underlying reader doesn't support seek.
    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        match self.inner() {
            Some(v) => v.poll_seek(cx, pos),
            None => Poll::Ready(Err(Error::new(
                ErrorKind::Unsupported,
                "output reader doesn't support seeking",
            ))),
        }
    }

    /// Check if this reader is streamable.
    fn is_streamable(&mut self) -> bool {
        match self.inner() {
            Some(v) => v.is_streamable(),
            None => false,
        }
    }

    /// Stream [`Bytes`] from underlying reader.
    ///
    /// Returns `Unsupported` error if underlying reader doesn't support stream.
    ///
    /// This API exists for avoiding bytes copying inside async runtime.
    /// Users can poll bytes from underlying reader and decide when to
    /// read/consume them.
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match self.inner() {
            Some(v) => v.poll_next(cx),
            None => Poll::Ready(Some(Err(Error::new(
                ErrorKind::Unsupported,
                "output reader doesn't support streaming",
            )))),
        }
    }
}

/// OutputBytesReader is a boxed dyn [`OutputBytesRead`].
pub type OutputBytesReader = Box<dyn OutputBytesRead>;

impl AsyncRead for dyn OutputBytesRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        let this: &mut dyn OutputBytesRead = &mut *self;
        this.poll_read(cx, buf)
    }
}

impl Stream for dyn OutputBytesRead {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this: &mut dyn OutputBytesRead = &mut *self;
        this.poll_next(cx)
    }
}

/// SeekableOutputBytesReader is a wrapper for seekable bytes reader.
pub struct SeekableOutputBytesReader<R: AsyncRead + AsyncSeek + Unpin + Send + Sync>(R);

impl<R> From<R> for SeekableOutputBytesReader<R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send + Sync,
{
    fn from(r: R) -> Self {
        SeekableOutputBytesReader(r)
    }
}

impl<R> OutputBytesRead for SeekableOutputBytesReader<R> where
    R: AsyncRead + AsyncSeek + Unpin + Send + Sync
{
}

/// LimitedBytesReader is a wrapper for limited bytes reader.
pub struct LimitedBytesReader<R: AsyncRead + AsyncSeek + Unpin + Send + Sync> {
    _inner: R,
    _limit: u64,
}

impl<R> LimitedBytesReader<R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send + Sync,
{
    /// Create a new limited bytes reader.
    pub fn new(inner: R, limit: u64) -> Self {
        Self {
            _inner: inner,
            _limit: limit,
        }
    }
}

impl<R> OutputBytesRead for LimitedBytesReader<R> where
    R: AsyncRead + AsyncSeek + Unpin + Send + Sync
{
}

/// BlockingBytesRead represents a blocking reader of bytes.
pub trait BlockingBytesRead: Read + Send {}
impl<T> BlockingBytesRead for T where T: Read + Send {}

/// BlockingBytesReader is a boxed dyn [`BlockingBytesRead`].
pub type BlockingBytesReader = Box<dyn BlockingBytesRead>;

/// BlockingOutputBytesRead is the output version of bytes reader
/// returned by OpenDAL.
pub trait BlockingOutputBytesRead: BlockingBytesRead + Sync {}
impl<T> BlockingOutputBytesRead for T where T: BlockingBytesRead + Sync {}

/// BlockingOutputBytesReader is a boxed dyn `BlockingOutputBytesRead`.
pub type BlockingOutputBytesReader = Box<dyn BlockingOutputBytesRead>;

/// BytesHandle represents a handle of bytes which can be read and seek.
pub trait BytesHandle: AsyncRead + AsyncSeek + Unpin + Send {}
impl<T> BytesHandle for T where T: AsyncRead + AsyncSeek + Unpin + Send {}

/// BytesHandler is a boxed dyn `BytesHandle`.
pub type BytesHandler = Box<dyn BytesHandle>;

/// BytesHandle represents a handle of bytes which can be read an seek.
pub trait BlockingBytesHandle: Read + Seek + Send {}
impl<T> BlockingBytesHandle for T where T: Read + Seek + Send {}

/// BlockingBytesHandler is a boxed dyn `BlockingBytesHandle`.
pub type BlockingBytesHandler = Box<dyn BlockingBytesHandle>;

/// BytesWrite represents a writer of bytes.
pub trait BytesWrite: AsyncWrite + Unpin + Send {}
impl<T> BytesWrite for T where T: AsyncWrite + Unpin + Send {}

/// BytesWriter is a boxed dyn [`BytesWrite`].
pub type BytesWriter = Box<dyn BytesWrite>;

/// BytesStream represents a stream of bytes.
///
/// This trait is used as alias to `Stream<Item = Result<Bytes>> + Unpin + Send`.
pub trait BytesStream: Stream<Item = Result<Bytes>> + Unpin + Send + Sync {}
impl<T> BytesStream for T where T: Stream<Item = Result<Bytes>> + Unpin + Send + Sync {}

/// BytesStreamer is a boxed dyn [`BytesStream`].
pub type BytesStreamer = Box<dyn BytesStream>;

/// BytesSink represents a sink of bytes.
///
/// THis trait is used as alias to `Sink<Bytes, Error = Error> + Unpin + Send`.
pub trait BytesSink: Sink<Bytes, Error = Error> + Unpin + Send {}
impl<T> BytesSink for T where T: Sink<Bytes, Error = Error> + Unpin + Send {}

/// BytesCursor is the cursor for [`Bytes`] that implements `AsyncRead`
/// and `BytesStream`
pub struct BytesCursor {
    inner: Bytes,
    pos: usize,
}

impl BytesCursor {
    /// Returns `true` if the remaining slice is empty.
    pub fn is_empty(&self) -> bool {
        self.pos >= self.inner.len()
    }

    /// Returns the remaining slice.
    pub fn remaining_slice(&self) -> &[u8] {
        let len = self.pos.min(self.inner.len());
        &self.inner.as_ref()[(len)..]
    }
}

impl From<Vec<u8>> for BytesCursor {
    fn from(v: Vec<u8>) -> Self {
        BytesCursor {
            inner: Bytes::from(v),
            pos: 0,
        }
    }
}

impl OutputBytesRead for BytesCursor {
    fn poll_read(&mut self, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        let n = Read::read(&mut self.remaining_slice(), buf)?;
        self.pos += n;
        Poll::Ready(Ok(n))
    }

    fn is_streamable(&mut self) -> bool {
        true
    }

    fn poll_next(&mut self, _: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if self.is_empty() {
            Poll::Ready(None)
        } else {
            let bs = self.inner.split_off(self.pos);
            self.pos += bs.len();
            Poll::Ready(Some(Ok(bs)))
        }
    }
}
