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

use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::io::SeekFrom;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::Future;
use pin_project::pin_project;

/// Reader is a boxed dyn [`Read`].
pub type Reader = Box<dyn Read>;

/// Read is the trait that OpenDAL returns to callers.
///
/// Read is compose of the following trait
///
/// - `AsyncRead`
/// - `AsyncSeek`
/// - `Stream<Item = Result<Bytes>>`
///
/// `AsyncRead` is required to be implemented, `AsyncSeek` and `Stream`
/// is optional. We use `Read` to make users life easier.
pub trait Read: Unpin + Send + Sync {
    /// Read bytes asynchronously.
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>>;

    /// Seek asynchronously.
    ///
    /// Returns `Unsupported` error if underlying reader doesn't support seek.
    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>>;

    /// Stream [`Bytes`] from underlying reader.
    ///
    /// Returns `Unsupported` error if underlying reader doesn't support stream.
    ///
    /// This API exists for avoiding bytes copying inside async runtime.
    /// Users can poll bytes from underlying reader and decide when to
    /// read/consume them.
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>>;
}

impl Read for () {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        let (_, _) = (cx, buf);

        unimplemented!("poll_read is required to be implemented for output::Read")
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        let (_, _) = (cx, pos);

        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support seeking",
        )))
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let _ = cx;

        Poll::Ready(Some(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support streaming",
        ))))
    }
}

/// `Box<dyn Read>` won't implement `Read` automanticly. To make Reader
/// work as expected, we must add this impl.
impl<T: Read + ?Sized> Read for Box<T> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        (**self).poll_read(cx, buf)
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        (**self).poll_seek(cx, pos)
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        (**self).poll_next(cx)
    }
}

impl futures::AsyncRead for dyn Read {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        let this: &mut dyn Read = &mut *self;
        this.poll_read(cx, buf)
    }
}

impl futures::AsyncSeek for dyn Read {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<Result<u64>> {
        let this: &mut dyn Read = &mut *self;
        this.poll_seek(cx, pos)
    }
}

impl futures::Stream for dyn Read {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this: &mut dyn Read = &mut *self;
        this.poll_next(cx)
    }
}

/// Impl ReadExt for all T: Read
impl<T: Read> ReadExt for T {}

/// Extension of [`Read`] to make it easier for use.
pub trait ReadExt: Read {
    /// Build a future for `poll_read`.
    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadFuture<'a, Self> {
        ReadFuture {
            reader: self,
            buf,
            _pin: PhantomPinned::default(),
        }
    }

    /// Build a future for `poll_seek`.
    fn seek(&mut self, pos: SeekFrom) -> SeekFuture<'_, Self> {
        SeekFuture {
            reader: self,
            pos,
            _pin: PhantomPinned::default(),
        }
    }

    /// Build a future for `poll_next`
    fn next(&mut self) -> NextFuture<'_, Self> {
        NextFuture {
            reader: self,
            _pin: PhantomPinned::default(),
        }
    }
}

#[pin_project]
pub struct ReadFuture<'a, R: Read + Unpin + ?Sized> {
    reader: &'a mut R,
    buf: &'a mut [u8],
    /// Make this future `!Unpin` for compatibility with async trait methods.
    ///
    /// Borrowed from tokio.
    #[pin]
    _pin: PhantomPinned,
}

impl<R> Future for ReadFuture<'_, R>
where
    R: Read + Unpin + ?Sized,
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<usize>> {
        let this = self.project();
        Pin::new(this.reader).poll_read(cx, this.buf)
    }
}

#[pin_project]
pub struct SeekFuture<'a, R: Read + Unpin + ?Sized> {
    reader: &'a mut R,
    pos: SeekFrom,
    /// Make this future `!Unpin` for compatibility with async trait methods.
    ///
    /// Borrowed from tokio.
    #[pin]
    _pin: PhantomPinned,
}

impl<R> Future for SeekFuture<'_, R>
where
    R: Read + Unpin + ?Sized,
{
    type Output = Result<u64>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<u64>> {
        let this = self.project();
        Pin::new(this.reader).poll_seek(cx, *this.pos)
    }
}

#[pin_project]
pub struct NextFuture<'a, R: Read + Unpin + ?Sized> {
    reader: &'a mut R,
    /// Make this future `!Unpin` for compatibility with async trait methods.
    ///
    /// Borrowed from tokio.
    #[pin]
    _pin: PhantomPinned,
}

impl<R> Future for NextFuture<'_, R>
where
    R: Read + Unpin + ?Sized,
{
    type Output = Option<Result<Bytes>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let this = self.project();
        Pin::new(this.reader).poll_next(cx)
    }
}
