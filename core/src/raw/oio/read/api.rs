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

use std::fmt::Display;
use std::fmt::Formatter;
use std::io;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::Future;
use pin_project::pin_project;
use tokio::io::ReadBuf;

use crate::*;

/// PageOperation is the name for APIs of lister.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReadOperation {
    /// Operation for [`Read::poll_read`]
    Read,
    /// Operation for [`Read::poll_seek`]
    Seek,
    /// Operation for [`Read::poll_next`]
    Next,
    /// Operation for [`BlockingRead::read`]
    BlockingRead,
    /// Operation for [`BlockingRead::seek`]
    BlockingSeek,
    /// Operation for [`BlockingRead::next`]
    BlockingNext,
}

impl ReadOperation {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl Display for ReadOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.into_static())
    }
}

impl From<ReadOperation> for &'static str {
    fn from(v: ReadOperation) -> &'static str {
        use ReadOperation::*;

        match v {
            Read => "Reader::read",
            Seek => "Reader::seek",
            Next => "Reader::next",
            BlockingRead => "BlockingReader::read",
            BlockingSeek => "BlockingReader::seek",
            BlockingNext => "BlockingReader::next",
        }
    }
}

/// Reader is a type erased [`Read`].
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
    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>>;

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

        unimplemented!("poll_read is required to be implemented for oio::Read")
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
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

/// `Box<dyn Read>` won't implement `Read` automatically. To make Reader
/// work as expected, we must add this impl.
impl<T: Read + ?Sized> Read for Box<T> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        (**self).poll_read(cx, buf)
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
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
    ) -> Poll<io::Result<usize>> {
        let this: &mut dyn Read = &mut *self;
        this.poll_read(cx, buf).map_err(format_io_error)
    }
}

impl futures::AsyncSeek for dyn Read {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: io::SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let this: &mut dyn Read = &mut *self;
        this.poll_seek(cx, pos).map_err(format_io_error)
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
        ReadFuture { reader: self, buf }
    }

    /// Build a future for `poll_seek`.
    fn seek(&mut self, pos: io::SeekFrom) -> SeekFuture<'_, Self> {
        SeekFuture { reader: self, pos }
    }

    /// Build a future for `poll_next`
    fn next(&mut self) -> NextFuture<'_, Self> {
        NextFuture { reader: self }
    }

    /// Build a future for `read_to_end`.
    fn read_to_end<'a>(&'a mut self, buf: &'a mut Vec<u8>) -> ReadToEndFuture<'a, Self> {
        ReadToEndFuture { reader: self, buf }
    }
}

/// Make this future `!Unpin` for compatibility with async trait methods.
#[pin_project(!Unpin)]
pub struct ReadFuture<'a, R: Read + Unpin + ?Sized> {
    reader: &'a mut R,
    buf: &'a mut [u8],
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

/// Make this future `!Unpin` for compatibility with async trait methods.
#[pin_project(!Unpin)]
pub struct SeekFuture<'a, R: Read + Unpin + ?Sized> {
    reader: &'a mut R,
    pos: io::SeekFrom,
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

/// Make this future `!Unpin` for compatibility with async trait methods.
#[pin_project(!Unpin)]
pub struct NextFuture<'a, R: Read + Unpin + ?Sized> {
    reader: &'a mut R,
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

/// Make this future `!Unpin` for compatibility with async trait methods.
#[pin_project(!Unpin)]
pub struct ReadToEndFuture<'a, R: Read + Unpin + ?Sized> {
    reader: &'a mut R,
    buf: &'a mut Vec<u8>,
}

impl<R> Future for ReadToEndFuture<'_, R>
where
    R: Read + Unpin + ?Sized,
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<usize>> {
        let this = self.project();
        let start_len = this.buf.len();

        loop {
            if this.buf.len() == this.buf.capacity() {
                this.buf.reserve(32); // buf is full, need more space
            }

            let spare = this.buf.spare_capacity_mut();
            let mut read_buf: ReadBuf = ReadBuf::uninit(spare);

            // SAFETY: These bytes were initialized but not filled in the previous loop
            unsafe {
                read_buf.assume_init(read_buf.capacity());
            }

            match ready!(this.reader.poll_read(cx, read_buf.initialize_unfilled())) {
                Ok(0) => {
                    return Poll::Ready(Ok(this.buf.len() - start_len));
                }
                Ok(n) => {
                    // SAFETY: Read API makes sure that returning `n` is correct.
                    unsafe {
                        this.buf.set_len(this.buf.len() + n);
                    }
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

/// BlockingReader is a boxed dyn `BlockingRead`.
pub type BlockingReader = Box<dyn BlockingRead>;

/// Read is the trait that OpenDAL returns to callers.
///
/// Read is compose of the following trait
///
/// - `Read`
/// - `Seek`
/// - `Iterator<Item = Result<Bytes>>`
///
/// `Read` is required to be implemented, `Seek` and `Iterator`
/// is optional. We use `Read` to make users life easier.
pub trait BlockingRead: Send + Sync {
    /// Read synchronously.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    /// Seek synchronously.
    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64>;

    /// Iterating [`Bytes`] from underlying reader.
    fn next(&mut self) -> Option<Result<Bytes>>;

    /// Read all data of current reader to the end of buf.
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        let start_len = buf.len();
        let start_cap = buf.capacity();

        loop {
            if buf.len() == buf.capacity() {
                buf.reserve(32); // buf is full, need more space
            }

            let spare = buf.spare_capacity_mut();
            let mut read_buf: ReadBuf = ReadBuf::uninit(spare);

            // SAFETY: These bytes were initialized but not filled in the previous loop
            unsafe {
                read_buf.assume_init(read_buf.capacity());
            }

            match self.read(read_buf.initialize_unfilled()) {
                Ok(0) => return Ok(buf.len() - start_len),
                Ok(n) => {
                    // SAFETY: Read API makes sure that returning `n` is correct.
                    unsafe {
                        buf.set_len(buf.len() + n);
                    }
                }
                Err(e) => return Err(e),
            }

            // The buffer might be an exact fit. Let's read into a probe buffer
            // and see if it returns `Ok(0)`. If so, we've avoided an
            // unnecessary doubling of the capacity. But if not, append the
            // probe buffer to the primary buffer and let its capacity grow.
            if buf.len() == buf.capacity() && buf.capacity() == start_cap {
                let mut probe = [0u8; 32];

                match self.read(&mut probe) {
                    Ok(0) => return Ok(buf.len() - start_len),
                    Ok(n) => {
                        buf.extend_from_slice(&probe[..n]);
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    }
}

impl BlockingRead for () {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let _ = buf;

        unimplemented!("read is required to be implemented for oio::BlockingRead")
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        let _ = pos;

        Err(Error::new(
            ErrorKind::Unsupported,
            "output blocking reader doesn't support seeking",
        ))
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        Some(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support iterating",
        )))
    }
}

/// `Box<dyn BlockingRead>` won't implement `BlockingRead` automatically.
/// To make BlockingReader work as expected, we must add this impl.
impl<T: BlockingRead + ?Sized> BlockingRead for Box<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        (**self).read(buf)
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        (**self).seek(pos)
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        (**self).next()
    }
}

impl io::Read for dyn BlockingRead {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let this: &mut dyn BlockingRead = &mut *self;
        this.read(buf).map_err(format_io_error)
    }
}

impl io::Seek for dyn BlockingRead {
    #[inline]
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let this: &mut dyn BlockingRead = &mut *self;
        this.seek(pos).map_err(format_io_error)
    }
}

impl Iterator for dyn BlockingRead {
    type Item = Result<Bytes>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let this: &mut dyn BlockingRead = &mut *self;
        this.next()
    }
}

/// helper functions to format `Error` into `io::Error`.
///
/// This function is added privately by design and only valid in current
/// context (i.e. `oio` crate). We don't want to expose this function to
/// users.
#[inline]
fn format_io_error(err: Error) -> io::Error {
    let kind = match err.kind() {
        ErrorKind::NotFound => io::ErrorKind::NotFound,
        ErrorKind::PermissionDenied => io::ErrorKind::PermissionDenied,
        ErrorKind::InvalidInput => io::ErrorKind::InvalidInput,
        _ => io::ErrorKind::Interrupted,
    };

    io::Error::new(kind, err)
}
