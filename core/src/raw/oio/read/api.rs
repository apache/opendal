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
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::Future;
use tokio::io::ReadBuf;

use crate::raw::BoxedFuture;
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
pub type Reader = Box<dyn DynRead>;

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
    fn read(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<usize>> + Send;

    /// Seek asynchronously.
    ///
    /// Returns `Unsupported` error if underlying reader doesn't support seek.
    fn seek(&mut self, pos: io::SeekFrom) -> impl Future<Output = Result<u64>> + Send;

    /// Fetch more bytes from underlying reader.
    ///
    /// `size` is used to hint the data that user want to read at most. Implementer
    /// MUST NOT return more than `size` bytes. However, implementer can decide
    /// whether to split or merge the read requests underground.
    ///
    /// Returning `bytes`'s `length == 0` means:
    ///
    /// - This reader has reached its “end of file” and will likely no longer be able to produce bytes.
    /// - The `size` specified was `0`.
    fn next_v2(&mut self, size: usize) -> impl Future<Output = Result<Bytes>> + Send;
}

impl Read for () {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let _ = buf;

        unimplemented!("poll_read is required to be implemented for oio::Read")
    }

    async fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        let (_) = (pos);

        Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support seeking",
        ))
    }

    async fn next_v2(&mut self, size: usize) -> Result<Bytes> {
        let _ = size;

        Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support streaming",
        ))
    }
}

/// Impl ReadExt for all T: Read
impl<T: Read> ReadExt for T {}

/// Extension of [`Read`] to make it easier for use.
pub trait ReadExt: Read {
    /// Build a future for `read_to_end`.
    fn read_to_end<'a>(&'a mut self, buf: &'a mut Vec<u8>) -> ReadToEndFuture<'a, Self> {
        ReadToEndFuture { reader: self, buf }
    }
}

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
        todo!()
        // let this = self.get_mut();
        // let start_len = this.buf.len();
        //
        // loop {
        //     if this.buf.len() == this.buf.capacity() {
        //         this.buf.reserve(32); // buf is full, need more space
        //     }
        //
        //     let spare = this.buf.spare_capacity_mut();
        //     let mut read_buf: ReadBuf = ReadBuf::uninit(spare);
        //
        //     // SAFETY: These bytes were initialized but not filled in the previous loop
        //     unsafe {
        //         read_buf.assume_init(read_buf.capacity());
        //     }
        //
        //     match ready!(this.reader.poll_read(cx, read_buf.initialize_unfilled())) {
        //         Ok(0) => {
        //             return Poll::Ready(Ok(this.buf.len() - start_len));
        //         }
        //         Ok(n) => {
        //             // SAFETY: Read API makes sure that returning `n` is correct.
        //             unsafe {
        //                 this.buf.set_len(this.buf.len() + n);
        //             }
        //         }
        //         Err(e) => return Poll::Ready(Err(e)),
        //     }
        // }
    }
}

pub trait DynRead: Unpin + Send + Sync {
    fn dyn_read<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxedFuture<'a, Result<usize>>;

    fn dyn_seek(&mut self, pos: io::SeekFrom) -> BoxedFuture<Result<u64>>;

    fn dyn_next_v2(&mut self, size: usize) -> BoxedFuture<Result<Bytes>>;
}

impl<T: Read + ?Sized> DynRead for T {
    fn dyn_read<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxedFuture<'a, Result<usize>> {
        Box::pin(self.read(buf))
    }

    fn dyn_seek(&mut self, pos: SeekFrom) -> BoxedFuture<Result<u64>> {
        Box::pin(self.seek(pos))
    }

    fn dyn_next_v2(&mut self, size: usize) -> BoxedFuture<Result<Bytes>> {
        Box::pin(self.next_v2(size))
    }
}

impl<T: DynRead + ?Sized> Read for Box<T> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.dyn_read(buf).await
    }

    async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.dyn_seek(pos).await
    }

    async fn next_v2(&mut self, size: usize) -> Result<Bytes> {
        self.dyn_next_v2(size).await
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
