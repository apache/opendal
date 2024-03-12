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
use std::ops::DerefMut;

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
pub type Reader = Box<dyn ReadDyn>;

/// Read is the internal trait used by OpenDAL to read data from storage.
///
/// Users should not use or import this trait unless they are implementing an `Accessor`.
///
/// # Notes
///
/// ## Object Safety
///
/// `Read` uses `async in trait`, making it not object safe, preventing the use of `Box<dyn Read>`.
/// To address this, we've introduced [`ReadDyn`] and its compatible type `Box<dyn ReadDyn>`.
///
/// `ReadDyn` uses `Box::pin()` to transform the returned future into a [`BoxedFuture`], introducing
/// an additional layer of indirection and an extra allocation. Ideally, `ReadDyn` should occur only
/// once, at the outermost level of our API.
pub trait Read: Unpin + Send + Sync {
    /// Fetch more bytes from underlying reader.
    ///
    /// `limit` is used to hint the data that user want to read at most. Implementer
    /// MUST NOT return more than `limit` bytes. However, implementer can decide
    /// whether to split or merge the read requests underground.
    ///
    /// Returning `bytes`'s `length == 0` means:
    ///
    /// - This reader has reached its “end of file” and will likely no longer be able to produce bytes.
    /// - The `limit` specified was `0`.
    #[cfg(not(target_arch = "wasm32"))]
    fn read(&mut self, limit: usize) -> impl Future<Output = Result<Bytes>> + Send;
    #[cfg(target_arch = "wasm32")]
    fn read(&mut self, size: usize) -> impl Future<Output = Result<Bytes>>;

    /// Seek asynchronously.
    ///
    /// Returns `Unsupported` error if underlying reader doesn't support seek.
    #[cfg(not(target_arch = "wasm32"))]
    fn seek(&mut self, pos: io::SeekFrom) -> impl Future<Output = Result<u64>> + Send;
    #[cfg(target_arch = "wasm32")]
    fn seek(&mut self, pos: io::SeekFrom) -> impl Future<Output = Result<u64>>;
}

impl Read for () {
    async fn read(&mut self, limit: usize) -> Result<Bytes> {
        let _ = limit;

        Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support streaming",
        ))
    }

    async fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        let _ = pos;

        Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support seeking",
        ))
    }
}

pub trait ReadDyn: Unpin + Send + Sync {
    fn read_dyn(&mut self, limit: usize) -> BoxedFuture<Result<Bytes>>;

    fn seek_dyn(&mut self, pos: io::SeekFrom) -> BoxedFuture<Result<u64>>;
}

impl<T: Read + ?Sized> ReadDyn for T {
    fn read_dyn(&mut self, limit: usize) -> BoxedFuture<Result<Bytes>> {
        Box::pin(self.read(limit))
    }

    fn seek_dyn(&mut self, pos: io::SeekFrom) -> BoxedFuture<Result<u64>> {
        Box::pin(self.seek(pos))
    }
}

/// # NOTE
///
/// Take care about the `deref_mut()` here. This makes sure that we are calling functions
/// upon `&mut T` instead of `&mut Box<T>`. The later could result in infinite recursion.
impl<T: ReadDyn + ?Sized> Read for Box<T> {
    async fn read(&mut self, limit: usize) -> Result<Bytes> {
        self.deref_mut().read_dyn(limit).await
    }

    async fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        self.deref_mut().seek_dyn(pos).await
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
