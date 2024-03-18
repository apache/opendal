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

use crate::raw::BoxedFuture;
use crate::*;

/// PageOperation is the name for APIs of lister.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReadOperation {
    /// Operation for [`Read::read`]
    Read,
    /// Operation for [`Read::seek`]
    Seek,
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
    #[cfg(not(target_arch = "wasm32"))]
    fn read_at(&self, offset: u64, limit: usize) -> impl Future<Output = Result<Bytes>> + Send;
    #[cfg(target_arch = "wasm32")]
    fn read_at(&self, offset: u64, limit: usize) -> impl Future<Output = Result<Bytes>>;
}

impl Read for () {
    async fn read_at(&mut self, offset: u64, limit: usize) -> Result<Bytes> {
        let (_, _) = (offset, limit);

        Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support streaming",
        ))
    }
}

pub trait ReadDyn: Unpin + Send + Sync {
    fn read_at_dyn(&self, offset: u64, limit: usize) -> BoxedFuture<Result<Bytes>>;
}

impl<T: Read + ?Sized> ReadDyn for T {
    fn read_at_dyn(&self, offset: u64, limit: usize) -> BoxedFuture<Result<Bytes>> {
        Box::pin(self.read_at(offset, limit))
    }
}

/// # NOTE
///
/// Take care about the `deref_mut()` here. This makes sure that we are calling functions
/// upon `&mut T` instead of `&mut Box<T>`. The later could result in infinite recursion.
impl<T: ReadDyn + ?Sized> Read for Box<T> {
    async fn read_at(&mut self, offset: u64, limit: usize) -> Result<Bytes> {
        self.deref_mut().read_at_dyn(offset, limit).await
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
    fn read(&mut self, limit: usize) -> Result<Bytes>;

    /// Seek synchronously.
    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64>;
}

impl BlockingRead for () {
    fn read(&mut self, limit: usize) -> Result<Bytes> {
        let _ = limit;

        unimplemented!("read is required to be implemented for oio::BlockingRead")
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        let _ = pos;

        Err(Error::new(
            ErrorKind::Unsupported,
            "output blocking reader doesn't support seeking",
        ))
    }
}

/// `Box<dyn BlockingRead>` won't implement `BlockingRead` automatically.
/// To make BlockingReader work as expected, we must add this impl.
impl<T: BlockingRead + ?Sized> BlockingRead for Box<T> {
    fn read(&mut self, limit: usize) -> Result<Bytes> {
        (**self).read(limit)
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        (**self).seek(pos)
    }
}
