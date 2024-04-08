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
use std::ops::Deref;

use bytes::BufMut;
use bytes::Bytes;
use futures::Future;

use crate::raw::*;
use crate::*;

/// PageOperation is the name for APIs of lister.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReadOperation {
    /// Operation for [`Read::read`]
    Read,
    /// Operation for [`BlockingRead::read`]
    BlockingRead,
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
            BlockingRead => "BlockingReader::read",
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
    /// Read at the given offset with the given limit.
    ///
    /// # Notes
    ///
    /// Storage services should try to read as much as possible, only return bytes less than the
    /// limit while reaching the end of the file.
    #[cfg(not(target_arch = "wasm32"))]
    fn read_at(
        &self,
        buf: &mut oio::WritableBuf,
        offset: u64,
    ) -> impl Future<Output = Result<usize>> + Send;
    #[cfg(target_arch = "wasm32")]
    fn read_at(
        &self,
        buf: &mut oio::WritableBuf,
        offset: u64,
    ) -> impl Future<Output = Result<usize>>;
}

impl Read for () {
    async fn read_at(&self, buf: &mut oio::WritableBuf, offset: u64) -> Result<usize> {
        let _ = (buf, offset);

        Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support streaming",
        ))
    }
}

impl Read for Bytes {
    async fn read_at(&self, mut buf: &mut oio::WritableBuf, offset: u64) -> Result<usize> {
        if offset >= self.len() as u64 {
            return Ok(0);
        }
        let offset = offset as usize;
        let size = buf.remaining_mut().min(self.len() - offset);
        buf.put(self.slice(offset..offset + size));
        Ok(0)
    }
}

pub trait ReadDyn: Unpin + Send + Sync {
    fn read_at_dyn(&self, buf: &mut oio::WritableBuf, offset: u64) -> BoxedFuture<Result<usize>>;
}

impl<T: Read + ?Sized> ReadDyn for T {
    fn read_at_dyn(&self, buf: &mut oio::WritableBuf, offset: u64) -> BoxedFuture<Result<usize>> {
        Box::pin(self.read_at(buf, offset))
    }
}

/// # NOTE
///
/// Take care about the `deref_mut()` here. This makes sure that we are calling functions
/// upon `&mut T` instead of `&mut Box<T>`. The later could result in infinite recursion.
impl<T: ReadDyn + ?Sized> Read for Box<T> {
    async fn read_at(&self, buf: &mut oio::WritableBuf, offset: u64) -> Result<usize> {
        self.deref().read_at_dyn(buf, offset).await
    }
}

/// BlockingReader is a boxed dyn `BlockingRead`.
pub type BlockingReader = Box<dyn BlockingRead>;

/// Read is the trait that OpenDAL returns to callers.
pub trait BlockingRead: Send + Sync {
    /// Read data from the reader at the given offset with the given limit.
    ///
    /// # Notes
    ///
    /// Storage services should try to read as much as possible, only return bytes less than the
    /// limit while reaching the end of the file.
    fn read_at(&self, buf: &mut oio::WritableBuf, offset: u64) -> Result<usize>;
}

impl BlockingRead for () {
    fn read_at(&self, buf: &mut oio::WritableBuf, offset: u64) -> Result<usize> {
        let _ = (buf, offset);

        unimplemented!("read is required to be implemented for oio::BlockingRead")
    }
}

impl BlockingRead for Bytes {
    fn read_at(&self, mut buf: &mut oio::WritableBuf, offset: u64) -> Result<usize> {
        if offset >= self.len() as u64 {
            return Ok(0);
        }
        let offset = offset as usize;
        let size = buf.remaining_mut().min(self.len() - offset);
        buf.put(self.slice(offset..offset + size));
        Ok(size)
    }
}

/// `Box<dyn BlockingRead>` won't implement `BlockingRead` automatically.
/// To make BlockingReader work as expected, we must add this impl.
impl<T: BlockingRead + ?Sized> BlockingRead for Box<T> {
    fn read_at(&self, buf: &mut oio::WritableBuf, offset: u64) -> Result<usize> {
        (**self).read_at(buf, offset)
    }
}
