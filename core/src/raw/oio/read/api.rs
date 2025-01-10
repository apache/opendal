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

use std::mem;
use std::ops::DerefMut;

use bytes::Bytes;
use futures::Future;

use crate::raw::*;
use crate::*;

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
    /// Read at the given offset with the given size.
    fn read(&mut self) -> impl Future<Output = Result<Buffer>> + MaybeSend;

    /// Read all data from the reader.
    fn read_all(&mut self) -> impl Future<Output = Result<Buffer>> + MaybeSend {
        async {
            let mut bufs = vec![];
            loop {
                match self.read().await {
                    Ok(buf) if buf.is_empty() => break,
                    Ok(buf) => bufs.push(buf),
                    Err(err) => return Err(err),
                }
            }
            Ok(bufs.into_iter().flatten().collect())
        }
    }
}

impl Read for () {
    async fn read(&mut self) -> Result<Buffer> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support read",
        ))
    }
}

impl Read for Bytes {
    async fn read(&mut self) -> Result<Buffer> {
        Ok(Buffer::from(self.split_off(0)))
    }
}

impl Read for Buffer {
    async fn read(&mut self) -> Result<Buffer> {
        Ok(mem::take(self))
    }
}

/// ReadDyn is the dyn version of [`Read`] make it possible to use as
/// `Box<dyn ReadDyn>`.
pub trait ReadDyn: Unpin + Send + Sync {
    /// The dyn version of [`Read::read`].
    ///
    /// This function returns a boxed future to make it object safe.
    fn read_dyn(&mut self) -> BoxedFuture<Result<Buffer>>;

    /// The dyn version of [`Read::read_all`]
    fn read_all_dyn(&mut self) -> BoxedFuture<Result<Buffer>>;
}

impl<T: Read + ?Sized> ReadDyn for T {
    fn read_dyn(&mut self) -> BoxedFuture<Result<Buffer>> {
        Box::pin(self.read())
    }

    fn read_all_dyn(&mut self) -> BoxedFuture<Result<Buffer>> {
        Box::pin(self.read_all())
    }
}

/// # NOTE
///
/// Take care about the `deref_mut()` here. This makes sure that we are calling functions
/// upon `&mut T` instead of `&mut Box<T>`. The later could result in infinite recursion.
impl<T: ReadDyn + ?Sized> Read for Box<T> {
    async fn read(&mut self) -> Result<Buffer> {
        self.deref_mut().read_dyn().await
    }

    async fn read_all(&mut self) -> Result<Buffer> {
        self.deref_mut().read_all_dyn().await
    }
}

/// BlockingReader is a arc dyn `BlockingRead`.
pub type BlockingReader = Box<dyn BlockingRead>;

/// Read is the trait that OpenDAL returns to callers.
pub trait BlockingRead: Send + Sync {
    /// Read data from the reader at the given offset with the given size.
    fn read(&mut self) -> Result<Buffer>;
}

impl BlockingRead for () {
    fn read(&mut self) -> Result<Buffer> {
        unimplemented!("read is required to be implemented for oio::BlockingRead")
    }
}

impl BlockingRead for Bytes {
    fn read(&mut self) -> Result<Buffer> {
        Ok(Buffer::from(self.split_off(0)))
    }
}

impl BlockingRead for Buffer {
    fn read(&mut self) -> Result<Buffer> {
        Ok(mem::take(self))
    }
}

/// `Arc<dyn BlockingRead>` won't implement `BlockingRead` automatically.
/// To make BlockingReader work as expected, we must add this impl.
impl<T: BlockingRead + ?Sized> BlockingRead for Box<T> {
    fn read(&mut self) -> Result<Buffer> {
        (**self).read()
    }
}
