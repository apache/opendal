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
use std::future::Future;
use std::ops::DerefMut;

use crate::raw::*;
use crate::*;

/// WriteOperation is the name for APIs of Writer.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
#[non_exhaustive]
pub enum WriteOperation {
    /// Operation for [`Write::write`]
    Write,
    /// Operation for [`Write::close`]
    Close,
    /// Operation for [`Write::abort`]
    Abort,

    /// Operation for [`BlockingWrite::write`]
    BlockingWrite,
    /// Operation for [`BlockingWrite::close`]
    BlockingClose,
}

impl WriteOperation {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl Display for WriteOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.into_static())
    }
}

impl From<WriteOperation> for &'static str {
    fn from(v: WriteOperation) -> &'static str {
        use WriteOperation::*;

        match v {
            Write => "Writer::write",
            Close => "Writer::close",
            Abort => "Writer::abort",

            BlockingWrite => "BlockingWriter::write",
            BlockingClose => "BlockingWriter::close",
        }
    }
}

/// Writer is a type erased [`Write`]
pub type Writer = Box<dyn WriteDyn>;

/// Write is the trait that OpenDAL returns to callers.
pub trait Write: Unpin + Send + Sync {
    /// Write given bytes into writer.
    ///
    /// # Behavior
    ///
    /// - `Ok(n)` means `n` bytes has been written successfully.
    /// - `Err(err)` means error happens and no bytes has been written.
    ///
    /// It's possible that `n < bs.len()`, caller should pass the remaining bytes
    /// repeatedly until all bytes has been written.
    ///
    /// # Safety
    ///
    /// - The caller MUST ensure that the buffer is valid before `write` returns `Ready`.
    /// - The implementor SHOULD NOT store [`oio::ReadableBuf`] in anyways. The buf MUST
    ///   be passed by or copied out to an owned buffer.
    #[cfg(not(target_arch = "wasm32"))]
    unsafe fn write(&mut self, bs: oio::Buffer) -> impl Future<Output = Result<usize>> + Send;
    #[cfg(target_arch = "wasm32")]
    unsafe fn write(&mut self, bs: oio::Buffer) -> impl Future<Output = Result<usize>>;

    /// Close the writer and make sure all data has been flushed.
    #[cfg(not(target_arch = "wasm32"))]
    fn close(&mut self) -> impl Future<Output = Result<()>> + Send;
    #[cfg(target_arch = "wasm32")]
    fn close(&mut self) -> impl Future<Output = Result<()>>;

    /// Abort the pending writer.
    #[cfg(not(target_arch = "wasm32"))]
    fn abort(&mut self) -> impl Future<Output = Result<()>> + Send;
    #[cfg(target_arch = "wasm32")]
    fn abort(&mut self) -> impl Future<Output = Result<()>>;
}

impl Write for () {
    async unsafe fn write(&mut self, _: oio::Buffer) -> Result<usize> {
        unimplemented!("write is required to be implemented for oio::Write")
    }

    async fn close(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support close",
        ))
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support abort",
        ))
    }
}

pub trait WriteDyn: Unpin + Send + Sync {
    fn write_dyn(&mut self, bs: oio::Buffer) -> BoxedFuture<Result<usize>>;

    fn close_dyn(&mut self) -> BoxedFuture<Result<()>>;

    fn abort_dyn(&mut self) -> BoxedFuture<Result<()>>;
}

impl<T: Write + ?Sized> WriteDyn for T {
    fn write_dyn(&mut self, bs: oio::Buffer) -> BoxedFuture<Result<usize>> {
        unsafe { Box::pin(self.write(bs)) }
    }

    fn close_dyn(&mut self) -> BoxedFuture<Result<()>> {
        Box::pin(self.close())
    }

    fn abort_dyn(&mut self) -> BoxedFuture<Result<()>> {
        Box::pin(self.abort())
    }
}

impl<T: WriteDyn + ?Sized> Write for Box<T> {
    async unsafe fn write(&mut self, bs: oio::Buffer) -> Result<usize> {
        self.deref_mut().write_dyn(bs).await
    }

    async fn close(&mut self) -> Result<()> {
        self.deref_mut().close_dyn().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.deref_mut().abort_dyn().await
    }
}

/// BlockingWriter is a type erased [`BlockingWrite`]
pub type BlockingWriter = Box<dyn BlockingWrite>;

/// BlockingWrite is the trait that OpenDAL returns to callers.
pub trait BlockingWrite: Send + Sync + 'static {
    /// Write whole content at once.
    ///
    /// # Behavior
    ///
    /// - `Ok(n)` means `n` bytes has been written successfully.
    /// - `Err(err)` means error happens and no bytes has been written.
    ///
    /// It's possible that `n < bs.len()`, caller should pass the remaining bytes
    /// repeatedly until all bytes has been written.
    ///
    /// # Safety
    ///
    /// - The caller MUST ensure that the buffer is valid before `write` returns `Ready`.
    /// - The implementor SHOULD NOT store [`oio::ReadableBuf`] in anyways. The buf MUST
    ///   be passed by or copied out to an owned buffer.
    unsafe fn write(&mut self, bs: oio::Buffer) -> Result<usize>;

    /// Close the writer and make sure all data has been flushed.
    fn close(&mut self) -> Result<()>;
}

impl BlockingWrite for () {
    unsafe fn write(&mut self, bs: oio::Buffer) -> Result<usize> {
        let _ = bs;

        unimplemented!("write is required to be implemented for oio::BlockingWrite")
    }

    fn close(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support close",
        ))
    }
}

/// `Box<dyn BlockingWrite>` won't implement `BlockingWrite` automatically.
///
/// To make BlockingWriter work as expected, we must add this impl.
impl<T: BlockingWrite + ?Sized> BlockingWrite for Box<T> {
    unsafe fn write(&mut self, bs: oio::Buffer) -> Result<usize> {
        (**self).write(bs)
    }

    fn close(&mut self) -> Result<()> {
        (**self).close()
    }
}
