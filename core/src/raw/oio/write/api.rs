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
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use pin_project::pin_project;

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
pub type Writer = Box<dyn Write>;

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
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>>;

    /// Close the writer and make sure all data has been flushed.
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>>;

    /// Abort the pending writer.
    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>>;
}

impl Write for () {
    fn poll_write(&mut self, _: &mut Context<'_>, _: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        unimplemented!("write is required to be implemented for oio::Write")
    }

    fn poll_close(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support close",
        )))
    }

    fn poll_abort(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support abort",
        )))
    }
}

/// `Box<dyn Write>` won't implement `Write` automatically.
///
/// To make Writer work as expected, we must add this impl.
impl<T: Write + ?Sized> Write for Box<T> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        (**self).poll_write(cx, bs)
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        (**self).poll_close(cx)
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        (**self).poll_abort(cx)
    }
}

/// Impl WriteExt for all T: Write
impl<T: Write> WriteExt for T {}

/// Extension of [`Read`] to make it easier for use.
pub trait WriteExt: Write {
    /// Build a future for `poll_write`.
    fn write<'a>(&'a mut self, buf: &'a dyn oio::WriteBuf) -> WriteFuture<'a, Self> {
        WriteFuture { writer: self, buf }
    }

    /// Build a future for `poll_close`.
    fn close(&mut self) -> CloseFuture<Self> {
        CloseFuture { writer: self }
    }

    /// Build a future for `poll_abort`.
    fn abort(&mut self) -> AbortFuture<Self> {
        AbortFuture { writer: self }
    }
}

/// Make this future `!Unpin` for compatibility with async trait methods.
#[pin_project(!Unpin)]
pub struct WriteFuture<'a, W: Write + Unpin + ?Sized> {
    writer: &'a mut W,
    buf: &'a dyn oio::WriteBuf,
}

impl<W> Future for WriteFuture<'_, W>
where
    W: Write + Unpin + ?Sized,
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<usize>> {
        let this = self.project();
        Pin::new(this.writer).poll_write(cx, *this.buf)
    }
}

/// Make this future `!Unpin` for compatibility with async trait methods.
#[pin_project(!Unpin)]
pub struct AbortFuture<'a, W: Write + Unpin + ?Sized> {
    writer: &'a mut W,
}

impl<W> Future for AbortFuture<'_, W>
where
    W: Write + Unpin + ?Sized,
{
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.project();
        Pin::new(this.writer).poll_abort(cx)
    }
}

/// Make this future `!Unpin` for compatibility with async trait methods.
#[pin_project(!Unpin)]
pub struct CloseFuture<'a, W: Write + Unpin + ?Sized> {
    writer: &'a mut W,
}

impl<W> Future for CloseFuture<'_, W>
where
    W: Write + Unpin + ?Sized,
{
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.project();
        Pin::new(this.writer).poll_close(cx)
    }
}

/// BlockingWriter is a type erased [`BlockingWrite`]
pub type BlockingWriter = Box<dyn BlockingWrite>;

/// BlockingWrite is the trait that OpenDAL returns to callers.
pub trait BlockingWrite: Send + Sync + 'static {
    /// Write whole content at once.
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize>;

    /// Close the writer and make sure all data has been flushed.
    fn close(&mut self) -> Result<()>;
}

impl BlockingWrite for () {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
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
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        (**self).write(bs)
    }

    fn close(&mut self) -> Result<()> {
        (**self).close()
    }
}
