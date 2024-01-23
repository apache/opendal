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

use std::future::Future;
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::Poll;
use std::task::{ready, Context};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::time::Sleep;

use crate::raw::oio::ListOperation;
use crate::raw::oio::ReadOperation;
use crate::raw::oio::WriteOperation;
use crate::raw::*;
use crate::*;

/// Add timeout for every operations to avoid slow or unexpected hang operations.
///
/// For example, a dead connection could hang a databases sql query. TimeoutLayer
/// will break this connection and returns an error so users can handle it by
/// retrying or print to users.
///
/// # Notes
///
/// `TimeoutLayer` treats all operations in two kinds:
///
/// - Non IO Operation like `stat`, `delete` they operate on a single file. We control
///   them by setting `timeout`.
/// - IO Operation like `read`, `Reader::read` and `Writer::write`, they operate on data directly, we
///   control them by setting `io_timeout`.
///
/// # Default
///
/// - timeout: 60 seconds
/// - io_timeout: 10 seconds
///
/// # Examples
///
/// The following examples will create a timeout layer with 10 seconds timeout for all non-io
/// operations, 3 seconds timeout for all io operations.
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::TimeoutLayer;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Scheme;
/// use std::time::Duration;
///
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(TimeoutLayer::default()
///         .with_timeout(Duration::from_secs(10))
///         .with_io_timeout(Duration::from_secs(3)))
///     .finish();
/// ```
#[derive(Clone)]
pub struct TimeoutLayer {
    timeout: Duration,
    io_timeout: Duration,
}

impl Default for TimeoutLayer {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            io_timeout: Duration::from_secs(10),
        }
    }
}

impl TimeoutLayer {
    /// Create a new `TimeoutLayer` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set timeout for TimeoutLayer with given value.
    ///
    /// This timeout is for all non-io operations like `stat`, `delete`.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set io timeout for TimeoutLayer with given value.
    ///
    /// This timeout is for all io operations like `read`, `Reader::read` and `Writer::write`.
    pub fn with_io_timeout(mut self, timeout: Duration) -> Self {
        self.io_timeout = timeout;
        self
    }

    /// Set speed for TimeoutLayer with given value.
    ///
    /// # Notes
    ///
    /// The speed should be the lower bound of the IO speed. Set this value too
    /// large could result in all write operations failing.
    ///
    /// # Panics
    ///
    /// This function will panic if speed is 0.
    #[deprecated(note = "with speed is not supported anymore, please use with_io_timeout instead")]
    pub fn with_speed(self, _: u64) -> Self {
        self
    }
}

impl<A: Accessor> Layer<A> for TimeoutLayer {
    type LayeredAccessor = TimeoutAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        TimeoutAccessor {
            inner,

            timeout: self.timeout,
            io_timeout: self.io_timeout,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimeoutAccessor<A: Accessor> {
    inner: A,

    timeout: Duration,
    io_timeout: Duration,
}

impl<A: Accessor> TimeoutAccessor<A> {
    async fn timeout<F: Future<Output = Result<T>>, T>(&self, op: Operation, fut: F) -> Result<T> {
        tokio::time::timeout(self.timeout, fut).await.map_err(|_| {
            Error::new(ErrorKind::Unexpected, "operation timeout reached")
                .with_operation(op)
                .with_context("timeout", self.timeout.as_secs_f64().to_string())
                .set_temporary()
        })?
    }

    async fn io_timeout<F: Future<Output = Result<T>>, T>(
        &self,
        op: Operation,
        fut: F,
    ) -> Result<T> {
        tokio::time::timeout(self.io_timeout, fut)
            .await
            .map_err(|_| {
                Error::new(ErrorKind::Unexpected, "io timeout reached")
                    .with_operation(op)
                    .with_context("timeout", self.io_timeout.as_secs_f64().to_string())
                    .set_temporary()
            })?
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<A: Accessor> LayeredAccessor for TimeoutAccessor<A> {
    type Inner = A;
    type Reader = TimeoutWrapper<A::Reader>;
    type BlockingReader = A::BlockingReader;
    type Writer = TimeoutWrapper<A::Writer>;
    type BlockingWriter = A::BlockingWriter;
    type Lister = TimeoutWrapper<A::Lister>;
    type BlockingLister = A::BlockingLister;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.timeout(Operation::CreateDir, self.inner.create_dir(path, args))
            .await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.io_timeout(Operation::Read, self.inner.read(path, args))
            .await
            .map(|(rp, r)| (rp, TimeoutWrapper::new(r, self.io_timeout)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.io_timeout(Operation::Write, self.inner.write(path, args))
            .await
            .map(|(rp, r)| (rp, TimeoutWrapper::new(r, self.io_timeout)))
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.timeout(Operation::Copy, self.inner.copy(from, to, args))
            .await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.timeout(Operation::Rename, self.inner.rename(from, to, args))
            .await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.timeout(Operation::Stat, self.inner.stat(path, args))
            .await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.timeout(Operation::Delete, self.inner.delete(path, args))
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.io_timeout(Operation::List, self.inner.list(path, args))
            .await
            .map(|(rp, r)| (rp, TimeoutWrapper::new(r, self.io_timeout)))
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.timeout(Operation::Batch, self.inner.batch(args)).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.timeout(Operation::Presign, self.inner.presign(path, args))
            .await
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.inner.blocking_list(path, args)
    }
}

pub struct TimeoutWrapper<R> {
    inner: R,

    timeout: Duration,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl<R> TimeoutWrapper<R> {
    fn new(inner: R, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            sleep: None,
        }
    }

    #[inline]
    fn poll_timeout(&mut self, cx: &mut Context<'_>, op: &'static str) -> Result<()> {
        if let Some(sleep) = self.sleep.as_mut() {
            match sleep.as_mut().poll(cx) {
                Poll::Pending => Ok(()),
                Poll::Ready(_) => {
                    self.sleep = None;
                    Err(
                        Error::new(ErrorKind::Unexpected, "io operation timeout reached")
                            .with_operation(op)
                            .with_context("io_timeout", self.timeout.as_secs_f64().to_string())
                            .set_temporary(),
                    )
                }
            }
        } else {
            self.sleep = Some(Box::pin(tokio::time::sleep(self.timeout)));
            Ok(())
        }
    }
}

impl<R: oio::Read> oio::Read for TimeoutWrapper<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        self.poll_timeout(cx, ReadOperation::Read.into_static())?;

        let v = ready!(self.inner.poll_read(cx, buf));
        self.sleep = None;
        Poll::Ready(v)
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        self.poll_timeout(cx, ReadOperation::Seek.into_static())?;

        let v = ready!(self.inner.poll_seek(cx, pos));
        self.sleep = None;
        Poll::Ready(v)
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        self.poll_timeout(cx, ReadOperation::Next.into_static())?;

        let v = ready!(self.inner.poll_next(cx));
        self.sleep = None;
        Poll::Ready(v)
    }
}

impl<R: oio::Write> oio::Write for TimeoutWrapper<R> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        self.poll_timeout(cx, WriteOperation::Write.into_static())?;

        let v = ready!(self.inner.poll_write(cx, bs));
        self.sleep = None;
        Poll::Ready(v)
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_timeout(cx, WriteOperation::Close.into_static())?;

        let v = ready!(self.inner.poll_close(cx));
        self.sleep = None;
        Poll::Ready(v)
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_timeout(cx, WriteOperation::Abort.into_static())?;

        let v = ready!(self.inner.poll_abort(cx));
        self.sleep = None;
        Poll::Ready(v)
    }
}

impl<R: oio::List> oio::List for TimeoutWrapper<R> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<oio::Entry>>> {
        self.poll_timeout(cx, ListOperation::Next.into_static())?;

        let v = ready!(self.inner.poll_next(cx));
        self.sleep = None;
        Poll::Ready(v)
    }
}
