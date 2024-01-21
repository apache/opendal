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
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;

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
/// It happens that a connection could be slow but not dead, so we have a `max_io_timeouts` to
/// control how many consecutive IO timeouts we can tolerate. If `max_io_timeouts` is not reached,
/// we will print a warning and keep waiting this io operation instead.
///
/// # Default
///
/// - timeout: 60 seconds
/// - io_timeout: 10 seconds
/// - max_io_timeouts: 3 times
///
/// # Examples
///
/// The following examples will create a timeout layer with 10 seconds timeout for all non-io
/// operations, 3 seconds timeout for all io operations and 2 consecutive io timeouts are allowed.
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
///     .layer(TimeoutLayer::default().with_timeout(Duration::from_secs(10)).with_io_timeout(3).with_max_io_timeouts(2))
///     .finish();
/// ```
#[derive(Clone)]
pub struct TimeoutLayer {
    timeout: Duration,
    io_timeout: Duration,
    max_io_timeouts: usize,
}

impl Default for TimeoutLayer {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            io_timeout: Duration::from_secs(10),
            max_io_timeouts: 3,
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

    /// Set max io timeouts for TimeoutLayer with given value.
    ///
    /// This value is used to control how many consecutive io timeouts we can tolerate.
    pub fn with_max_io_timeouts(mut self, v: usize) -> Self {
        self.max_io_timeouts = v;
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
    pub fn with_speed(mut self, _: u64) -> Self {
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
            max_io_timeouts: self.max_io_timeouts,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimeoutAccessor<A: Accessor> {
    inner: A,

    timeout: Duration,
    io_timeout: Duration,
    max_io_timeouts: usize,
}

impl<A: Accessor> TimeoutAccessor<A> {
    async fn io_timeout<F: Future<Output = Result<T>>, T>(
        &self,
        op: Operation,
        fut: F,
    ) -> Result<T> {
        tokio::time::timeout(self.io_timeout, fut)
            .await
            .map_err(|_| {
                Error::new(ErrorKind::Unexpected, "io operation timeout reached")
                    .with_operation(op)
                    .with_context("io_timeout", self.io_timeout.as_secs_f64().to_string())
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

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.io_timeout(Operation::Read, self.inner.read(path, args))
            .await
            .map(|(rp, r)| {
                (
                    rp,
                    TimeoutWrapper::new(r, self.io_timeout, self.max_io_timeouts),
                )
            })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.io_timeout(Operation::Write, self.inner.write(path, args))
            .await
            .map(|(rp, r)| {
                (
                    rp,
                    TimeoutWrapper::new(r, self.io_timeout, self.max_io_timeouts),
                )
            })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.io_timeout(Operation::List, self.inner.list(path, args))
            .await
            .map(|(rp, r)| {
                (
                    rp,
                    TimeoutWrapper::new(r, self.io_timeout, self.max_io_timeouts),
                )
            })
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
    max_timeouts: usize,

    current_timeouts: usize,
    futures: Option<(BoxedFuture)>,
}

impl<R> TimeoutWrapper<R> {
    fn new(inner: R, timeout: Duration, max_timeouts: usize) -> Self {
        Self {
            inner,
            timeout,
            max_timeouts,
            start: None,
        }
    }
}

impl<R: oio::Read> oio::Read for TimeoutWrapper<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        match self.start {
            Some(start) => {
                if start.elapsed() > self.timeout {
                    // Clean up the start time before return ready.
                    self.start = None;

                    return Poll::Ready(Err(Error::new(
                        ErrorKind::Unexpected,
                        "operation timeout",
                    )
                    .with_operation(ReadOperation::Read)
                    .with_context("timeout", self.timeout.as_secs_f64().to_string())
                    .set_temporary()));
                }
            }
            None => {
                self.start = Some(Instant::now());
            }
        }

        match self.inner.poll_read(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(v) => {
                self.start = None;
                Poll::Ready(v)
            }
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        match self.start {
            Some(start) => {
                if start.elapsed() > self.timeout {
                    // Clean up the start time before return ready.
                    self.start = None;

                    return Poll::Ready(Err(Error::new(
                        ErrorKind::Unexpected,
                        "operation timeout",
                    )
                    .with_operation(ReadOperation::Seek)
                    .with_context("timeout", self.timeout.as_secs_f64().to_string())
                    .set_temporary()));
                }
            }
            None => {
                self.start = Some(Instant::now());
            }
        }

        match self.inner.poll_seek(cx, pos) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(v) => {
                self.start = None;
                Poll::Ready(v)
            }
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match self.start {
            Some(start) => {
                if start.elapsed() > self.timeout {
                    // Clean up the start time before return ready.
                    self.start = None;

                    return Poll::Ready(Some(Err(Error::new(
                        ErrorKind::Unexpected,
                        "operation timeout",
                    )
                    .with_operation(ReadOperation::Next)
                    .with_context("timeout", self.timeout.as_secs_f64().to_string())
                    .set_temporary())));
                }
            }
            None => {
                self.start = Some(Instant::now());
            }
        }

        match self.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(v) => {
                self.start = None;
                Poll::Ready(v)
            }
        }
    }
}

impl<R: oio::Write> oio::Write for TimeoutWrapper<R> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        match self.start {
            Some(start) => {
                if start.elapsed() > self.timeout {
                    // Clean up the start time before return ready.
                    self.start = None;

                    return Poll::Ready(Err(Error::new(
                        ErrorKind::Unexpected,
                        "operation timeout",
                    )
                    .with_operation(WriteOperation::Write)
                    .with_context("timeout", self.timeout.as_secs_f64().to_string())
                    .set_temporary()));
                }
            }
            None => {
                self.start = Some(Instant::now());
            }
        }

        match self.inner.poll_write(cx, bs) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(v) => {
                self.start = None;
                Poll::Ready(v)
            }
        }
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self.start {
            Some(start) => {
                if start.elapsed() > self.timeout {
                    // Clean up the start time before return ready.
                    self.start = None;

                    return Poll::Ready(Err(Error::new(
                        ErrorKind::Unexpected,
                        "operation timeout",
                    )
                    .with_operation(WriteOperation::Abort)
                    .with_context("timeout", self.timeout.as_secs_f64().to_string())
                    .set_temporary()));
                }
            }
            None => {
                self.start = Some(Instant::now());
            }
        }

        match self.inner.poll_abort(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(v) => {
                self.start = None;
                Poll::Ready(v)
            }
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self.start {
            Some(start) => {
                if start.elapsed() > self.timeout {
                    // Clean up the start time before return ready.
                    self.start = None;

                    return Poll::Ready(Err(Error::new(
                        ErrorKind::Unexpected,
                        "operation timeout",
                    )
                    .with_operation(WriteOperation::Close)
                    .with_context("timeout", self.timeout.as_secs_f64().to_string())
                    .set_temporary()));
                }
            }
            None => {
                self.start = Some(Instant::now());
            }
        }

        match self.inner.poll_close(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(v) => {
                self.start = None;
                Poll::Ready(v)
            }
        }
    }
}

impl<R: oio::List> oio::List for TimeoutWrapper<R> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<oio::Entry>>> {
        match self.start {
            Some(start) => {
                if start.elapsed() > self.timeout {
                    // Clean up the start time before return ready.
                    self.start = None;

                    return Poll::Ready(Err(Error::new(
                        ErrorKind::Unexpected,
                        "operation timeout",
                    )
                    .with_operation(ListOperation::Next)
                    .with_context("timeout", self.timeout.as_secs_f64().to_string())
                    .set_temporary()));
                }
            }
            None => {
                self.start = Some(Instant::now());
            }
        }

        match self.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(v) => {
                self.start = None;
                Poll::Ready(v)
            }
        }
    }
}
