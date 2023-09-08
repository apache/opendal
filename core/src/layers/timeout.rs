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

use std::io::SeekFrom;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;

use crate::raw::oio::PageOperation;
use crate::raw::oio::ReadOperation;
use crate::raw::oio::WriteOperation;
use crate::raw::*;
use crate::*;

/// Add timeout for every operations.
///
/// # Notes
///
/// - For IO operations like `read`, `write`, we will set a timeout
///   for each single IO operation.
/// - For other operations like `stat`, and `delete`, the timeout is for the whole
///   operation.
///
/// Besides, we will also set a slow speed for each IO operation. If the IO
/// operation's speed is lower than the slow speed, we will return a timeout error
/// instead of kept waiting for it.
///
/// For examples, if we set timeout to 60 seconds and speed to 1MiB/s, then:
///
/// - If `stat` didn't return in 60 seconds, we will return a timeout error.
/// - If `Reader::read` didn't return in 60 seconds, we will return a timeout error.
/// - For `Writer::write(vec![1024*1024*1024])`
///   - didn't return in 60s, it's ok, we will keep waiting.
///   - didn't return in 1024s (1GiB/1MiB), we will return a timeout error.
///
/// # Default
///
/// - timeout: 60 seconds
/// - speed: 1024 bytes per second, aka, 1KiB/s.
///
/// # Examples
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::TimeoutLayer;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(TimeoutLayer::default())
///     .finish();
/// ```
#[derive(Clone)]
pub struct TimeoutLayer {
    timeout: Duration,
    speed: u64,
}

impl Default for TimeoutLayer {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            speed: 1024,
        }
    }
}

impl TimeoutLayer {
    /// Create a new `TimeoutLayer` with default settings.
    ///
    /// - timeout: 60 seconds
    /// - speed: 1024 bytes per second, aka, 1KiB/s.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set timeout for TimeoutLayer with given value.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
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
    pub fn with_speed(mut self, speed: u64) -> Self {
        assert_ne!(speed, 0, "TimeoutLayer speed must not be 0");

        self.speed = speed;
        self
    }
}

impl<A: Accessor> Layer<A> for TimeoutLayer {
    type LayeredAccessor = TimeoutAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        TimeoutAccessor {
            inner,

            timeout: self.timeout,
            speed: self.speed,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimeoutAccessor<A: Accessor> {
    inner: A,

    timeout: Duration,
    speed: u64,
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for TimeoutAccessor<A> {
    type Inner = A;
    type Reader = TimeoutWrapper<A::Reader>;
    type BlockingReader = A::BlockingReader;
    type Writer = TimeoutWrapper<A::Writer>;
    type BlockingWriter = A::BlockingWriter;
    type Pager = TimeoutWrapper<A::Pager>;
    type BlockingPager = A::BlockingPager;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        tokio::time::timeout(self.timeout, self.inner.read(path, args))
            .await
            .map_err(|_| {
                Error::new(ErrorKind::Unexpected, "operation timeout")
                    .with_operation(Operation::Read)
                    .with_context("timeout", self.timeout.as_secs_f64().to_string())
                    .set_temporary()
            })?
            .map(|(rp, r)| (rp, TimeoutWrapper::new(r, self.timeout, self.speed)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        tokio::time::timeout(self.timeout, self.inner.write(path, args))
            .await
            .map_err(|_| {
                Error::new(ErrorKind::Unexpected, "operation timeout")
                    .with_operation(Operation::Write)
                    .with_context("timeout", self.timeout.as_secs_f64().to_string())
                    .set_temporary()
            })?
            .map(|(rp, r)| (rp, TimeoutWrapper::new(r, self.timeout, self.speed)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        tokio::time::timeout(self.timeout, self.inner.list(path, args))
            .await
            .map_err(|_| {
                Error::new(ErrorKind::Unexpected, "operation timeout")
                    .with_operation(Operation::List)
                    .with_context("timeout", self.timeout.as_secs_f64().to_string())
                    .set_temporary()
            })?
            .map(|(rp, r)| (rp, TimeoutWrapper::new(r, self.timeout, self.speed)))
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner.blocking_list(path, args)
    }
}

pub struct TimeoutWrapper<R> {
    inner: R,

    timeout: Duration,
    #[allow(dead_code)]
    speed: u64,

    start: Option<Instant>,
}

impl<R> TimeoutWrapper<R> {
    fn new(inner: R, timeout: Duration, speed: u64) -> Self {
        Self {
            inner,
            timeout,
            speed,
            start: None,
        }
    }

    #[allow(dead_code)]
    fn io_timeout(&self, size: u64) -> Duration {
        let timeout = Duration::from_millis(size * 1000 / self.speed + 1);

        timeout.max(self.timeout)
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

#[async_trait]
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

#[async_trait]
impl<R: oio::Page> oio::Page for TimeoutWrapper<R> {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        tokio::time::timeout(self.timeout, self.inner.next())
            .await
            .map_err(|_| {
                Error::new(ErrorKind::Unexpected, "operation timeout")
                    .with_operation(PageOperation::Next)
                    .with_context("timeout", self.timeout.as_secs_f64().to_string())
                    .set_temporary()
            })?
    }
}
