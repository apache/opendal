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

use std::fmt::Debug;
use std::io;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use futures::TryFutureExt;
use log::debug;
use log::log;
use log::trace;
use log::Level;

use crate::ops::*;
use crate::raw::oio::ReadOperation;
use crate::raw::oio::WriteOperation;
use crate::raw::*;
use crate::*;

/// Add [log](https://docs.rs/log/) for every operations.
///
/// # Logging
///
/// - OpenDAL will log in structural way.
/// - Every operation will start with a `started` log entry.
/// - Every operation will finish with the following status:
///   - `finished`: the operation is successful.
///   - `errored`: the operation returns an expected error like `NotFound`.
///   - `failed`: the operation returns an unexpected error.
///
/// # Todo
///
/// We should migrate to log's kv api after it's ready.
///
/// Tracking issue: <https://github.com/rust-lang/log/issues/328>
///
/// # Examples
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::LoggingLayer;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(LoggingLayer::default())
///     .finish();
/// ```
///
/// # Output
///
/// OpenDAL is using [`log`](https://docs.rs/log/latest/log/) for logging internally.
///
/// To enable logging output, please set `RUST_LOG`:
///
/// ```shell
/// RUST_LOG=debug ./app
/// ```
///
/// To config logging output, please refer to [Configure Logging](https://rust-lang-nursery.github.io/rust-cookbook/development_tools/debugging/config_log.html):
///
/// ```shell
/// RUST_LOG="info,opendal::services=debug" ./app
/// ```
#[derive(Debug, Copy, Clone)]
pub struct LoggingLayer {
    error_level: Option<Level>,
    failure_level: Option<Level>,
}

impl Default for LoggingLayer {
    fn default() -> Self {
        Self {
            error_level: Some(Level::Warn),
            failure_level: Some(Level::Error),
        }
    }
}

impl LoggingLayer {
    /// Setting the log level while expected error happened.
    ///
    /// For example: accessor returns NotFound.
    ///
    /// `None` means disable the log for error.
    pub fn with_error_level(mut self, level: Option<Level>) -> Self {
        self.error_level = level;
        self
    }

    /// Setting the log level while unexpected failure happened.
    ///
    /// For example: accessor returns Unexpected network error.
    ///
    /// `None` means disable the log for failure.
    pub fn with_failure_level(mut self, level: Option<Level>) -> Self {
        self.failure_level = level;
        self
    }
}

impl<A: Accessor> Layer<A> for LoggingLayer {
    type LayeredAccessor = LoggingAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        let meta = inner.info();
        LoggingAccessor {
            scheme: meta.scheme(),
            inner,

            error_level: self.error_level,
            failure_level: self.failure_level,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LoggingAccessor<A: Accessor> {
    scheme: Scheme,
    inner: A,

    error_level: Option<Level>,
    failure_level: Option<Level>,
}

static LOGGING_TARGET: &str = "opendal::services";

impl<A: Accessor> LoggingAccessor<A> {
    #[inline]
    fn err_status(&self, err: &Error) -> &'static str {
        if err.kind() == ErrorKind::Unexpected {
            "failed"
        } else {
            "errored"
        }
    }

    #[inline]
    fn err_level(&self, err: &Error) -> Option<Level> {
        if err.kind() == ErrorKind::Unexpected {
            self.failure_level
        } else {
            self.error_level
        }
    }
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for LoggingAccessor<A> {
    type Inner = A;
    type Reader = LoggingReader<A::Reader>;
    type BlockingReader = LoggingReader<A::BlockingReader>;
    type Writer = LoggingWriter<A::Writer>;
    type BlockingWriter = LoggingWriter<A::BlockingWriter>;
    type Pager = LoggingPager<A::Pager>;
    type BlockingPager = LoggingPager<A::BlockingPager>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn metadata(&self) -> AccessorInfo {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} -> started",
            self.scheme,
            Operation::Info
        );
        let result = self.inner.info();
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} -> finished: {:?}",
            self.scheme,
            Operation::Info,
            result
        );

        result
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::Create,
            path
        );

        self.inner
            .create(path, args)
            .await
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished",
                    self.scheme,
                    Operation::Create,
                    path
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.err_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}: {err:?}",
                        self.scheme,
                        Operation::Create,
                        path,
                        self.err_status(&err)
                    )
                };
                err
            })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} range={} -> started",
            self.scheme,
            Operation::Read,
            path,
            args.range()
        );

        let range = args.range();

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} range={} -> got reader",
                    self.scheme,
                    Operation::Read,
                    path,
                    range
                );
                (
                    rp,
                    LoggingReader::new(self.scheme, Operation::Read, path, r, self.failure_level),
                )
            })
            .map_err(|err| {
                if let Some(lvl) = self.err_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} range={} -> {}: {err:?}",
                        self.scheme,
                        Operation::Read,
                        path,
                        range,
                        self.err_status(&err)
                    )
                }
                err
            })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::Write,
            path
        );

        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> start writing",
                    self.scheme,
                    Operation::Write,
                    path,
                );
                let w =
                    LoggingWriter::new(self.scheme, Operation::Write, path, w, self.failure_level);
                (rp, w)
            })
            .map_err(|err| {
                if let Some(lvl) = self.err_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}: {err:?}",
                        self.scheme,
                        Operation::Write,
                        path,
                        self.err_status(&err)
                    )
                };
                err
            })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::Stat,
            path
        );

        self.inner
            .stat(path, args)
            .await
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished: {v:?}",
                    self.scheme,
                    Operation::Stat,
                    path
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.err_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}: {err:?}",
                        self.scheme,
                        Operation::Stat,
                        path,
                        self.err_status(&err)
                    );
                };
                err
            })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::Delete,
            path
        );

        self.inner
            .delete(path, args.clone())
            .inspect(|v| match v {
                Ok(_) => {
                    debug!(
                        target: LOGGING_TARGET,
                        "service={} operation={} path={} -> finished",
                        self.scheme,
                        Operation::Delete,
                        path
                    );
                }
                Err(err) => {
                    if let Some(lvl) = self.err_level(err) {
                        log!(
                            target: LOGGING_TARGET,
                            lvl,
                            "service={} operation={} path={} -> {}: {err:?}",
                            self.scheme,
                            Operation::Delete,
                            path,
                            self.err_status(err)
                        );
                    }
                }
            })
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::List,
            path
        );

        self.inner
            .list(path, args)
            .map(|v| match v {
                Ok((rp, v)) => {
                    debug!(
                        target: LOGGING_TARGET,
                        "service={} operation={} path={} -> start listing dir",
                        self.scheme,
                        Operation::List,
                        path
                    );
                    let streamer = LoggingPager::new(
                        self.scheme,
                        path,
                        Operation::List,
                        v,
                        self.error_level,
                        self.failure_level,
                    );
                    Ok((rp, streamer))
                }
                Err(err) => {
                    if let Some(lvl) = self.err_level(&err) {
                        log!(
                            target: LOGGING_TARGET,
                            lvl,
                            "service={} operation={} path={} -> {}: {err:?}",
                            self.scheme,
                            Operation::List,
                            path,
                            self.err_status(&err)
                        );
                    }
                    Err(err)
                }
            })
            .await
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::Scan,
            path
        );

        self.inner
            .scan(path, args)
            .map(|v| match v {
                Ok((rp, v)) => {
                    debug!(
                        target: LOGGING_TARGET,
                        "service={} operation={} path={} -> start scanning",
                        self.scheme,
                        Operation::Scan,
                        path
                    );
                    let streamer = LoggingPager::new(
                        self.scheme,
                        path,
                        Operation::Scan,
                        v,
                        self.error_level,
                        self.failure_level,
                    );
                    Ok((rp, streamer))
                }
                Err(err) => {
                    if let Some(lvl) = self.err_level(&err) {
                        log!(
                            target: LOGGING_TARGET,
                            lvl,
                            "service={} operation={} path={} -> {}: {err:?}",
                            self.scheme,
                            Operation::Scan,
                            path,
                            self.err_status(&err)
                        );
                    }
                    Err(err)
                }
            })
            .await
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::Presign,
            path
        );

        self.inner
            .presign(path, args)
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished: {v:?}",
                    self.scheme,
                    Operation::Presign,
                    path
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.err_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}: {err:?}",
                        self.scheme,
                        Operation::Presign,
                        path,
                        self.err_status(&err)
                    );
                }
                err
            })
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let (op, count) = (args.operation().operation(), args.operation().len());

        debug!(
            target: LOGGING_TARGET,
            "service={} operation={}-{op} count={count} -> started",
            self.scheme,
            Operation::Batch,
        );

        self.inner
            .batch(args)
            .map_ok(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={}-{op} count={count} -> finished: {}, succeed: {}, failed: {}",
                    self.scheme,
                    Operation::Batch,
                    v.results().len(),
                    v.results().len_ok(),
                    v.results().len_err(),
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.err_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={}-{op} count={count} -> {}: {err:?}",
                        self.scheme,
                        Operation::Batch,
                        self.err_status(&err)
                    );
                }
                err
            })
            .await
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingCreate,
            path
        );

        self.inner
            .blocking_create(path, args)
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished",
                    self.scheme,
                    Operation::BlockingCreate,
                    path
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.err_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}: {err:?}",
                        self.scheme,
                        Operation::BlockingCreate,
                        path,
                        self.err_status(&err)
                    );
                }
                err
            })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} range={} -> started",
            self.scheme,
            Operation::BlockingRead,
            path,
            args.range(),
        );

        self.inner
            .blocking_read(path, args.clone())
            .map(|(rp, r)| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} range={} -> got reader",
                    self.scheme,
                    Operation::BlockingRead,
                    path,
                    args.range(),
                );
                let r = LoggingReader::new(
                    self.scheme,
                    Operation::BlockingRead,
                    path,
                    r,
                    self.failure_level,
                );
                (rp, r)
            })
            .map_err(|err| {
                if let Some(lvl) = self.err_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} range={} -> {}: {err:?}",
                        self.scheme,
                        Operation::BlockingRead,
                        path,
                        args.range(),
                        self.err_status(&err)
                    );
                }
                err
            })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingWrite,
            path,
        );

        self.inner
            .blocking_write(path, args)
            .map(|(rp, w)| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> written",
                    self.scheme,
                    Operation::BlockingWrite,
                    path,
                );
                let w = LoggingWriter::new(
                    self.scheme,
                    Operation::BlockingWrite,
                    path,
                    w,
                    self.failure_level,
                );
                (rp, w)
            })
            .map_err(|err| {
                if let Some(lvl) = self.err_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}: {err:?}",
                        self.scheme,
                        Operation::BlockingWrite,
                        path,
                        self.err_status(&err)
                    );
                }
                err
            })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingStat,
            path
        );

        self.inner
            .blocking_stat(path, args)
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished: {v:?}",
                    self.scheme,
                    Operation::BlockingStat,
                    path
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.err_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}: {err:?}",
                        self.scheme,
                        Operation::BlockingStat,
                        path,
                        self.err_status(&err)
                    );
                }
                err
            })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingDelete,
            path
        );

        self.inner
            .blocking_delete(path, args)
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished",
                    self.scheme,
                    Operation::BlockingDelete,
                    path
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.err_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}: {err:?}",
                        self.scheme,
                        Operation::BlockingDelete,
                        path,
                        self.err_status(&err)
                    );
                }
                err
            })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingList,
            path
        );

        self.inner
            .blocking_list(path, args)
            .map(|(rp, v)| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> got dir",
                    self.scheme,
                    Operation::BlockingList,
                    path
                );
                let li = LoggingPager::new(
                    self.scheme,
                    path,
                    Operation::BlockingList,
                    v,
                    self.error_level,
                    self.failure_level,
                );
                (rp, li)
            })
            .map_err(|err| {
                if let Some(lvl) = self.err_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}: {err:?}",
                        self.scheme,
                        Operation::BlockingList,
                        path,
                        self.err_status(&err)
                    );
                }
                err
            })
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.scheme,
            Operation::BlockingScan,
            path
        );

        self.inner
            .blocking_scan(path, args)
            .map(|(rp, v)| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> start scanning",
                    self.scheme,
                    Operation::BlockingScan,
                    path
                );
                let li = LoggingPager::new(
                    self.scheme,
                    path,
                    Operation::BlockingScan,
                    v,
                    self.error_level,
                    self.failure_level,
                );
                (rp, li)
            })
            .map_err(|err| {
                if let Some(lvl) = self.err_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}: {err:?}",
                        self.scheme,
                        Operation::BlockingScan,
                        path,
                        self.err_status(&err)
                    );
                }
                err
            })
    }
}

/// `LoggingReader` is a wrapper of `BytesReader`, with logging functionality.
pub struct LoggingReader<R> {
    scheme: Scheme,
    path: String,
    op: Operation,

    read: u64,
    failure_level: Option<Level>,

    inner: R,
}

impl<R> LoggingReader<R> {
    fn new(
        scheme: Scheme,
        op: Operation,
        path: &str,
        reader: R,
        failure_level: Option<Level>,
    ) -> Self {
        Self {
            scheme,
            op,
            path: path.to_string(),

            read: 0,

            inner: reader,
            failure_level,
        }
    }
}

impl<R> Drop for LoggingReader<R> {
    fn drop(&mut self) {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} read={} -> data read finished",
            self.scheme,
            self.op,
            self.path,
            self.read
        );
    }
}

impl<R: oio::Read> oio::Read for LoggingReader<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        match self.inner.poll_read(cx, buf) {
            Poll::Ready(res) => match res {
                Ok(n) => {
                    self.read += n as u64;
                    trace!(
                        target: LOGGING_TARGET,
                        "service={} operation={} path={} read={} -> data read {}B ",
                        self.scheme,
                        ReadOperation::Read,
                        self.path,
                        self.read,
                        n
                    );
                    Poll::Ready(Ok(n))
                }
                Err(err) => {
                    if let Some(lvl) = self.failure_level {
                        log!(
                            target: LOGGING_TARGET,
                            lvl,
                            "service={} operation={} path={} read={} -> data read failed: {err:?}",
                            self.scheme,
                            ReadOperation::Read,
                            self.path,
                            self.read,
                        )
                    }
                    Poll::Ready(Err(err))
                }
            },
            Poll::Pending => {
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} read={} -> data read pending",
                    self.scheme,
                    ReadOperation::Read,
                    self.path,
                    self.read
                );
                Poll::Pending
            }
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
        match self.inner.poll_seek(cx, pos) {
            Poll::Ready(res) => match res {
                Ok(n) => {
                    trace!(
                        target: LOGGING_TARGET,
                        "service={} operation={} path={} read={} -> data seek to offset {n}",
                        self.scheme,
                        ReadOperation::Seek,
                        self.path,
                        self.read,
                    );
                    Poll::Ready(Ok(n))
                }
                Err(err) => {
                    if let Some(lvl) = self.failure_level {
                        log!(
                            target: LOGGING_TARGET,
                            lvl,
                            "service={} operation={} path={} read={} -> data read failed: {err:?}",
                            self.scheme,
                            ReadOperation::Seek,
                            self.path,
                            self.read,
                        )
                    }
                    Poll::Ready(Err(err))
                }
            },
            Poll::Pending => {
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} read={} -> data seek pending",
                    self.scheme,
                    ReadOperation::Seek,
                    self.path,
                    self.read
                );
                Poll::Pending
            }
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match self.inner.poll_next(cx) {
            Poll::Ready(res) => match res {
                Some(Ok(bs)) => {
                    self.read += bs.len() as u64;
                    trace!(
                        target: LOGGING_TARGET,
                        "service={} operation={} path={} read={} -> data read {}B",
                        self.scheme,
                        ReadOperation::Next,
                        self.path,
                        self.read,
                        bs.len()
                    );
                    Poll::Ready(Some(Ok(bs)))
                }
                Some(Err(err)) => {
                    if let Some(lvl) = self.failure_level {
                        log!(
                            target: LOGGING_TARGET,
                            lvl,
                            "service={} operation={} path={} read={} -> data read failed: {err:?}",
                            self.scheme,
                            ReadOperation::Next,
                            self.path,
                            self.read,
                        )
                    }
                    Poll::Ready(Some(Err(err)))
                }
                None => Poll::Ready(None),
            },
            Poll::Pending => {
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} read={} -> data read pending",
                    self.scheme,
                    ReadOperation::Next,
                    self.path,
                    self.read
                );
                Poll::Pending
            }
        }
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for LoggingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self.inner.read(buf) {
            Ok(n) => {
                self.read += n as u64;
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} read={} -> data read {}B",
                    self.scheme,
                    ReadOperation::BlockingRead,
                    self.path,
                    self.read,
                    n
                );
                Ok(n)
            }
            Err(err) => {
                if let Some(lvl) = self.failure_level {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} read={} -> data read failed: {err:?}",
                        self.scheme,
                        ReadOperation::BlockingRead,
                        self.path,
                        self.read,
                    );
                }
                Err(err)
            }
        }
    }

    #[inline]
    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        match self.inner.seek(pos) {
            Ok(n) => {
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} read={} -> data seek to offset {n}",
                    self.scheme,
                    ReadOperation::BlockingSeek,
                    self.path,
                    self.read,
                );
                Ok(n)
            }
            Err(err) => {
                if let Some(lvl) = self.failure_level {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} read={} -> data read failed: {err:?}",
                        self.scheme,
                        ReadOperation::BlockingSeek,
                        self.path,
                        self.read,
                    );
                }
                Err(err)
            }
        }
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        match self.inner.next() {
            Some(Ok(bs)) => {
                self.read += bs.len() as u64;
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} read={} -> data read {}B",
                    self.scheme,
                    ReadOperation::BlockingNext,
                    self.path,
                    self.read,
                    bs.len()
                );
                Some(Ok(bs))
            }
            Some(Err(err)) => {
                if let Some(lvl) = self.failure_level {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} read={} -> data read failed: {err:?}",
                        self.scheme,
                        ReadOperation::BlockingNext,
                        self.path,
                        self.read,
                    )
                }
                Some(Err(err))
            }
            None => None,
        }
    }
}

pub struct LoggingWriter<W> {
    scheme: Scheme,
    op: Operation,
    path: String,

    written: u64,
    failure_level: Option<Level>,

    inner: W,
}

impl<W> LoggingWriter<W> {
    fn new(
        scheme: Scheme,
        op: Operation,
        path: &str,
        writer: W,
        failure_level: Option<Level>,
    ) -> Self {
        Self {
            scheme,
            op,
            path: path.to_string(),

            written: 0,
            inner: writer,
            failure_level,
        }
    }
}

impl<W> Drop for LoggingWriter<W> {
    fn drop(&mut self) {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} written={} -> data written finished",
            self.scheme,
            self.op,
            self.path,
            self.written
        );
    }
}

#[async_trait]
impl<W: oio::Write> oio::Write for LoggingWriter<W> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        match self.inner.write(bs).await {
            Ok(_) => {
                self.written += size as u64;
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} written={} -> data write {}B",
                    self.scheme,
                    WriteOperation::Write,
                    self.path,
                    self.written,
                    size
                );
                Ok(())
            }
            Err(err) => {
                if let Some(lvl) = self.failure_level {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} written={} -> data write failed: {err:?}",
                        self.scheme,
                        WriteOperation::Write,
                        self.path,
                        self.written,
                    )
                }
                Err(err)
            }
        }
    }

    async fn append(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        match self.inner.append(bs).await {
            Ok(_) => {
                self.written += size as u64;
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} written={} -> data write {}B",
                    self.scheme,
                    WriteOperation::Append,
                    self.path,
                    self.written,
                    size
                );
                Ok(())
            }
            Err(err) => {
                if let Some(lvl) = self.failure_level {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} written={} -> data write failed: {err:?}",
                        self.scheme,
                        WriteOperation::Append,
                        self.path,
                        self.written,
                    )
                }
                Err(err)
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        match self.inner.close().await {
            Ok(_) => Ok(()),
            Err(err) => {
                if let Some(lvl) = self.failure_level {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} written={} -> data close failed: {err:?}",
                        self.scheme,
                        WriteOperation::Close,
                        self.path,
                        self.written,
                    )
                }
                Err(err)
            }
        }
    }
}

impl<W: oio::BlockingWrite> oio::BlockingWrite for LoggingWriter<W> {
    fn write(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        match self.inner.write(bs) {
            Ok(_) => {
                self.written += size as u64;
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} written={} -> data write {}B",
                    self.scheme,
                    WriteOperation::BlockingWrite,
                    self.path,
                    self.written,
                    size
                );
                Ok(())
            }
            Err(err) => {
                if let Some(lvl) = self.failure_level {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} written={} -> data write failed: {err:?}",
                        self.scheme,
                        WriteOperation::BlockingWrite,
                        self.path,
                        self.written,
                    )
                }
                Err(err)
            }
        }
    }

    fn append(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        match self.inner.append(bs) {
            Ok(_) => {
                self.written += size as u64;
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} written={} -> data write {}B",
                    self.scheme,
                    WriteOperation::BlockingAppend,
                    self.path,
                    self.written,
                    size
                );
                Ok(())
            }
            Err(err) => {
                if let Some(lvl) = self.failure_level {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} written={} -> data write failed: {err:?}",
                        self.scheme,
                        WriteOperation::BlockingAppend,
                        self.path,
                        self.written,
                    )
                }
                Err(err)
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        match self.inner.close() {
            Ok(_) => Ok(()),
            Err(err) => {
                if let Some(lvl) = self.failure_level {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} written={} -> data close failed: {err:?}",
                        self.scheme,
                        WriteOperation::BlockingClose,
                        self.path,
                        self.written,
                    )
                }
                Err(err)
            }
        }
    }
}

pub struct LoggingPager<P> {
    scheme: Scheme,
    path: String,
    op: Operation,

    finished: bool,
    inner: P,
    error_level: Option<Level>,
    failure_level: Option<Level>,
}

impl<P> LoggingPager<P> {
    fn new(
        scheme: Scheme,
        path: &str,
        op: Operation,
        inner: P,
        error_level: Option<Level>,
        failure_level: Option<Level>,
    ) -> Self {
        Self {
            scheme,
            path: path.to_string(),
            op,
            finished: false,
            inner,
            error_level,
            failure_level,
        }
    }
}

impl<P> Drop for LoggingPager<P> {
    fn drop(&mut self) {
        if self.finished {
            debug!(
                target: LOGGING_TARGET,
                "service={} operation={} path={} -> all entries read finished",
                self.scheme,
                self.op,
                self.path
            );
        } else {
            debug!(
                target: LOGGING_TARGET,
                "service={} operation={} path={} -> partial entries read finished",
                self.scheme,
                self.op,
                self.path
            );
        }
    }
}

impl<P> LoggingPager<P> {
    #[inline]
    fn err_status(&self, err: &Error) -> &'static str {
        if err.kind() == ErrorKind::Unexpected {
            "failed"
        } else {
            "errored"
        }
    }

    #[inline]
    fn err_level(&self, err: &Error) -> Option<Level> {
        if err.kind() == ErrorKind::Unexpected {
            self.failure_level
        } else {
            self.error_level
        }
    }
}

#[async_trait]
impl<P: oio::Page> oio::Page for LoggingPager<P> {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        let res = self.inner.next().await;

        match &res {
            Ok(Some(des)) => {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> listed {} entries",
                    self.scheme,
                    self.op,
                    self.path,
                    des.len(),
                );
            }
            Ok(None) => {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished", self.scheme, self.op, self.path
                );
                self.finished = true;
            }
            Err(err) => {
                if let Some(lvl) = self.err_level(err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}: {err:?}",
                        self.scheme,
                        self.op,
                        self.path,
                        self.err_status(err)
                    )
                }
            }
        };

        res
    }
}

impl<P: oio::BlockingPage> oio::BlockingPage for LoggingPager<P> {
    fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        let res = self.inner.next();

        match &res {
            Ok(Some(des)) => {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> got {} entries",
                    self.scheme,
                    self.op,
                    self.path,
                    des.len(),
                );
            }
            Ok(None) => {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished", self.scheme, self.op, self.path
                );
                self.finished = true;
            }
            Err(err) => {
                if let Some(lvl) = self.err_level(err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}: {err:?}",
                        self.scheme,
                        self.op,
                        self.path,
                        self.err_status(err)
                    )
                }
            }
        };

        res
    }
}
