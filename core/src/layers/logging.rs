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
use std::task::ready;
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
/// - The default log level while expected error happened is `Warn`.
/// - The default log level while unexpected failure happened is `Error`.
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
    backtrace_output: bool,
}

impl Default for LoggingLayer {
    fn default() -> Self {
        Self {
            error_level: Some(Level::Warn),
            failure_level: Some(Level::Error),
            backtrace_output: false,
        }
    }
}

impl LoggingLayer {
    /// Setting the log level while expected error happened.
    ///
    /// For example: accessor returns NotFound.
    ///
    /// `None` means disable the log for error.
    pub fn with_error_level(mut self, level: Option<&str>) -> Result<Self> {
        if let Some(level_str) = level {
            let level = level_str.parse().map_err(|_| {
                Error::new(ErrorKind::ConfigInvalid, "invalid log level")
                    .with_context("level", level_str)
            })?;
            self.error_level = Some(level);
        } else {
            self.error_level = None;
        }
        Ok(self)
    }

    /// Setting the log level while unexpected failure happened.
    ///
    /// For example: accessor returns Unexpected network error.
    ///
    /// `None` means disable the log for failure.
    pub fn with_failure_level(mut self, level: Option<&str>) -> Result<Self> {
        if let Some(level_str) = level {
            let level = level_str.parse().map_err(|_| {
                Error::new(ErrorKind::ConfigInvalid, "invalid log level")
                    .with_context("level", level_str)
            })?;
            self.failure_level = Some(level);
        } else {
            self.failure_level = None;
        }
        Ok(self)
    }

    /// Setting whether to output backtrace while unexpected failure happened.
    ///
    /// # Notes
    ///
    /// - When the error is an expected error, backtrace will not be output.
    /// - backtrace output is disable by default.
    pub fn with_backtrace_output(mut self, enable: bool) -> Self {
        self.backtrace_output = enable;
        self
    }
}

impl<A: Accessor> Layer<A> for LoggingLayer {
    type LayeredAccessor = LoggingAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        let meta = inner.info();
        LoggingAccessor {
            inner,

            ctx: LoggingContext {
                scheme: meta.scheme(),
                error_level: self.error_level,
                failure_level: self.failure_level,
                backtrace_output: self.backtrace_output,
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct LoggingContext {
    scheme: Scheme,
    error_level: Option<Level>,
    failure_level: Option<Level>,
    backtrace_output: bool,
}

impl LoggingContext {
    #[inline]
    fn error_level(&self, err: &Error) -> Option<Level> {
        if err.kind() == ErrorKind::Unexpected {
            self.failure_level
        } else {
            self.error_level
        }
    }

    /// Print error with backtrace if it's unexpected error.
    #[inline]
    fn error_print(&self, err: &Error) -> String {
        // Don't print backtrace if it's not unexpected error.
        if err.kind() != ErrorKind::Unexpected {
            return format!("{err}");
        }

        if self.backtrace_output {
            format!("{err:?}")
        } else {
            format!("{err}")
        }
    }
}

#[derive(Clone, Debug)]
pub struct LoggingAccessor<A: Accessor> {
    inner: A,

    ctx: LoggingContext,
}

static LOGGING_TARGET: &str = "opendal::services";

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<A: Accessor> LayeredAccessor for LoggingAccessor<A> {
    type Inner = A;
    type Reader = LoggingReader<A::Reader>;
    type BlockingReader = LoggingReader<A::BlockingReader>;
    type Writer = LoggingWriter<A::Writer>;
    type BlockingWriter = LoggingWriter<A::BlockingWriter>;
    type Lister = LoggingLister<A::Lister>;
    type BlockingLister = LoggingLister<A::BlockingLister>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn metadata(&self) -> AccessorInfo {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} -> started",
            self.ctx.scheme,
            Operation::Info
        );
        let result = self.inner.info();
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} -> finished: {:?}",
            self.ctx.scheme,
            Operation::Info,
            result
        );

        result
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.ctx.scheme,
            Operation::CreateDir,
            path
        );

        self.inner
            .create_dir(path, args)
            .await
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished",
                    self.ctx.scheme,
                    Operation::CreateDir,
                    path
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}",
                        self.ctx.scheme,
                        Operation::CreateDir,
                        path,
                        self.ctx.error_print(&err)
                    )
                };
                err
            })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} range={} -> started",
            self.ctx.scheme,
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
                    self.ctx.scheme,
                    Operation::Read,
                    path,
                    range
                );
                (
                    rp,
                    LoggingReader::new(self.ctx.clone(), Operation::Read, path, r),
                )
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} range={} -> {}",
                        self.ctx.scheme,
                        Operation::Read,
                        path,
                        range,
                        self.ctx.error_print(&err)
                    )
                }
                err
            })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.ctx.scheme,
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
                    self.ctx.scheme,
                    Operation::Write,
                    path,
                );
                let w = LoggingWriter::new(self.ctx.clone(), Operation::Write, path, w);
                (rp, w)
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}",
                        self.ctx.scheme,
                        Operation::Write,
                        path,
                        self.ctx.error_print(&err)
                    )
                };
                err
            })
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} from={} to={} -> started",
            self.ctx.scheme,
            Operation::Copy,
            from,
            to
        );

        self.inner
            .copy(from, to, args)
            .await
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} from={} to={} -> finished",
                    self.ctx.scheme,
                    Operation::Copy,
                    from,
                    to
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} from={} to={} -> {}",
                        self.ctx.scheme,
                        Operation::Copy,
                        from,
                        to,
                        self.ctx.error_print(&err),
                    )
                };
                err
            })
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} from={} to={} -> started",
            self.ctx.scheme,
            Operation::Rename,
            from,
            to
        );

        self.inner
            .rename(from, to, args)
            .await
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} from={} to={} -> finished",
                    self.ctx.scheme,
                    Operation::Rename,
                    from,
                    to
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} from={} to={} -> {}",
                        self.ctx.scheme,
                        Operation::Rename,
                        from,
                        to,
                        self.ctx.error_print(&err)
                    )
                };
                err
            })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.ctx.scheme,
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
                    self.ctx.scheme,
                    Operation::Stat,
                    path
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}",
                        self.ctx.scheme,
                        Operation::Stat,
                        path,
                        self.ctx.error_print(&err)
                    );
                };
                err
            })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.ctx.scheme,
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
                        self.ctx.scheme,
                        Operation::Delete,
                        path
                    );
                }
                Err(err) => {
                    if let Some(lvl) = self.ctx.error_level(err) {
                        log!(
                            target: LOGGING_TARGET,
                            lvl,
                            "service={} operation={} path={} -> {}",
                            self.ctx.scheme,
                            Operation::Delete,
                            path,
                            self.ctx.error_print(err)
                        );
                    }
                }
            })
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.ctx.scheme,
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
                        self.ctx.scheme,
                        Operation::List,
                        path
                    );
                    let streamer = LoggingLister::new(self.ctx.clone(), path, Operation::List, v);
                    Ok((rp, streamer))
                }
                Err(err) => {
                    if let Some(lvl) = self.ctx.error_level(&err) {
                        log!(
                            target: LOGGING_TARGET,
                            lvl,
                            "service={} operation={} path={} -> {}",
                            self.ctx.scheme,
                            Operation::List,
                            path,
                            self.ctx.error_print(&err)
                        );
                    }
                    Err(err)
                }
            })
            .await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.ctx.scheme,
            Operation::Presign,
            path
        );

        self.inner
            .presign(path, args)
            .await
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished: {v:?}",
                    self.ctx.scheme,
                    Operation::Presign,
                    path
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}",
                        self.ctx.scheme,
                        Operation::Presign,
                        path,
                        self.ctx.error_print(&err)
                    );
                }
                err
            })
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let (op, count) = (args.operation()[0].1.operation(), args.operation().len());

        debug!(
            target: LOGGING_TARGET,
            "service={} operation={}-{op} count={count} -> started",
            self.ctx.scheme,
            Operation::Batch,
        );

        self.inner
            .batch(args)
            .map_ok(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={}-{op} count={count} -> finished: {}, succeed: {}, failed: {}",
                    self.ctx.scheme,
                    Operation::Batch,
                    v.results().len(),
                    v.results().iter().filter(|(_, v)|v.is_ok()).count(),
                    v.results().iter().filter(|(_, v)|v.is_err()).count(),
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={}-{op} count={count} -> {}",
                        self.ctx.scheme,
                        Operation::Batch,
                        self.ctx.error_print(&err)
                    );
                }
                err
            })
            .await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.ctx.scheme,
            Operation::BlockingCreateDir,
            path
        );

        self.inner
            .blocking_create_dir(path, args)
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished",
                    self.ctx.scheme,
                    Operation::BlockingCreateDir,
                    path
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}",
                        self.ctx.scheme,
                        Operation::BlockingCreateDir,
                        path,
                        self.ctx.error_print(&err)
                    );
                }
                err
            })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} range={} -> started",
            self.ctx.scheme,
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
                    self.ctx.scheme,
                    Operation::BlockingRead,
                    path,
                    args.range(),
                );
                let r = LoggingReader::new(self.ctx.clone(), Operation::BlockingRead, path, r);
                (rp, r)
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} range={} -> {}",
                        self.ctx.scheme,
                        Operation::BlockingRead,
                        path,
                        args.range(),
                        self.ctx.error_print(&err)
                    );
                }
                err
            })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.ctx.scheme,
            Operation::BlockingWrite,
            path,
        );

        self.inner
            .blocking_write(path, args)
            .map(|(rp, w)| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> start writing",
                    self.ctx.scheme,
                    Operation::BlockingWrite,
                    path,
                );
                let w = LoggingWriter::new(self.ctx.clone(), Operation::BlockingWrite, path, w);
                (rp, w)
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}",
                        self.ctx.scheme,
                        Operation::BlockingWrite,
                        path,
                        self.ctx.error_print(&err)
                    );
                }
                err
            })
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} from={} to={} -> started",
            self.ctx.scheme,
            Operation::BlockingCopy,
            from,
            to,
        );

        self.inner
            .blocking_copy(from, to, args)
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} from={} to={} -> finished",
                    self.ctx.scheme,
                    Operation::BlockingCopy,
                    from,
                    to,
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} from={} to={} -> {}",
                        self.ctx.scheme,
                        Operation::BlockingCopy,
                        from,
                        to,
                        self.ctx.error_print(&err)
                    );
                }
                err
            })
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} from={} to={} -> started",
            self.ctx.scheme,
            Operation::BlockingRename,
            from,
            to,
        );

        self.inner
            .blocking_rename(from, to, args)
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} from={} to={} -> finished",
                    self.ctx.scheme,
                    Operation::BlockingRename,
                    from,
                    to,
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} from={} to={} -> {}",
                        self.ctx.scheme,
                        Operation::BlockingRename,
                        from,
                        to,
                        self.ctx.error_print(&err)
                    );
                }
                err
            })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.ctx.scheme,
            Operation::BlockingStat,
            path
        );

        self.inner
            .blocking_stat(path, args)
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished: {v:?}",
                    self.ctx.scheme,
                    Operation::BlockingStat,
                    path
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}",
                        self.ctx.scheme,
                        Operation::BlockingStat,
                        path,
                        self.ctx.error_print(&err)
                    );
                }
                err
            })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.ctx.scheme,
            Operation::BlockingDelete,
            path
        );

        self.inner
            .blocking_delete(path, args)
            .map(|v| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished",
                    self.ctx.scheme,
                    Operation::BlockingDelete,
                    path
                );
                v
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}",
                        self.ctx.scheme,
                        Operation::BlockingDelete,
                        path,
                        self.ctx.error_print(&err)
                    );
                }
                err
            })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} -> started",
            self.ctx.scheme,
            Operation::BlockingList,
            path
        );

        self.inner
            .blocking_list(path, args)
            .map(|(rp, v)| {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> got dir",
                    self.ctx.scheme,
                    Operation::BlockingList,
                    path
                );
                let li = LoggingLister::new(self.ctx.clone(), path, Operation::BlockingList, v);
                (rp, li)
            })
            .map_err(|err| {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}",
                        self.ctx.scheme,
                        Operation::BlockingList,
                        path,
                        self.ctx.error_print(&err)
                    );
                }
                err
            })
    }
}

/// `LoggingReader` is a wrapper of `BytesReader`, with logging functionality.
pub struct LoggingReader<R> {
    ctx: LoggingContext,
    path: String,
    op: Operation,

    read: u64,
    inner: R,
}

impl<R> LoggingReader<R> {
    fn new(ctx: LoggingContext, op: Operation, path: &str, reader: R) -> Self {
        Self {
            ctx,
            op,
            path: path.to_string(),

            read: 0,
            inner: reader,
        }
    }
}

impl<R> Drop for LoggingReader<R> {
    fn drop(&mut self) {
        debug!(
            target: LOGGING_TARGET,
            "service={} operation={} path={} read={} -> data read finished",
            self.ctx.scheme,
            self.op,
            self.path,
            self.read
        );
    }
}

impl<R: oio::Read> oio::Read for LoggingReader<R> {
    async fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        match self.inner.seek(pos).await {
            Ok(n) => {
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} read={} -> seek to {pos:?}, current offset {n}",
                    self.ctx.scheme,
                    ReadOperation::Seek,
                    self.path,
                    self.read,
                );
                Ok(n)
            }
            Err(err) => {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} read={} -> seek to {pos:?} failed: {}",
                        self.ctx.scheme,
                        ReadOperation::Seek,
                        self.path,
                        self.read,
                        self.ctx.error_print(&err),
                    )
                }
                Err(err)
            }
        }
    }

    async fn read(&mut self, size: usize) -> Result<Bytes> {
        match self.inner.read(size).await {
            Ok(bs) => {
                self.read += bs.len() as u64;
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} read={} -> next returns {}B",
                    self.ctx.scheme,
                    ReadOperation::Next,
                    self.path,
                    self.read,
                    bs.len()
                );
                Ok(bs)
            }
            Err(err) => {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} read={} -> next failed: {}",
                        self.ctx.scheme,
                        ReadOperation::Next,
                        self.path,
                        self.read,
                        self.ctx.error_print(&err),
                    )
                }
                Err(err)
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
                    self.ctx.scheme,
                    ReadOperation::BlockingRead,
                    self.path,
                    self.read,
                    n
                );
                Ok(n)
            }
            Err(err) => {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} read={} -> data read failed: {}",
                        self.ctx.scheme,
                        ReadOperation::BlockingRead,
                        self.path,
                        self.read,
                        self.ctx.error_print(&err),
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
                    self.ctx.scheme,
                    ReadOperation::BlockingSeek,
                    self.path,
                    self.read,
                );
                Ok(n)
            }
            Err(err) => {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} read={} -> data read failed: {}",
                        self.ctx.scheme,
                        ReadOperation::BlockingSeek,
                        self.path,
                        self.read,
                        self.ctx.error_print(&err),
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
                    self.ctx.scheme,
                    ReadOperation::BlockingNext,
                    self.path,
                    self.read,
                    bs.len()
                );
                Some(Ok(bs))
            }
            Some(Err(err)) => {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} read={} -> data read failed: {}",
                        self.ctx.scheme,
                        ReadOperation::BlockingNext,
                        self.path,
                        self.read,
                        self.ctx.error_print(&err),
                    )
                }
                Some(Err(err))
            }
            None => None,
        }
    }
}

pub struct LoggingWriter<W> {
    ctx: LoggingContext,
    op: Operation,
    path: String,

    written: u64,
    inner: W,
}

impl<W> LoggingWriter<W> {
    fn new(ctx: LoggingContext, op: Operation, path: &str, writer: W) -> Self {
        Self {
            ctx,
            op,
            path: path.to_string(),

            written: 0,
            inner: writer,
        }
    }
}

impl<W: oio::Write> oio::Write for LoggingWriter<W> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        match ready!(self.inner.poll_write(cx, bs)) {
            Ok(n) => {
                self.written += n as u64;
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} written={}B -> input data {}B, write {}B",
                    self.ctx.scheme,
                    WriteOperation::Write,
                    self.path,
                    self.written,
                    bs.remaining(),
                    n,
                );
                Poll::Ready(Ok(n))
            }
            Err(err) => {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} written={}B -> data write failed: {}",
                        self.ctx.scheme,
                        WriteOperation::Write,
                        self.path,
                        self.written,
                        self.ctx.error_print(&err),
                    )
                }
                Poll::Ready(Err(err))
            }
        }
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match ready!(self.inner.poll_abort(cx)) {
            Ok(_) => {
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} written={}B -> abort writer",
                    self.ctx.scheme,
                    WriteOperation::Abort,
                    self.path,
                    self.written,
                );
                Poll::Ready(Ok(()))
            }
            Err(err) => {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} written={}B -> abort writer failed: {}",
                        self.ctx.scheme,
                        WriteOperation::Abort,
                        self.path,
                        self.written,
                        self.ctx.error_print(&err),
                    )
                }
                Poll::Ready(Err(err))
            }
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match ready!(self.inner.poll_close(cx)) {
            Ok(_) => {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} written={}B -> data written finished",
                    self.ctx.scheme,
                    self.op,
                    self.path,
                    self.written
                );
                Poll::Ready(Ok(()))
            }
            Err(err) => {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} written={}B -> data close failed: {}",
                        self.ctx.scheme,
                        WriteOperation::Close,
                        self.path,
                        self.written,
                        self.ctx.error_print(&err),
                    )
                }
                Poll::Ready(Err(err))
            }
        }
    }
}

impl<W: oio::BlockingWrite> oio::BlockingWrite for LoggingWriter<W> {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        match self.inner.write(bs) {
            Ok(n) => {
                self.written += n as u64;
                trace!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} written={}B -> input data {}B, write {}B",
                    self.ctx.scheme,
                    WriteOperation::BlockingWrite,
                    self.path,
                    self.written,
                    bs.remaining(),
                    n
                );
                Ok(n)
            }
            Err(err) => {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} written={}B -> data write failed: {}",
                        self.ctx.scheme,
                        WriteOperation::BlockingWrite,
                        self.path,
                        self.written,
                        self.ctx.error_print(&err),
                    )
                }
                Err(err)
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        match self.inner.close() {
            Ok(_) => {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} written={}B -> data written finished",
                    self.ctx.scheme,
                    self.op,
                    self.path,
                    self.written
                );
                Ok(())
            }
            Err(err) => {
                if let Some(lvl) = self.ctx.error_level(&err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} written={}B -> data close failed: {}",
                        self.ctx.scheme,
                        WriteOperation::BlockingClose,
                        self.path,
                        self.written,
                        self.ctx.error_print(&err),
                    )
                }
                Err(err)
            }
        }
    }
}

pub struct LoggingLister<P> {
    ctx: LoggingContext,
    path: String,
    op: Operation,

    finished: bool,
    inner: P,
}

impl<P> LoggingLister<P> {
    fn new(ctx: LoggingContext, path: &str, op: Operation, inner: P) -> Self {
        Self {
            ctx,
            path: path.to_string(),
            op,
            finished: false,
            inner,
        }
    }
}

impl<P> Drop for LoggingLister<P> {
    fn drop(&mut self) {
        if self.finished {
            debug!(
                target: LOGGING_TARGET,
                "service={} operation={} path={} -> all entries read finished",
                self.ctx.scheme,
                self.op,
                self.path
            );
        } else {
            debug!(
                target: LOGGING_TARGET,
                "service={} operation={} path={} -> partial entries read finished",
                self.ctx.scheme,
                self.op,
                self.path
            );
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<P: oio::List> oio::List for LoggingLister<P> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<oio::Entry>>> {
        let res = ready!(self.inner.poll_next(cx));

        match &res {
            Ok(Some(de)) => {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> listed entry: {}",
                    self.ctx.scheme,
                    self.op,
                    self.path,
                    de.path(),
                );
            }
            Ok(None) => {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished",
                    self.ctx.scheme,
                    self.op,
                    self.path
                );
                self.finished = true;
            }
            Err(err) => {
                if let Some(lvl) = self.ctx.error_level(err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}",
                        self.ctx.scheme,
                        self.op,
                        self.path,
                        self.ctx.error_print(err)
                    )
                }
            }
        };

        Poll::Ready(res)
    }
}

impl<P: oio::BlockingList> oio::BlockingList for LoggingLister<P> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        let res = self.inner.next();

        match &res {
            Ok(Some(des)) => {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> listed entry: {}",
                    self.ctx.scheme,
                    self.op,
                    self.path,
                    des.path(),
                );
            }
            Ok(None) => {
                debug!(
                    target: LOGGING_TARGET,
                    "service={} operation={} path={} -> finished",
                    self.ctx.scheme,
                    self.op,
                    self.path
                );
                self.finished = true;
            }
            Err(err) => {
                if let Some(lvl) = self.ctx.error_level(err) {
                    log!(
                        target: LOGGING_TARGET,
                        lvl,
                        "service={} operation={} path={} -> {}",
                        self.ctx.scheme,
                        self.op,
                        self.path,
                        self.ctx.error_print(err)
                    )
                }
            }
        };

        res
    }
}
