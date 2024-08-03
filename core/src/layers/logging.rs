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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Buf;
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

/// Add [log](https://docs.rs/log/) for every operation.
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
/// ```no_run
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
#[derive(Debug, Clone)]
pub struct LoggingLayer {
    error_level: Option<Level>,
    failure_level: Option<Level>,
    backtrace_output: bool,
    notify: Arc<DefaultLoggingInterceptor>,
}

impl Default for LoggingLayer {
    fn default() -> Self {
        Self {
            // FIXME(yingwen): Remove
            error_level: Some(Level::Warn),
            failure_level: Some(Level::Error),
            backtrace_output: false,
            notify: Arc::new(DefaultLoggingInterceptor::default()),
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

impl<A: Access> Layer<A> for LoggingLayer {
    type LayeredAccess = LoggingAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let meta = inner.info();
        LoggingAccessor {
            inner,

            ctx: LoggingContext {
                scheme: meta.scheme(),
                error_level: self.error_level,
                failure_level: self.failure_level,
                backtrace_output: self.backtrace_output,
                notify: self.notify.clone(),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct LoggingContext<I> {
    scheme: Scheme,
    error_level: Option<Level>,
    failure_level: Option<Level>,
    backtrace_output: bool,
    notify: Arc<I>,
}

impl<I> LoggingContext<I> {
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

/// LoggingInterceptor is used to intercept the log.
pub trait LoggingInterceptor: Debug {
    /// Everytime there is a log, this function will be called.
    // TODO(yingwen): docuement inputs.
    fn log(
        &self,
        scheme: Scheme,
        operation: &'static str,
        path: &str,
        message: &str,
        err: Option<&Error>,
    );
}

/// The DefaultLoggingInterceptor will log the message by the standard logging macro.
#[derive(Debug, Default)]
pub struct DefaultLoggingInterceptor {
    error_level: Option<Level>,
    failure_level: Option<Level>,
    backtrace_output: bool,
}

impl LoggingInterceptor for DefaultLoggingInterceptor {
    fn log(
        &self,
        scheme: Scheme,
        operation: &'static str,
        path: &str,
        message: &str,
        err: Option<&Error>,
    ) {
        // TODO(yingwen): Use trace for reader/writer
        // ReadOperation::Read/ReadOperation::BlockingRead/WriteOperation::Write/WriteOperation::Abort/WriteOperation::BlockingWrite
        let Some(err) = err else {
            debug!(
                target: LOGGING_TARGET,
                "service={} operation={} path={} {}",
                scheme,
                operation,
                path,
                message,
            );
            return;
        };

        if let Some(lvl) = self.error_level(err) {
            // TODO(yingwen): don't print message if no message
            log!(
                target: LOGGING_TARGET,
                lvl,
                "service={} operation={} path={} {} {}",
                scheme,
                operation,
                path,
                message,
                self.error_print(&err)
            )
        }
    }
}

impl DefaultLoggingInterceptor {
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
pub struct LoggingAccessor<A: Access> {
    inner: A,

    ctx: LoggingContext<DefaultLoggingInterceptor>,
}

static LOGGING_TARGET: &str = "opendal::services";

impl<A: Access> LayeredAccess for LoggingAccessor<A> {
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

    fn metadata(&self) -> Arc<AccessorInfo> {
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::Info.into_static(),
            "",
            "-> started",
            None,
        );
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} -> started",
        //     self.ctx.scheme,
        //     Operation::Info
        // );
        let result = self.inner.info();
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} -> finished: {:?}",
        //     self.ctx.scheme,
        //     Operation::Info,
        //     result
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::Info.into_static(),
            "",
            &format!("-> finished: {:?}", result),
            None,
        );

        result
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} -> started",
        //     self.ctx.scheme,
        //     Operation::CreateDir,
        //     path
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::CreateDir.into_static(),
            path,
            "-> started",
            None,
        );

        self.inner
            .create_dir(path, args)
            .await
            .map(|v| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> finished",
                //     self.ctx.scheme,
                //     Operation::CreateDir,
                //     path
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::CreateDir.into_static(),
                    path,
                    "-> finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} -> {}",
                //         self.ctx.scheme,
                //         Operation::CreateDir,
                //         path,
                //         self.ctx.error_print(&err)
                //     )

                // };
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::CreateDir.into_static(),
                    path,
                    "->",
                    Some(&err),
                );
                err
            })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} -> started",
        //     self.ctx.scheme,
        //     Operation::Read,
        //     path,
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::Read.into_static(),
            path,
            "-> started",
            None,
        );

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> got reader",
                //     self.ctx.scheme,
                //     Operation::Read,
                //     path,
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Read.into_static(),
                    path,
                    "-> got reader",
                    None,
                );
                (
                    rp,
                    LoggingReader::new(self.ctx.clone(), Operation::Read, path, r),
                )
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} -> {}",
                //         self.ctx.scheme,
                //         Operation::Read,
                //         path,
                //         self.ctx.error_print(&err)
                //     )
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Read.into_static(),
                    path,
                    "->",
                    Some(&err),
                );
                err
            })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} -> started",
        //     self.ctx.scheme,
        //     Operation::Write,
        //     path
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::Write.into_static(),
            path,
            "-> started",
            None,
        );

        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> start writing",
                //     self.ctx.scheme,
                //     Operation::Write,
                //     path,
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Write.into_static(),
                    path,
                    "-> start writing",
                    None,
                );
                let w = LoggingWriter::new(self.ctx.clone(), Operation::Write, path, w);
                (rp, w)
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} -> {}",
                //         self.ctx.scheme,
                //         Operation::Write,
                //         path,
                //         self.ctx.error_print(&err)
                //     )
                // };
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Write.into_static(),
                    path,
                    "->",
                    Some(&err),
                );
                err
            })
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} from={} to={} -> started",
        //     self.ctx.scheme,
        //     Operation::Copy,
        //     from,
        //     to
        // );

        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::Copy.into_static(),
            "",
            &format!("from={from} to={to} -> started"),
            None,
        );

        self.inner
            .copy(from, to, args)
            .await
            .map(|v| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} from={} to={} -> finished",
                //     self.ctx.scheme,
                //     Operation::Copy,
                //     from,
                //     to
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Copy.into_static(),
                    &format!("from={from} to={to} -> finished"),
                    None,
                );
                v
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} from={} to={} -> {}",
                //         self.ctx.scheme,
                //         Operation::Copy,
                //         from,
                //         to,
                //         self.ctx.error_print(&err),
                //     )
                // };
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Copy.into_static(),
                    "",
                    &format!("from={from} to={to} ->"),
                    Some(&err),
                );
                err
            })
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} from={} to={} -> started",
        //     self.ctx.scheme,
        //     Operation::Rename,
        //     from,
        //     to
        // );
        // FIXME(yingwen): from, to
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::Rename.into_static(),
            "",
            &format!("from={from} to={to} -> started"),
            None,
        );

        self.inner
            .rename(from, to, args)
            .await
            .map(|v| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} from={} to={} -> finished",
                //     self.ctx.scheme,
                //     Operation::Rename,
                //     from,
                //     to
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Rename.into_static(),
                    "",
                    &format!("from={from} to={to} -> finished"),
                    None,
                );
                v
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} from={} to={} -> {}",
                //         self.ctx.scheme,
                //         Operation::Rename,
                //         from,
                //         to,
                //         self.ctx.error_print(&err)
                //     )
                // };
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Rename.into_static(),
                    "",
                    &format!("from={from} to={to} ->"),
                    Some(&err),
                );
                err
            })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} -> started",
        //     self.ctx.scheme,
        //     Operation::Stat,
        //     path
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::Stat.into_static(),
            path,
            "-> started",
            None,
        );

        self.inner
            .stat(path, args)
            .await
            .map(|v| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> finished: {v:?}",
                //     self.ctx.scheme,
                //     Operation::Stat,
                //     path
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Stat.into_static(),
                    path,
                    "-> finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} -> {}",
                //         self.ctx.scheme,
                //         Operation::Stat,
                //         path,
                //         self.ctx.error_print(&err)
                //     );
                // };
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Stat.into_static(),
                    path,
                    "->",
                    Some(&err),
                );
                err
            })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} -> started",
        //     self.ctx.scheme,
        //     Operation::Delete,
        //     path
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::Delete.into_static(),
            path,
            "-> started",
            None,
        );

        self.inner
            .delete(path, args.clone())
            .inspect(|v| match v {
                Ok(_) => {
                    // debug!(
                    //     target: LOGGING_TARGET,
                    //     "service={} operation={} path={} -> finished",
                    //     self.ctx.scheme,
                    //     Operation::Delete,
                    //     path
                    // );
                    self.ctx.notify.log(
                        self.ctx.scheme,
                        Operation::Delete.into_static(),
                        path,
                        "-> finished",
                        None,
                    );
                }
                Err(err) => {
                    // if let Some(lvl) = self.ctx.error_level(err) {
                    //     log!(
                    //         target: LOGGING_TARGET,
                    //         lvl,
                    //         "service={} operation={} path={} -> {}",
                    //         self.ctx.scheme,
                    //         Operation::Delete,
                    //         path,
                    //         self.ctx.error_print(err)
                    //     );
                    // }
                    self.ctx.notify.log(
                        self.ctx.scheme,
                        Operation::Delete.into_static(),
                        path,
                        "->",
                        Some(err),
                    );
                }
            })
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} -> started",
        //     self.ctx.scheme,
        //     Operation::List,
        //     path
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::List.into_static(),
            path,
            "-> started",
            None,
        );

        self.inner
            .list(path, args)
            .map(|v| match v {
                Ok((rp, v)) => {
                    // debug!(
                    //     target: LOGGING_TARGET,
                    //     "service={} operation={} path={} -> start listing dir",
                    //     self.ctx.scheme,
                    //     Operation::List,
                    //     path
                    // );
                    self.ctx.notify.log(
                        self.ctx.scheme,
                        Operation::List.into_static(),
                        path,
                        "-> start listing dir",
                        None,
                    );
                    let streamer = LoggingLister::new(self.ctx.clone(), path, Operation::List, v);
                    Ok((rp, streamer))
                }
                Err(err) => {
                    // if let Some(lvl) = self.ctx.error_level(&err) {
                    //     log!(
                    //         target: LOGGING_TARGET,
                    //         lvl,
                    //         "service={} operation={} path={} -> {}",
                    //         self.ctx.scheme,
                    //         Operation::List,
                    //         path,
                    //         self.ctx.error_print(&err)
                    //     );
                    // }
                    self.ctx.notify.log(
                        self.ctx.scheme,
                        Operation::List.into_static(),
                        path,
                        "->",
                        Some(&err),
                    );
                    Err(err)
                }
            })
            .await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} -> started",
        //     self.ctx.scheme,
        //     Operation::Presign,
        //     path
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::Presign.into_static(),
            path,
            "-> started",
            None,
        );

        self.inner
            .presign(path, args)
            .await
            .map(|v| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> finished: {v:?}",
                //     self.ctx.scheme,
                //     Operation::Presign,
                //     path
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Presign.into_static(),
                    path,
                    "-> finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} -> {}",
                //         self.ctx.scheme,
                //         Operation::Presign,
                //         path,
                //         self.ctx.error_print(&err)
                //     );
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Presign.into_static(),
                    path,
                    "->",
                    Some(&err),
                );
                err
            })
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let (op, count) = (args.operation()[0].1.operation(), args.operation().len());

        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={}-{op} count={count} -> started",
        //     self.ctx.scheme,
        //     Operation::Batch,
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::Batch.into_static(),
            "",
            &format!("op={op} count={count} -> started"),
            None,
        );

        self.inner
            .batch(args)
            .map_ok(|v| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={}-{op} count={count} -> finished: {}, succeed: {}, failed: {}",
                //     self.ctx.scheme,
                //     Operation::Batch,
                //     v.results().len(),
                //     v.results().iter().filter(|(_, v)|v.is_ok()).count(),
                //     v.results().iter().filter(|(_, v)|v.is_err()).count(),
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Batch.into_static(),
                    "",
                    &format!(
                        "op={op} count={count} -> finished: {}, succeed: {}, failed: {}",
                        v.results().len(),
                        v.results().iter().filter(|(_, v)| v.is_ok()).count(),
                        v.results().iter().filter(|(_, v)| v.is_err()).count(),
                    ),
                    None,
                );
                v
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={}-{op} count={count} -> {}",
                //         self.ctx.scheme,
                //         Operation::Batch,
                //         self.ctx.error_print(&err)
                //     );
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::Batch.into_static(),
                    "",
                    &format!("op={op} count={count} ->"),
                    Some(&err),
                );
                err
            })
            .await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} -> started",
        //     self.ctx.scheme,
        //     Operation::BlockingCreateDir,
        //     path
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::BlockingCreateDir.into_static(),
            path,
            "-> started",
            None,
        );

        self.inner
            .blocking_create_dir(path, args)
            .map(|v| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> finished",
                //     self.ctx.scheme,
                //     Operation::BlockingCreateDir,
                //     path
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingCreateDir.into_static(),
                    path,
                    "-> finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} -> {}",
                //         self.ctx.scheme,
                //         Operation::BlockingCreateDir,
                //         path,
                //         self.ctx.error_print(&err)
                //     );
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingCreateDir.into_static(),
                    path,
                    "->",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} -> started",
        //     self.ctx.scheme,
        //     Operation::BlockingRead,
        //     path,
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::BlockingRead.into_static(),
            path,
            "-> started",
            None,
        );

        self.inner
            .blocking_read(path, args.clone())
            .map(|(rp, r)| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> got reader",
                //     self.ctx.scheme,
                //     Operation::BlockingRead,
                //     path,
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingRead.into_static(),
                    path,
                    "-> got reader",
                    None,
                );
                let r = LoggingReader::new(self.ctx.clone(), Operation::BlockingRead, path, r);
                (rp, r)
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} -> {}",
                //         self.ctx.scheme,
                //         Operation::BlockingRead,
                //         path,
                //         self.ctx.error_print(&err)
                //     );
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingRead.into_static(),
                    path,
                    "->",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} -> started",
        //     self.ctx.scheme,
        //     Operation::BlockingWrite,
        //     path,
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::BlockingWrite.into_static(),
            path,
            "-> started",
            None,
        );

        self.inner
            .blocking_write(path, args)
            .map(|(rp, w)| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> start writing",
                //     self.ctx.scheme,
                //     Operation::BlockingWrite,
                //     path,
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingWrite.into_static(),
                    path,
                    "-> start writing",
                    None,
                );
                let w = LoggingWriter::new(self.ctx.clone(), Operation::BlockingWrite, path, w);
                (rp, w)
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} -> {}",
                //         self.ctx.scheme,
                //         Operation::BlockingWrite,
                //         path,
                //         self.ctx.error_print(&err)
                //     );
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingWrite.into_static(),
                    path,
                    "->",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} from={} to={} -> started",
        //     self.ctx.scheme,
        //     Operation::BlockingCopy,
        //     from,
        //     to,
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::BlockingCopy.into_static(),
            "",
            &format!("from={from} to={to} -> started"),
            None,
        );

        self.inner
            .blocking_copy(from, to, args)
            .map(|v| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} from={} to={} -> finished",
                //     self.ctx.scheme,
                //     Operation::BlockingCopy,
                //     from,
                //     to,
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingCopy.into_static(),
                    "",
                    &format!("from={from} to={to} -> finished"),
                    None,
                );
                v
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} from={} to={} -> {}",
                //         self.ctx.scheme,
                //         Operation::BlockingCopy,
                //         from,
                //         to,
                //         self.ctx.error_print(&err)
                //     );
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingCopy.into_static(),
                    "",
                    &format!("from={from} to={to} ->"),
                    Some(&err),
                );
                err
            })
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} from={} to={} -> started",
        //     self.ctx.scheme,
        //     Operation::BlockingRename,
        //     from,
        //     to,
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::BlockingRename.into_static(),
            "",
            &format!("from={from} to={to} -> started"),
            None,
        );

        self.inner
            .blocking_rename(from, to, args)
            .map(|v| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} from={} to={} -> finished",
                //     self.ctx.scheme,
                //     Operation::BlockingRename,
                //     from,
                //     to,
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingRename.into_static(),
                    "",
                    &format!("from={from} to={to} -> finished"),
                    None,
                );
                v
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} from={} to={} -> {}",
                //         self.ctx.scheme,
                //         Operation::BlockingRename,
                //         from,
                //         to,
                //         self.ctx.error_print(&err)
                //     );
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingRename.into_static(),
                    "",
                    &format!("from={from} to={to} ->"),
                    Some(&err),
                );
                err
            })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} -> started",
        //     self.ctx.scheme,
        //     Operation::BlockingStat,
        //     path
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::BlockingStat.into_static(),
            path,
            "-> started",
            None,
        );

        self.inner
            .blocking_stat(path, args)
            .map(|v| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> finished: {v:?}",
                //     self.ctx.scheme,
                //     Operation::BlockingStat,
                //     path
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingStat.into_static(),
                    path,
                    "-> finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} -> {}",
                //         self.ctx.scheme,
                //         Operation::BlockingStat,
                //         path,
                //         self.ctx.error_print(&err)
                //     );
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingStat.into_static(),
                    path,
                    "->",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} -> started",
        //     self.ctx.scheme,
        //     Operation::BlockingDelete,
        //     path
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::BlockingDelete.into_static(),
            path,
            "-> started",
            None,
        );

        self.inner
            .blocking_delete(path, args)
            .map(|v| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> finished",
                //     self.ctx.scheme,
                //     Operation::BlockingDelete,
                //     path
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingDelete.into_static(),
                    path,
                    "-> finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} -> {}",
                //         self.ctx.scheme,
                //         Operation::BlockingDelete,
                //         path,
                //         self.ctx.error_print(&err)
                //     );
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingDelete.into_static(),
                    path,
                    "->",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} -> started",
        //     self.ctx.scheme,
        //     Operation::BlockingList,
        //     path
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            Operation::BlockingList.into_static(),
            path,
            "-> started",
            None,
        );

        self.inner
            .blocking_list(path, args)
            .map(|(rp, v)| {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> got dir",
                //     self.ctx.scheme,
                //     Operation::BlockingList,
                //     path
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingList.into_static(),
                    path,
                    "-> got dir",
                    Some(&rp),
                );
                let li = LoggingLister::new(self.ctx.clone(), path, Operation::BlockingList, v);
                (rp, li)
            })
            .map_err(|err| {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} -> {}",
                //         self.ctx.scheme,
                //         Operation::BlockingList,
                //         path,
                //         self.ctx.error_print(&err)
                //     );
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    Operation::BlockingList.into_static(),
                    path,
                    "->",
                    Some(&err),
                );
                err
            })
    }
}

/// `LoggingReader` is a wrapper of `BytesReader`, with logging functionality.
pub struct LoggingReader<R, I> {
    ctx: LoggingContext<I>,
    path: String,
    op: Operation,

    read: AtomicU64,
    inner: R,
}

impl<R, I> LoggingReader<R, I> {
    fn new(ctx: LoggingContext<I>, op: Operation, path: &str, reader: R) -> Self {
        Self {
            ctx,
            op,
            path: path.to_string(),

            read: AtomicU64::new(0),
            inner: reader,
        }
    }
}

impl<R, I: LoggingInterceptor> Drop for LoggingReader<R, I> {
    fn drop(&mut self) {
        // debug!(
        //     target: LOGGING_TARGET,
        //     "service={} operation={} path={} read={} -> data read finished",
        //     self.ctx.scheme,
        //     self.op,
        //     self.path,
        //     self.read.load(Ordering::Relaxed)
        // );
        self.ctx.notify.log(
            self.ctx.scheme,
            self.op.into_static(),
            &self.path,
            &format!(
                "read={} -> data read finished",
                self.read.load(Ordering::Relaxed)
            ),
            None,
        );
    }
}

impl<R: oio::Read> oio::Read for LoggingReader<R> {
    async fn read(&mut self) -> Result<Buffer> {
        match self.inner.read().await {
            Ok(bs) => {
                self.read
                    .fetch_add(bs.remaining() as u64, Ordering::Relaxed);
                // trace!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} read={} -> read returns {}B",
                //     self.ctx.scheme,
                //     ReadOperation::Read,
                //     self.path,
                //     self.read.load(Ordering::Relaxed),
                //     bs.remaining()
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    ReadOperation::Read.into_static(),
                    &self.path,
                    &format!(
                        "read={} -> read returns {}B",
                        self.read.load(Ordering::Relaxed),
                        bs.remaining()
                    ),
                    None,
                );
                Ok(bs)
            }
            Err(err) => {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} read={} -> read failed: {}",
                //         self.ctx.scheme,
                //         ReadOperation::Read,
                //         self.path,
                //         self.read.load(Ordering::Relaxed),
                //         self.ctx.error_print(&err),
                //     )
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    ReadOperation::Read.into_static(),
                    &self.path,
                    &format!("read={} -> read failed:", self.read.load(Ordering::Relaxed)),
                    Some(&err),
                );
                Err(err)
            }
        }
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for LoggingReader<R> {
    fn read(&mut self) -> Result<Buffer> {
        match self.inner.read() {
            Ok(bs) => {
                self.read
                    .fetch_add(bs.remaining() as u64, Ordering::Relaxed);
                // trace!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} read={} -> read returns {}B",
                //     self.ctx.scheme,
                //     ReadOperation::BlockingRead,
                //     self.path,
                //     self.read.load(Ordering::Relaxed),
                //     bs.remaining()
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    ReadOperation::BlockingRead.into_static(),
                    &self.path,
                    &format!(
                        "read={} -> read returns {}B",
                        self.read.load(Ordering::Relaxed),
                        bs.remaining()
                    ),
                    None,
                );
                Ok(bs)
            }
            Err(err) => {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} read={} -> read failed: {}",
                //         self.ctx.scheme,
                //         ReadOperation::BlockingRead,
                //         self.path,
                //         self.read.load(Ordering::Relaxed),
                //         self.ctx.error_print(&err),
                //     );
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    ReadOperation::BlockingRead.into_static(),
                    &self.path,
                    &format!("read={} -> read failed:", self.read.load(Ordering::Relaxed)),
                    Some(&err),
                );
                Err(err)
            }
        }
    }
}

pub struct LoggingWriter<W, I> {
    ctx: LoggingContext<I>,
    op: Operation,
    path: String,

    written: u64,
    inner: W,
}

impl<W, I> LoggingWriter<W, I> {
    fn new(ctx: LoggingContext<I>, op: Operation, path: &str, writer: W) -> Self {
        Self {
            ctx,
            op,
            path: path.to_string(),

            written: 0,
            inner: writer,
        }
    }
}

impl<W: oio::Write, I: LoggingInterceptor> oio::Write for LoggingWriter<W, I> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let size = bs.len();
        match self.inner.write(bs).await {
            Ok(_) => {
                // trace!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} written={}B -> data write {}B",
                //     self.ctx.scheme,
                //     WriteOperation::Write,
                //     self.path,
                //     self.written,
                //     size,
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    WriteOperation::Write.into_static(),
                    &self.path,
                    &format!("written={}B -> data write {}B", self.written, size),
                    None,
                );
                Ok(())
            }
            Err(err) => {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} written={}B -> data write failed: {}",
                //         self.ctx.scheme,
                //         WriteOperation::Write,
                //         self.path,
                //         self.written,
                //         self.ctx.error_print(&err),
                //     )
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    WriteOperation::Write.into_static(),
                    &self.path,
                    &format!("written={}B -> data write failed:", self.written),
                    Some(&err),
                );
                Err(err)
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        match self.inner.abort().await {
            Ok(_) => {
                // trace!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} written={}B -> abort writer",
                //     self.ctx.scheme,
                //     WriteOperation::Abort,
                //     self.path,
                //     self.written,
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    WriteOperation::Abort.into_static(),
                    &self.path,
                    &format!("written={}B -> abort writer", self.written),
                    None,
                );
                Ok(())
            }
            Err(err) => {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} written={}B -> abort writer failed: {}",
                //         self.ctx.scheme,
                //         WriteOperation::Abort,
                //         self.path,
                //         self.written,
                //         self.ctx.error_print(&err),
                //     )
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    WriteOperation::Abort.into_static(),
                    &self.path,
                    &format!("written={}B -> abort writer failed:", self.written),
                    Some(&err),
                );
                Err(err)
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        match self.inner.close().await {
            Ok(_) => {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} written={}B -> data written finished",
                //     self.ctx.scheme,
                //     self.op,
                //     self.path,
                //     self.written
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    self.op.into_static(),
                    &self.path,
                    &format!("written={}B -> data written finished", self.written),
                    None,
                );
                Ok(())
            }
            Err(err) => {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} written={}B -> data close failed: {}",
                //         self.ctx.scheme,
                //         WriteOperation::Close,
                //         self.path,
                //         self.written,
                //         self.ctx.error_print(&err),
                //     )
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    WriteOperation::Close.into_static(),
                    &self.path,
                    &format!("written={}B -> data close failed:", self.written),
                    Some(&err),
                );
                Err(err)
            }
        }
    }
}

impl<W: oio::BlockingWrite> oio::BlockingWrite for LoggingWriter<W> {
    fn write(&mut self, bs: Buffer) -> Result<()> {
        match self.inner.write(bs.clone()) {
            Ok(_) => {
                // trace!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} written={}B -> data write {}B",
                //     self.ctx.scheme,
                //     WriteOperation::BlockingWrite,
                //     self.path,
                //     self.written,
                //     bs.len(),
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    WriteOperation::BlockingWrite.into_static(),
                    &self.path,
                    &format!("written={}B -> data write {}B", self.written, bs.len()),
                    None,
                );
                Ok(())
            }
            Err(err) => {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} written={}B -> data write failed: {}",
                //         self.ctx.scheme,
                //         WriteOperation::BlockingWrite,
                //         self.path,
                //         self.written,
                //         self.ctx.error_print(&err),
                //     )
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    WriteOperation::BlockingWrite.into_static(),
                    &self.path,
                    &format!("written={}B -> data write failed:", self.written),
                    Some(&err),
                );
                Err(err)
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        match self.inner.close() {
            Ok(_) => {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} written={}B -> data written finished",
                //     self.ctx.scheme,
                //     self.op,
                //     self.path,
                //     self.written
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    self.op.into_static(),
                    &self.path,
                    &format!("written={}B -> data written finished", self.written),
                    None,
                );
                Ok(())
            }
            Err(err) => {
                // if let Some(lvl) = self.ctx.error_level(&err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} written={}B -> data close failed: {}",
                //         self.ctx.scheme,
                //         WriteOperation::BlockingClose,
                //         self.path,
                //         self.written,
                //         self.ctx.error_print(&err),
                //     )
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    WriteOperation::BlockingClose.into_static(),
                    &self.path,
                    &format!("written={}B -> data close failed:", self.written),
                    Some(&err),
                );
                Err(err)
            }
        }
    }
}

pub struct LoggingLister<P, I> {
    ctx: LoggingContext<I>,
    path: String,
    op: Operation,

    finished: bool,
    inner: P,
}

impl<P, I> LoggingLister<P, I> {
    fn new(ctx: LoggingContext<I>, path: &str, op: Operation, inner: P) -> Self {
        Self {
            ctx,
            path: path.to_string(),
            op,
            finished: false,
            inner,
        }
    }
}

impl<P, I> Drop for LoggingLister<P, I> {
    fn drop(&mut self) {
        if self.finished {
            // debug!(
            //     target: LOGGING_TARGET,
            //     "service={} operation={} path={} -> all entries read finished",
            //     self.ctx.scheme,
            //     self.op,
            //     self.path
            // );
            self.ctx.notify.log(
                self.ctx.scheme,
                self.op.into_static(),
                &self.path,
                "-> all entries read finished",
                None,
            );
        } else {
            // debug!(
            //     target: LOGGING_TARGET,
            //     "service={} operation={} path={} -> partial entries read finished",
            //     self.ctx.scheme,
            //     self.op,
            //     self.path
            // );
            self.ctx.notify.log(
                self.ctx.scheme,
                self.op.into_static(),
                &self.path,
                "-> partial entries read finished",
                None,
            );
        }
    }
}

impl<P: oio::List> oio::List for LoggingLister<P> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let res = self.inner.next().await;

        match &res {
            Ok(Some(de)) => {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> listed entry: {}",
                //     self.ctx.scheme,
                //     self.op,
                //     self.path,
                //     de.path(),
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    self.op.into_static(),
                    &self.path,
                    &format!("-> listed entry: {}", de.path()),
                    None,
                );
            }
            Ok(None) => {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> finished",
                //     self.ctx.scheme,
                //     self.op,
                //     self.path
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    self.op.into_static(),
                    &self.path,
                    "-> finished",
                    None,
                );
                self.finished = true;
            }
            Err(err) => {
                // if let Some(lvl) = self.ctx.error_level(err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} -> {}",
                //         self.ctx.scheme,
                //         self.op,
                //         self.path,
                //         self.ctx.error_print(err)
                //     )
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    self.op.into_static(),
                    &self.path,
                    "->",
                    Some(&err),
                );
            }
        };

        res
    }
}

impl<P: oio::BlockingList> oio::BlockingList for LoggingLister<P> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        let res = self.inner.next();

        match &res {
            Ok(Some(des)) => {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> listed entry: {}",
                //     self.ctx.scheme,
                //     self.op,
                //     self.path,
                //     des.path(),
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    self.op.into_static(),
                    &self.path,
                    &format!("-> listed entry: {}", des.path()),
                    None,
                );
            }
            Ok(None) => {
                // debug!(
                //     target: LOGGING_TARGET,
                //     "service={} operation={} path={} -> finished",
                //     self.ctx.scheme,
                //     self.op,
                //     self.path
                // );
                self.ctx.notify.log(
                    self.ctx.scheme,
                    self.op.into_static(),
                    &self.path,
                    "-> finished",
                    None,
                );
                self.finished = true;
            }
            Err(err) => {
                // if let Some(lvl) = self.ctx.error_level(err) {
                //     log!(
                //         target: LOGGING_TARGET,
                //         lvl,
                //         "service={} operation={} path={} -> {}",
                //         self.ctx.scheme,
                //         self.op,
                //         self.path,
                //         self.ctx.error_print(err)
                //     )
                // }
                self.ctx.notify.log(
                    self.ctx.scheme,
                    self.op.into_static(),
                    &self.path,
                    "->",
                    Some(&err),
                );
            }
        };

        res
    }
}
