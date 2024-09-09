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
use std::fmt::Display;
use std::sync::Arc;

use futures::FutureExt;
use futures::TryFutureExt;
use log::log;
use log::Level;

use crate::raw::*;
use crate::*;

/// Add [log](https://docs.rs/log/) for every operation.
///
/// # Logging
///
/// - OpenDAL will log in structural way.
/// - Every operation will start with a `started` log entry.
/// - Every operation will finish with the following status:
///   - `succeeded`: the operation is successful, but might have more to take.
///   - `finished`: the whole operation is finished.
///   - `failed`: the operation returns an unexpected error.
/// - The default log level while expected error happened is `Warn`.
/// - The default log level while unexpected failure happened is `Error`.
///
/// # Examples
///
/// ```no_run
/// # use opendal::layers::LoggingLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opendal::Result;
/// # use opendal::Scheme;
///
/// # fn main() -> Result<()> {
///  let _ = Operator::new(services::Memory::default())?
///     .layer(LoggingLayer::default())
///     .finish();
///  Ok(())
/// # }
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
///
/// # Logging Interceptor
///
/// You can implement your own logging interceptor to customize the logging behavior.
///
/// ```no_run
/// # use opendal::layers::LoggingInterceptor;
/// # use opendal::layers::LoggingLayer;
/// # use opendal::raw;
/// # use opendal::services;
/// # use opendal::Error;
/// # use opendal::Operator;
/// # use opendal::Result;
/// # use opendal::Scheme;
///
/// #[derive(Debug, Clone)]
/// struct MyLoggingInterceptor;
///
/// impl LoggingInterceptor for MyLoggingInterceptor {
///     fn log(
///         &self,
///         info: &raw::AccessorInfo,
///         operation: raw::Operation,
///         context: &[(&str, &str)],
///         message: &str,
///         err: Option<&Error>,
///     ) {
///         // log something
///     }
/// }
///
/// # fn main() -> Result<()> {
///  let _ = Operator::new(services::Memory::default())?
///     .layer(LoggingLayer::new(MyLoggingInterceptor))
///     .finish();
///  Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct LoggingLayer<I = DefaultLoggingInterceptor> {
    logger: I,
}

impl Default for LoggingLayer {
    fn default() -> Self {
        Self {
            logger: DefaultLoggingInterceptor,
        }
    }
}

impl LoggingLayer {
    /// Create the layer with specific logging interceptor.
    pub fn new<I: LoggingInterceptor>(logger: I) -> LoggingLayer<I> {
        LoggingLayer { logger }
    }
}

impl<A: Access, I: LoggingInterceptor> Layer<A> for LoggingLayer<I> {
    type LayeredAccess = LoggingAccessor<A, I>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();
        LoggingAccessor {
            inner,

            info,
            logger: self.logger.clone(),
        }
    }
}

/// LoggingInterceptor is used to intercept the log.
pub trait LoggingInterceptor: Debug + Clone + Send + Sync + Unpin + 'static {
    /// Everytime there is a log, this function will be called.
    ///
    /// # Inputs
    ///
    /// - info: The service's access info.
    /// - operation: The operation to log.
    /// - context: Additional context of the log like path, etc.
    /// - message: The log message.
    /// - err: The error to log.
    ///
    /// # Note
    ///
    /// Users should avoid calling resource-intensive operations such as I/O or network
    /// functions here, especially anything that takes longer than 10ms. Otherwise, Opendal
    /// could perform unexpectedly slow.
    fn log(
        &self,
        info: &AccessorInfo,
        operation: Operation,
        context: &[(&str, &str)],
        message: &str,
        err: Option<&Error>,
    );
}

/// The DefaultLoggingInterceptor will log the message by the standard logging macro.
#[derive(Debug, Copy, Clone, Default)]
pub struct DefaultLoggingInterceptor;

impl LoggingInterceptor for DefaultLoggingInterceptor {
    #[inline]
    fn log(
        &self,
        info: &AccessorInfo,
        operation: Operation,
        context: &[(&str, &str)],
        message: &str,
        err: Option<&Error>,
    ) {
        if let Some(err) = err {
            // Print error if it's unexpected, otherwise in warn.
            let lvl = if err.kind() == ErrorKind::Unexpected {
                Level::Error
            } else {
                Level::Warn
            };

            log!(
                target: LOGGING_TARGET,
                lvl,
                "service={} name={} {}: {operation} {message} {}",
                info.scheme(),
                info.name(),
                LoggingContext(context),
                // Print error message with debug output while unexpected happened.
                //
                // It's super sad that we can't bind `format_args!()` here.
                // See: https://github.com/rust-lang/rust/issues/92698
                if err.kind() != ErrorKind::Unexpected {
                   format!("{err}")
                } else {
                   format!("{err:?}")
                }
            );
        }

        // Print debug message if operation is oneshot, otherwise in trace.
        let lvl = if operation.is_oneshot() {
            Level::Debug
        } else {
            Level::Trace
        };

        log!(
            target: LOGGING_TARGET,
            lvl,
            "service={} name={} {}: {operation} {message}",
            info.scheme(),
            info.name(),
            LoggingContext(context),
        );
    }
}

struct LoggingContext<'a>(&'a [(&'a str, &'a str)]);

impl<'a> Display for LoggingContext<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, (k, v)) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, " {}={}", k, v)?;
            } else {
                write!(f, "{}={}", k, v)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct LoggingAccessor<A: Access, I: LoggingInterceptor> {
    inner: A,

    info: Arc<AccessorInfo>,
    logger: I,
}

static LOGGING_TARGET: &str = "opendal::services";

impl<A: Access, I: LoggingInterceptor> LayeredAccess for LoggingAccessor<A, I> {
    type Inner = A;
    type Reader = LoggingReader<A::Reader, I>;
    type BlockingReader = LoggingReader<A::BlockingReader, I>;
    type Writer = LoggingWriter<A::Writer, I>;
    type BlockingWriter = LoggingWriter<A::BlockingWriter, I>;
    type Lister = LoggingLister<A::Lister, I>;
    type BlockingLister = LoggingLister<A::BlockingLister, I>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn metadata(&self) -> Arc<AccessorInfo> {
        self.logger
            .log(&self.info, Operation::Info, &[], "started", None);

        let info = self.info.clone();

        self.logger
            .log(&self.info, Operation::Info, &[], "finished", None);

        info
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.logger.log(
            &self.info,
            Operation::CreateDir,
            &[("path", path)],
            "started",
            None,
        );

        self.inner
            .create_dir(path, args)
            .await
            .map(|v| {
                self.logger.log(
                    &self.info,
                    Operation::CreateDir,
                    &[("path", path)],
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::CreateDir,
                    &[("path", path)],
                    "failed",
                    Some(&err),
                );
                err
            })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.logger.log(
            &self.info,
            Operation::Read,
            &[("path", path)],
            "started",
            None,
        );

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| {
                self.logger.log(
                    &self.info,
                    Operation::Read,
                    &[("path", path)],
                    "created reader",
                    None,
                );
                (
                    rp,
                    LoggingReader::new(self.info.clone(), self.logger.clone(), path, r),
                )
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::Read,
                    &[("path", path)],
                    "failed",
                    Some(&err),
                );
                err
            })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.logger.log(
            &self.info,
            Operation::Write,
            &[("path", path)],
            "started",
            None,
        );

        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| {
                self.logger.log(
                    &self.info,
                    Operation::Write,
                    &[("path", path)],
                    "created writer",
                    None,
                );
                let w = LoggingWriter::new(self.info.clone(), self.logger.clone(), path, w);
                (rp, w)
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::Write,
                    &[("path", path)],
                    "failed",
                    Some(&err),
                );
                err
            })
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.logger.log(
            &self.info,
            Operation::Copy,
            &[("from", from), ("to", to)],
            "started",
            None,
        );

        self.inner
            .copy(from, to, args)
            .await
            .map(|v| {
                self.logger.log(
                    &self.info,
                    Operation::Copy,
                    &[("from", from), ("to", to)],
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::Copy,
                    &[("from", from), ("to", to)],
                    "failed",
                    Some(&err),
                );
                err
            })
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.logger.log(
            &self.info,
            Operation::Rename,
            &[("from", from), ("to", to)],
            "started",
            None,
        );

        self.inner
            .rename(from, to, args)
            .await
            .map(|v| {
                self.logger.log(
                    &self.info,
                    Operation::Rename,
                    &[("from", from), ("to", to)],
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::Rename,
                    &[("from", from), ("to", to)],
                    "failed",
                    Some(&err),
                );
                err
            })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.logger.log(
            &self.info,
            Operation::Stat,
            &[("path", path)],
            "started",
            None,
        );

        self.inner
            .stat(path, args)
            .await
            .map(|v| {
                self.logger.log(
                    &self.info,
                    Operation::Stat,
                    &[("path", path)],
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::Stat,
                    &[("path", path)],
                    "failed",
                    Some(&err),
                );
                err
            })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.logger.log(
            &self.info,
            Operation::Delete,
            &[("path", path)],
            "started",
            None,
        );

        self.inner
            .delete(path, args.clone())
            .inspect(|v| match v {
                Ok(_) => {
                    self.logger.log(
                        &self.info,
                        Operation::Delete,
                        &[("path", path)],
                        "finished",
                        None,
                    );
                }
                Err(err) => {
                    self.logger.log(
                        &self.info,
                        Operation::Delete,
                        &[("path", path)],
                        "failed",
                        Some(err),
                    );
                }
            })
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.logger.log(
            &self.info,
            Operation::List,
            &[("path", path)],
            "started",
            None,
        );

        self.inner
            .list(path, args)
            .map(|v| match v {
                Ok((rp, v)) => {
                    self.logger.log(
                        &self.info,
                        Operation::List,
                        &[("path", path)],
                        "created lister",
                        None,
                    );
                    let streamer =
                        LoggingLister::new(self.info.clone(), self.logger.clone(), path, v);
                    Ok((rp, streamer))
                }
                Err(err) => {
                    self.logger.log(
                        &self.info,
                        Operation::List,
                        &[("path", path)],
                        "failed",
                        Some(&err),
                    );
                    Err(err)
                }
            })
            .await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.logger.log(
            &self.info,
            Operation::Presign,
            &[("path", path)],
            "started",
            None,
        );

        self.inner
            .presign(path, args)
            .await
            .map(|v| {
                self.logger.log(
                    &self.info,
                    Operation::Presign,
                    &[("path", path)],
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::Presign,
                    &[("path", path)],
                    "failed",
                    Some(&err),
                );
                err
            })
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let (op, count) = (args.operation()[0].1.operation(), args.operation().len());

        self.logger.log(
            &self.info,
            Operation::Batch,
            &[("op", op.into_static()), ("count", &count.to_string())],
            "started",
            None,
        );

        self.inner
            .batch(args)
            .map_ok(|v| {
                self.logger.log(
                    &self.info,
                    Operation::Batch,
                    &[("op", op.into_static()), ("count", &count.to_string())],
                    &format!(
                        "finished: {}, succeed: {}, failed: {}",
                        v.results().len(),
                        v.results().iter().filter(|(_, v)| v.is_ok()).count(),
                        v.results().iter().filter(|(_, v)| v.is_err()).count(),
                    ),
                    None,
                );
                v
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::Batch,
                    &[("op", op.into_static()), ("count", &count.to_string())],
                    "failed",
                    Some(&err),
                );
                err
            })
            .await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.logger.log(
            &self.info,
            Operation::BlockingCreateDir,
            &[("path", path)],
            "started",
            None,
        );

        self.inner
            .blocking_create_dir(path, args)
            .map(|v| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingCreateDir,
                    &[("path", path)],
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingCreateDir,
                    &[("path", path)],
                    "failed",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.logger.log(
            &self.info,
            Operation::BlockingRead,
            &[("path", path)],
            "started",
            None,
        );

        self.inner
            .blocking_read(path, args.clone())
            .map(|(rp, r)| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingRead,
                    &[("path", path)],
                    "created reader",
                    None,
                );
                let r = LoggingReader::new(self.info.clone(), self.logger.clone(), path, r);
                (rp, r)
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingRead,
                    &[("path", path)],
                    "failed",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.logger.log(
            &self.info,
            Operation::BlockingWrite,
            &[("path", path)],
            "started",
            None,
        );

        self.inner
            .blocking_write(path, args)
            .map(|(rp, w)| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingWrite,
                    &[("path", path)],
                    "created writer",
                    None,
                );
                let w = LoggingWriter::new(self.info.clone(), self.logger.clone(), path, w);
                (rp, w)
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingWrite,
                    &[("path", path)],
                    "failed",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.logger.log(
            &self.info,
            Operation::BlockingCopy,
            &[("from", from), ("to", to)],
            "started",
            None,
        );

        self.inner
            .blocking_copy(from, to, args)
            .map(|v| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingCopy,
                    &[("from", from), ("to", to)],
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingCopy,
                    &[("from", from), ("to", to)],
                    "",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.logger.log(
            &self.info,
            Operation::BlockingRename,
            &[("from", from), ("to", to)],
            "started",
            None,
        );

        self.inner
            .blocking_rename(from, to, args)
            .map(|v| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingRename,
                    &[("from", from), ("to", to)],
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingRename,
                    &[("from", from), ("to", to)],
                    "failed",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.logger.log(
            &self.info,
            Operation::BlockingStat,
            &[("path", path)],
            "started",
            None,
        );

        self.inner
            .blocking_stat(path, args)
            .map(|v| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingStat,
                    &[("path", path)],
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingStat,
                    &[("path", path)],
                    "failed",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.logger.log(
            &self.info,
            Operation::BlockingDelete,
            &[("path", path)],
            "started",
            None,
        );

        self.inner
            .blocking_delete(path, args)
            .map(|v| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingDelete,
                    &[("path", path)],
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingDelete,
                    &[("path", path)],
                    "failed",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.logger.log(
            &self.info,
            Operation::BlockingList,
            &[("path", path)],
            "started",
            None,
        );

        self.inner
            .blocking_list(path, args)
            .map(|(rp, v)| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingList,
                    &[("path", path)],
                    "created lister",
                    None,
                );
                let li = LoggingLister::new(self.info.clone(), self.logger.clone(), path, v);
                (rp, li)
            })
            .map_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::BlockingList,
                    &[("path", path)],
                    "",
                    Some(&err),
                );
                err
            })
    }
}

/// `LoggingReader` is a wrapper of `BytesReader`, with logging functionality.
pub struct LoggingReader<R, I: LoggingInterceptor> {
    info: Arc<AccessorInfo>,
    logger: I,
    path: String,

    read: u64,
    inner: R,
}

impl<R, I: LoggingInterceptor> LoggingReader<R, I> {
    fn new(info: Arc<AccessorInfo>, logger: I, path: &str, reader: R) -> Self {
        Self {
            info,
            logger,
            path: path.to_string(),

            read: 0,
            inner: reader,
        }
    }
}

impl<R: oio::Read, I: LoggingInterceptor> oio::Read for LoggingReader<R, I> {
    async fn read(&mut self) -> Result<Buffer> {
        self.logger.log(
            &self.info,
            Operation::ReaderRead,
            &[("path", &self.path), ("read", &self.read.to_string())],
            "started",
            None,
        );

        match self.inner.read().await {
            Ok(bs) => {
                self.read += bs.len() as u64;
                self.logger.log(
                    &self.info,
                    Operation::ReaderRead,
                    &[
                        ("path", &self.path),
                        ("read", &self.read.to_string()),
                        ("size", &bs.len().to_string()),
                    ],
                    if bs.is_empty() {
                        "finished"
                    } else {
                        "succeeded"
                    },
                    None,
                );
                Ok(bs)
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::ReaderRead,
                    &[("path", &self.path), ("read", &self.read.to_string())],
                    "failed",
                    Some(&err),
                );
                Err(err)
            }
        }
    }
}

impl<R: oio::BlockingRead, I: LoggingInterceptor> oio::BlockingRead for LoggingReader<R, I> {
    fn read(&mut self) -> Result<Buffer> {
        self.logger.log(
            &self.info,
            Operation::BlockingReaderRead,
            &[("path", &self.path), ("read", &self.read.to_string())],
            "started",
            None,
        );

        match self.inner.read() {
            Ok(bs) => {
                self.read += bs.len() as u64;
                self.logger.log(
                    &self.info,
                    Operation::BlockingReaderRead,
                    &[
                        ("path", &self.path),
                        ("read", &self.read.to_string()),
                        ("size", &bs.len().to_string()),
                    ],
                    if bs.is_empty() {
                        "finished"
                    } else {
                        "succeeded"
                    },
                    None,
                );
                Ok(bs)
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::BlockingReaderRead,
                    &[("path", &self.path), ("read", &self.read.to_string())],
                    "failed",
                    Some(&err),
                );
                Err(err)
            }
        }
    }
}

pub struct LoggingWriter<W, I> {
    info: Arc<AccessorInfo>,
    logger: I,
    path: String,

    written: u64,
    inner: W,
}

impl<W, I> LoggingWriter<W, I> {
    fn new(info: Arc<AccessorInfo>, logger: I, path: &str, writer: W) -> Self {
        Self {
            info,
            logger,
            path: path.to_string(),

            written: 0,
            inner: writer,
        }
    }
}

impl<W: oio::Write, I: LoggingInterceptor> oio::Write for LoggingWriter<W, I> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let size = bs.len();

        self.logger.log(
            &self.info,
            Operation::WriterWrite,
            &[
                ("path", &self.path),
                ("written", &self.written.to_string()),
                ("size", &size.to_string()),
            ],
            "started",
            None,
        );

        match self.inner.write(bs).await {
            Ok(_) => {
                self.written += size as u64;
                self.logger.log(
                    &self.info,
                    Operation::WriterWrite,
                    &[
                        ("path", &self.path),
                        ("written", &self.written.to_string()),
                        ("size", &size.to_string()),
                    ],
                    "succeeded",
                    None,
                );
                Ok(())
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::WriterWrite,
                    &[
                        ("path", &self.path),
                        ("written", &self.written.to_string()),
                        ("size", &size.to_string()),
                    ],
                    "failed",
                    Some(&err),
                );
                Err(err)
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        self.logger.log(
            &self.info,
            Operation::WriterAbort,
            &[("path", &self.path), ("written", &self.written.to_string())],
            "started",
            None,
        );

        match self.inner.abort().await {
            Ok(_) => {
                self.logger.log(
                    &self.info,
                    Operation::WriterAbort,
                    &[("path", &self.path), ("written", &self.written.to_string())],
                    "succeeded",
                    None,
                );
                Ok(())
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::WriterAbort,
                    &[("path", &self.path), ("written", &self.written.to_string())],
                    "failed",
                    Some(&err),
                );
                Err(err)
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.logger.log(
            &self.info,
            Operation::WriterClose,
            &[("path", &self.path), ("written", &self.written.to_string())],
            "started",
            None,
        );

        match self.inner.close().await {
            Ok(_) => {
                self.logger.log(
                    &self.info,
                    Operation::WriterClose,
                    &[("path", &self.path), ("written", &self.written.to_string())],
                    "succeeded",
                    None,
                );
                Ok(())
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::WriterClose,
                    &[("path", &self.path), ("written", &self.written.to_string())],
                    "failed",
                    Some(&err),
                );
                Err(err)
            }
        }
    }
}

impl<W: oio::BlockingWrite, I: LoggingInterceptor> oio::BlockingWrite for LoggingWriter<W, I> {
    fn write(&mut self, bs: Buffer) -> Result<()> {
        let size = bs.len();

        self.logger.log(
            &self.info,
            Operation::BlockingWriterWrite,
            &[
                ("path", &self.path),
                ("written", &self.written.to_string()),
                ("size", &size.to_string()),
            ],
            "started",
            None,
        );

        match self.inner.write(bs) {
            Ok(_) => {
                self.logger.log(
                    &self.info,
                    Operation::BlockingWriterWrite,
                    &[
                        ("path", &self.path),
                        ("written", &self.written.to_string()),
                        ("size", &size.to_string()),
                    ],
                    "succeeded",
                    None,
                );
                Ok(())
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::BlockingWriterWrite,
                    &[
                        ("path", &self.path),
                        ("written", &self.written.to_string()),
                        ("size", &size.to_string()),
                    ],
                    "failed",
                    Some(&err),
                );
                Err(err)
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.logger.log(
            &self.info,
            Operation::BlockingWriterClose,
            &[("path", &self.path), ("written", &self.written.to_string())],
            "started",
            None,
        );

        match self.inner.close() {
            Ok(_) => {
                self.logger.log(
                    &self.info,
                    Operation::BlockingWriterWrite,
                    &[("path", &self.path), ("written", &self.written.to_string())],
                    "succeeded",
                    None,
                );
                Ok(())
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::BlockingWriterClose,
                    &[("path", &self.path), ("written", &self.written.to_string())],
                    "failed",
                    Some(&err),
                );
                Err(err)
            }
        }
    }
}

pub struct LoggingLister<P, I: LoggingInterceptor> {
    info: Arc<AccessorInfo>,
    logger: I,
    path: String,

    listed: usize,
    inner: P,
}

impl<P, I: LoggingInterceptor> LoggingLister<P, I> {
    fn new(info: Arc<AccessorInfo>, logger: I, path: &str, inner: P) -> Self {
        Self {
            info,
            logger,
            path: path.to_string(),

            listed: 0,
            inner,
        }
    }
}

impl<P: oio::List, I: LoggingInterceptor> oio::List for LoggingLister<P, I> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.logger.log(
            &self.info,
            Operation::ListerNext,
            &[("path", &self.path), ("listed", &self.listed.to_string())],
            "started",
            None,
        );

        let res = self.inner.next().await;

        match &res {
            Ok(Some(de)) => {
                self.listed += 1;
                self.logger.log(
                    &self.info,
                    Operation::ListerNext,
                    &[
                        ("path", &self.path),
                        ("listed", &self.listed.to_string()),
                        ("entry", de.path()),
                    ],
                    "succeeded",
                    None,
                );
            }
            Ok(None) => {
                self.logger.log(
                    &self.info,
                    Operation::ListerNext,
                    &[("path", &self.path), ("listed", &self.listed.to_string())],
                    "finished",
                    None,
                );
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::ListerNext,
                    &[("path", &self.path), ("listed", &self.listed.to_string())],
                    "failed",
                    Some(err),
                );
            }
        };

        res
    }
}

impl<P: oio::BlockingList, I: LoggingInterceptor> oio::BlockingList for LoggingLister<P, I> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.logger.log(
            &self.info,
            Operation::BlockingListerNext,
            &[("path", &self.path), ("listed", &self.listed.to_string())],
            "started",
            None,
        );

        let res = self.inner.next();
        match &res {
            Ok(Some(de)) => {
                self.listed += 1;
                self.logger.log(
                    &self.info,
                    Operation::BlockingListerNext,
                    &[
                        ("path", &self.path),
                        ("listed", &self.listed.to_string()),
                        ("entry", de.path()),
                    ],
                    "succeeded",
                    None,
                );
            }
            Ok(None) => {
                self.logger.log(
                    &self.info,
                    Operation::BlockingListerNext,
                    &[("path", &self.path), ("listed", &self.listed.to_string())],
                    "finished",
                    None,
                );
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::BlockingListerNext,
                    &[("path", &self.path), ("listed", &self.listed.to_string())],
                    "failed",
                    Some(err),
                );
            }
        };

        res
    }
}
