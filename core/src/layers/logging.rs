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
///
/// # Logging Interceptor
///
/// You can implement your own logging interceptor to customize the logging behavior.
///
/// ```no_run
/// use opendal::layers::LoggingInterceptor;
/// use opendal::layers::LoggingLayer;
/// use opendal::raw::Operation;
/// use opendal::services;
/// use opendal::Error;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// #[derive(Debug, Clone)]
/// struct MyLoggingInterceptor;
///
/// impl LoggingInterceptor for MyLoggingInterceptor {
///     fn log(
///         &self,
///         scheme: Scheme,
///         operation: Operation,
///         context: &str,
///         message: &str,
///         err: Option<&Error>,
///     ) {
///         // log something
///     }
/// }
///
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(LoggingLayer::new(MyLoggingInterceptor))
///     .finish();
/// ```
#[derive(Debug)]
pub struct LoggingLayer<I = DefaultLoggingInterceptor> {
    notify: Arc<I>,
}

impl<I> Clone for LoggingLayer<I> {
    fn clone(&self) -> Self {
        Self {
            notify: self.notify.clone(),
        }
    }
}

impl Default for LoggingLayer {
    fn default() -> Self {
        Self {
            notify: Arc::new(DefaultLoggingInterceptor),
        }
    }
}

impl LoggingLayer {
    /// Create the layer with specific logging interceptor.
    pub fn new<I: LoggingInterceptor>(notify: I) -> LoggingLayer<I> {
        LoggingLayer {
            notify: Arc::new(notify),
        }
    }
}

impl<A: Access, I: LoggingInterceptor> Layer<A> for LoggingLayer<I> {
    type LayeredAccess = LoggingAccessor<A, I>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let meta = inner.info();
        LoggingAccessor {
            inner,

            ctx: LoggingContext {
                scheme: meta.scheme(),
                notify: self.notify.clone(),
            },
        }
    }
}

#[derive(Debug)]
pub struct LoggingContext<I> {
    scheme: Scheme,
    notify: Arc<I>,
}

impl<I> Clone for LoggingContext<I> {
    fn clone(&self) -> Self {
        Self {
            scheme: self.scheme,
            notify: self.notify.clone(),
        }
    }
}

impl<I: LoggingInterceptor> LoggingContext<I> {
    fn log(&self, operation: Operation, context: &str, message: &str, err: Option<&Error>) {
        self.notify
            .log(self.scheme, operation, context, message, err)
    }

    fn log_with_path(&self, operation: Operation, path: &str, message: &str, err: Option<&Error>) {
        self.notify.log(
            self.scheme,
            operation,
            &format!("path={path}"),
            message,
            err,
        )
    }
}

/// LoggingInterceptor is used to intercept the log.
pub trait LoggingInterceptor: Debug + Send + Sync + 'static {
    /// Everytime there is a log, this function will be called.
    ///
    /// # Inputs
    ///
    /// - scheme: The service generates the log.
    /// - operation: The operation to log.
    /// - context: Additional context of the log.
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
        scheme: Scheme,
        operation: Operation,
        context: &str,
        message: &str,
        err: Option<&Error>,
    );
}

/// The DefaultLoggingInterceptor will log the message by the standard logging macro.
#[derive(Debug)]
pub struct DefaultLoggingInterceptor;

impl LoggingInterceptor for DefaultLoggingInterceptor {
    fn log(
        &self,
        scheme: Scheme,
        operation: Operation,
        context: &str,
        message: &str,
        err: Option<&Error>,
    ) {
        let Some(err) = err else {
            let lvl = self.operation_level(operation);
            log!(
                target: LOGGING_TARGET,
                lvl,
                "service={} operation={} {} -> {}",
                scheme,
                operation,
                context,
                message,
            );
            return;
        };

        let lvl = self.error_level(err);
        log!(
            target: LOGGING_TARGET,
            lvl,
            "service={} operation={} {} -> {} {}",
            scheme,
            operation,
            context,
            message,
            err,
        );
    }
}

impl DefaultLoggingInterceptor {
    fn operation_level(&self, operation: Operation) -> Level {
        match operation {
            Operation::ReaderRead
            | Operation::BlockingReaderRead
            | Operation::WriterWrite
            | Operation::BlockingWriterWrite => Level::Trace,
            _ => Level::Debug,
        }
    }

    #[inline]
    fn error_level(&self, err: &Error) -> Level {
        if err.kind() == ErrorKind::Unexpected {
            Level::Error
        } else {
            Level::Warn
        }
    }
}

#[derive(Clone, Debug)]
pub struct LoggingAccessor<A: Access, I: LoggingInterceptor> {
    inner: A,

    ctx: LoggingContext<I>,
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
        self.ctx.log(Operation::Info, "", "started", None);
        let result = self.inner.info();
        self.ctx.log(
            Operation::Info,
            "",
            &format!("finished: {:?}", result),
            None,
        );

        result
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.ctx
            .log_with_path(Operation::CreateDir, path, "started", None);

        self.inner
            .create_dir(path, args)
            .await
            .map(|v| {
                self.ctx
                    .log_with_path(Operation::CreateDir, path, "finished", None);
                v
            })
            .map_err(|err| {
                self.ctx
                    .log_with_path(Operation::CreateDir, path, "", Some(&err));
                err
            })
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.ctx
            .log_with_path(Operation::Read, path, "started", None);

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| {
                self.ctx
                    .log_with_path(Operation::Read, path, "got reader", None);
                (
                    rp,
                    LoggingReader::new(self.ctx.clone(), Operation::Read, path, r),
                )
            })
            .map_err(|err| {
                self.ctx
                    .log_with_path(Operation::Read, path, "", Some(&err));
                err
            })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.ctx
            .log_with_path(Operation::Write, path, "started", None);

        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| {
                self.ctx
                    .log_with_path(Operation::Write, path, "start writing", None);
                let w = LoggingWriter::new(self.ctx.clone(), Operation::Write, path, w);
                (rp, w)
            })
            .map_err(|err| {
                self.ctx
                    .log_with_path(Operation::Write, path, "", Some(&err));
                err
            })
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.ctx.log(
            Operation::Copy,
            &format!("from={from} to={to}"),
            "started",
            None,
        );

        self.inner
            .copy(from, to, args)
            .await
            .map(|v| {
                self.ctx.log(
                    Operation::Copy,
                    &format!("from={from} to={to}"),
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.ctx.log(
                    Operation::Copy,
                    &format!("from={from} to={to}"),
                    "",
                    Some(&err),
                );
                err
            })
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.ctx.log(
            Operation::Rename,
            &format!("from={from} to={to}"),
            "started",
            None,
        );

        self.inner
            .rename(from, to, args)
            .await
            .map(|v| {
                self.ctx.log(
                    Operation::Rename,
                    &format!("from={from} to={to}"),
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.ctx.log(
                    Operation::Rename,
                    &format!("from={from} to={to}"),
                    "",
                    Some(&err),
                );
                err
            })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.ctx
            .log_with_path(Operation::Stat, path, "started", None);

        self.inner
            .stat(path, args)
            .await
            .map(|v| {
                self.ctx
                    .log_with_path(Operation::Stat, path, "finished", None);
                v
            })
            .map_err(|err| {
                self.ctx
                    .log_with_path(Operation::Stat, path, "", Some(&err));
                err
            })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.ctx
            .log_with_path(Operation::Delete, path, "started", None);

        self.inner
            .delete(path, args.clone())
            .inspect(|v| match v {
                Ok(_) => {
                    self.ctx
                        .log_with_path(Operation::Delete, path, "finished", None);
                }
                Err(err) => {
                    self.ctx
                        .log_with_path(Operation::Delete, path, "", Some(err));
                }
            })
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner
            .list(path, args)
            .map(|v| match v {
                Ok((rp, v)) => {
                    self.ctx
                        .log_with_path(Operation::List, path, "start listing dir", None);
                    let streamer = LoggingLister::new(self.ctx.clone(), path, Operation::List, v);
                    Ok((rp, streamer))
                }
                Err(err) => {
                    self.ctx
                        .log_with_path(Operation::List, path, "", Some(&err));
                    Err(err)
                }
            })
            .await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.ctx
            .log_with_path(Operation::Presign, path, "started", None);

        self.inner
            .presign(path, args)
            .await
            .map(|v| {
                self.ctx
                    .log_with_path(Operation::Presign, path, "finished", None);
                v
            })
            .map_err(|err| {
                self.ctx
                    .log_with_path(Operation::Presign, path, "", Some(&err));
                err
            })
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let (op, count) = (args.operation()[0].1.operation(), args.operation().len());

        self.ctx.log(
            Operation::Batch,
            &format!("op={op} count={count} -> started"),
            "started",
            None,
        );

        self.inner
            .batch(args)
            .map_ok(|v| {
                self.ctx.log(
                    Operation::Batch,
                    &format!("op={op} count={count}"),
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
                self.ctx.log(
                    Operation::Batch,
                    &format!("op={op} count={count}"),
                    "",
                    Some(&err),
                );
                err
            })
            .await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.ctx
            .log_with_path(Operation::BlockingCreateDir, path, "started", None);

        self.inner
            .blocking_create_dir(path, args)
            .map(|v| {
                self.ctx
                    .log_with_path(Operation::BlockingCreateDir, path, "finished", None);
                v
            })
            .map_err(|err| {
                self.ctx
                    .log_with_path(Operation::BlockingCreateDir, path, "", Some(&err));
                err
            })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.ctx
            .log_with_path(Operation::BlockingRead, path, "started", None);

        self.inner
            .blocking_read(path, args.clone())
            .map(|(rp, r)| {
                self.ctx
                    .log_with_path(Operation::BlockingRead, path, "got reader", None);
                let r = LoggingReader::new(self.ctx.clone(), Operation::BlockingRead, path, r);
                (rp, r)
            })
            .map_err(|err| {
                self.ctx
                    .log_with_path(Operation::BlockingRead, path, "", Some(&err));
                err
            })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.ctx
            .log_with_path(Operation::BlockingWrite, path, "started", None);

        self.inner
            .blocking_write(path, args)
            .map(|(rp, w)| {
                self.ctx
                    .log_with_path(Operation::BlockingWrite, path, "start writing", None);
                let w = LoggingWriter::new(self.ctx.clone(), Operation::BlockingWrite, path, w);
                (rp, w)
            })
            .map_err(|err| {
                self.ctx
                    .log_with_path(Operation::BlockingWrite, path, "", Some(&err));
                err
            })
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.ctx.log(
            Operation::BlockingCopy,
            &format!("from={from} to={to}"),
            "started",
            None,
        );

        self.inner
            .blocking_copy(from, to, args)
            .map(|v| {
                self.ctx.log(
                    Operation::BlockingCopy,
                    &format!("from={from} to={to}"),
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.ctx.log(
                    Operation::BlockingCopy,
                    &format!("from={from} to={to}"),
                    "",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.ctx.log(
            Operation::BlockingRename,
            &format!("from={from} to={to}"),
            "started",
            None,
        );

        self.inner
            .blocking_rename(from, to, args)
            .map(|v| {
                self.ctx.log(
                    Operation::BlockingRename,
                    &format!("from={from} to={to}"),
                    "finished",
                    None,
                );
                v
            })
            .map_err(|err| {
                self.ctx.log(
                    Operation::BlockingRename,
                    &format!("from={from} to={to}"),
                    "",
                    Some(&err),
                );
                err
            })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.ctx
            .log_with_path(Operation::BlockingStat, path, "started", None);

        self.inner
            .blocking_stat(path, args)
            .map(|v| {
                self.ctx
                    .log_with_path(Operation::BlockingStat, path, "finished", None);
                v
            })
            .map_err(|err| {
                self.ctx
                    .log_with_path(Operation::BlockingStat, path, "", Some(&err));
                err
            })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.ctx
            .log_with_path(Operation::BlockingDelete, path, "started", None);

        self.inner
            .blocking_delete(path, args)
            .map(|v| {
                self.ctx
                    .log_with_path(Operation::BlockingDelete, path, "finished", None);
                v
            })
            .map_err(|err| {
                self.ctx
                    .log_with_path(Operation::BlockingDelete, path, "", Some(&err));
                err
            })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.ctx
            .log_with_path(Operation::BlockingList, path, "started", None);

        self.inner
            .blocking_list(path, args)
            .map(|(rp, v)| {
                self.ctx
                    .log_with_path(Operation::BlockingList, path, "got dir", None);
                let li = LoggingLister::new(self.ctx.clone(), path, Operation::BlockingList, v);
                (rp, li)
            })
            .map_err(|err| {
                self.ctx
                    .log_with_path(Operation::BlockingList, path, "", Some(&err));
                err
            })
    }
}

/// `LoggingReader` is a wrapper of `BytesReader`, with logging functionality.
pub struct LoggingReader<R, I: LoggingInterceptor> {
    ctx: LoggingContext<I>,
    path: String,
    op: Operation,

    read: AtomicU64,
    inner: R,
}

impl<R, I: LoggingInterceptor> LoggingReader<R, I> {
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
        self.ctx.log(
            self.op,
            &format!(
                "path={} read={}",
                self.path,
                self.read.load(Ordering::Relaxed)
            ),
            "data read finished",
            None,
        );
    }
}

impl<R: oio::Read, I: LoggingInterceptor> oio::Read for LoggingReader<R, I> {
    async fn read(&mut self) -> Result<Buffer> {
        match self.inner.read().await {
            Ok(bs) => {
                self.read
                    .fetch_add(bs.remaining() as u64, Ordering::Relaxed);
                self.ctx.log(
                    Operation::ReaderRead,
                    &format!(
                        "path={} read={}",
                        self.path,
                        self.read.load(Ordering::Relaxed),
                    ),
                    &format!("read returns {}B", bs.remaining()),
                    None,
                );
                Ok(bs)
            }
            Err(err) => {
                self.ctx.log(
                    Operation::ReaderRead,
                    &format!(
                        "path={} read={}",
                        self.path,
                        self.read.load(Ordering::Relaxed)
                    ),
                    "read failed:",
                    Some(&err),
                );
                Err(err)
            }
        }
    }
}

impl<R: oio::BlockingRead, I: LoggingInterceptor> oio::BlockingRead for LoggingReader<R, I> {
    fn read(&mut self) -> Result<Buffer> {
        match self.inner.read() {
            Ok(bs) => {
                self.read
                    .fetch_add(bs.remaining() as u64, Ordering::Relaxed);
                self.ctx.log(
                    Operation::BlockingReaderRead,
                    &format!(
                        "path={} read={}",
                        self.path,
                        self.read.load(Ordering::Relaxed),
                    ),
                    &format!("read returns {}B", bs.remaining()),
                    None,
                );
                Ok(bs)
            }
            Err(err) => {
                self.ctx.log(
                    Operation::BlockingReaderRead,
                    &format!(
                        "path={} read={}",
                        self.path,
                        self.read.load(Ordering::Relaxed)
                    ),
                    "read failed:",
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
                self.ctx.log(
                    Operation::WriterWrite,
                    &format!("path={} written={}B", self.path, self.written),
                    &format!("data write {}B", size),
                    None,
                );
                Ok(())
            }
            Err(err) => {
                self.ctx.log(
                    Operation::WriterWrite,
                    &format!("path={} written={}B", self.path, self.written),
                    "data write failed:",
                    Some(&err),
                );
                Err(err)
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        match self.inner.abort().await {
            Ok(_) => {
                self.ctx.log(
                    Operation::WriterAbort,
                    &format!("path={} written={}B", self.path, self.written),
                    "abort writer",
                    None,
                );
                Ok(())
            }
            Err(err) => {
                self.ctx.log(
                    Operation::WriterAbort,
                    &format!("path={} written={}B", self.path, self.written),
                    "abort writer failed:",
                    Some(&err),
                );
                Err(err)
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        match self.inner.close().await {
            Ok(_) => {
                self.ctx.log(
                    self.op,
                    &format!("path={} written={}B", self.path, self.written),
                    "data written finished",
                    None,
                );
                Ok(())
            }
            Err(err) => {
                self.ctx.log(
                    Operation::WriterClose,
                    &format!("path={} written={}B", self.path, self.written),
                    "data close failed:",
                    Some(&err),
                );
                Err(err)
            }
        }
    }
}

impl<W: oio::BlockingWrite, I: LoggingInterceptor> oio::BlockingWrite for LoggingWriter<W, I> {
    fn write(&mut self, bs: Buffer) -> Result<()> {
        match self.inner.write(bs.clone()) {
            Ok(_) => {
                self.ctx.log(
                    Operation::BlockingWriterWrite,
                    &format!("path={} written={}B", self.path, self.written),
                    &format!("data write {}B", bs.len()),
                    None,
                );
                Ok(())
            }
            Err(err) => {
                self.ctx.log(
                    Operation::BlockingWriterWrite,
                    &format!("path={} written={}B", self.path, self.written),
                    "data write failed:",
                    Some(&err),
                );
                Err(err)
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        match self.inner.close() {
            Ok(_) => {
                self.ctx.log(
                    self.op,
                    &format!("path={} written={}B", self.path, self.written),
                    "data written finished",
                    None,
                );
                Ok(())
            }
            Err(err) => {
                self.ctx.log(
                    Operation::BlockingWriterClose,
                    &format!("path={} written={}B", self.path, self.written),
                    "data close failed:",
                    Some(&err),
                );
                Err(err)
            }
        }
    }
}

pub struct LoggingLister<P, I: LoggingInterceptor> {
    ctx: LoggingContext<I>,
    path: String,
    op: Operation,

    finished: bool,
    inner: P,
}

impl<P, I: LoggingInterceptor> LoggingLister<P, I> {
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

impl<P, I: LoggingInterceptor> Drop for LoggingLister<P, I> {
    fn drop(&mut self) {
        if self.finished {
            self.ctx
                .log_with_path(self.op, &self.path, "all entries read finished", None);
        } else {
            self.ctx
                .log_with_path(self.op, &self.path, "partial entries read finished", None);
        }
    }
}

impl<P: oio::List, I: LoggingInterceptor> oio::List for LoggingLister<P, I> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let res = self.inner.next().await;

        match &res {
            Ok(Some(de)) => {
                self.ctx.log_with_path(
                    self.op,
                    &self.path,
                    &format!("listed entry: {}", de.path()),
                    None,
                );
            }
            Ok(None) => {
                self.ctx
                    .log_with_path(self.op, &self.path, "finished", None);
                self.finished = true;
            }
            Err(err) => {
                self.ctx.log_with_path(self.op, &self.path, "", Some(err));
            }
        };

        res
    }
}

impl<P: oio::BlockingList, I: LoggingInterceptor> oio::BlockingList for LoggingLister<P, I> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        let res = self.inner.next();

        match &res {
            Ok(Some(des)) => {
                self.ctx.log_with_path(
                    self.op,
                    &self.path,
                    &format!("listed entry: {}", des.path()),
                    None,
                );
            }
            Ok(None) => {
                self.ctx
                    .log_with_path(self.op, &self.path, "finished", None);
                self.finished = true;
            }
            Err(err) => {
                self.ctx.log_with_path(self.op, &self.path, "", Some(err));
            }
        };

        res
    }
}
