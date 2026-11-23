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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::future::Either;
use futures::future::select;
use opendal_core::raw::*;
use opendal_core::*;

/// `TimeoutLayer` adds deadlines to operations so slow or stalled work cannot
/// hang indefinitely.
///
/// For example, a dead connection could stall a database query. `TimeoutLayer`
/// interrupts the operation and returns an error that users can retry or report.
///
/// # Notes
///
/// `TimeoutLayer` has two default timeouts.
///
/// - `timeout` bounds asynchronous service calls such as `stat`, `create_dir`,
///   `rename`, `restore`, and `presign`.
/// - `io_timeout` bounds each I/O body method and each concurrent I/O task that
///   uses the executor's timeout. This includes opening a read range, reading
///   from its stream, fetching a list entry, and uploading a multipart part.
///
/// [`with_operation_timeout`](Self::with_operation_timeout) replaces the
/// applicable default for the specified operation.
///
/// # Default
///
/// - timeout: 60 seconds
/// - io_timeout: 10 seconds
///
/// # Cancellation Safety
///
/// `TimeoutLayer` enforces deadlines by dropping the in-flight future when a
/// timeout is reached. Lower layers and operation handles must therefore tolerate
/// dropped futures, and callers should follow each handle's recovery contract
/// after a timeout (for example, aborting an interrupted writer or reopening a
/// read stream when using a retry budget).
///
/// When composing `TimeoutLayer` with `RetryLayer`, layer order selects what
/// the timeout bounds:
///
/// - Adding `TimeoutLayer` before `RetryLayer` applies the timeout to each
///   individual attempt so `RetryLayer` can retry timed-out attempts.
/// - Adding `TimeoutLayer` after `RetryLayer` bounds total time across all
///   retries of one call without losing `RetryLayer` handle state.
///
/// ```no_run
/// # use std::time::Duration;
/// #
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_retry::RetryLayer;
/// # use opendal_layer_timeout::TimeoutLayer;
/// #
/// # fn main() -> Result<()> {
/// let op = Operator::new(services::Memory::default())?
///     // Time out each retry attempt so RetryLayer can retry it.
///     .layer(TimeoutLayer::default().with_io_timeout(Duration::from_secs(10)))
///     .layer(RetryLayer::default())
///     // Optional outer deadline across all retries of one call.
///     .layer(TimeoutLayer::default().with_io_timeout(Duration::from_secs(30)));
/// # Ok(())
/// # }
/// ```
///
/// # Examples
///
/// The following example sets defaults of 10 seconds for control operations
/// and 3 seconds for I/O. Each lister call may take up to 15 seconds.
///
/// ```no_run
/// # use std::time::Duration;
/// #
/// use opendal_core::raw::Operation;
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_timeout::TimeoutLayer;
/// #
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(
///         TimeoutLayer::default()
///             .with_timeout(Duration::from_secs(10))
///             .with_io_timeout(Duration::from_secs(3))
///             .with_operation_timeout(Operation::List, Duration::from_secs(15)),
///     );
/// # Ok(())
/// # }
/// ```
///
/// # Implementation Notes
///
/// `TimeoutLayer` uses the configured executor's timer to bound service calls,
/// I/O body methods, and individual concurrent I/O tasks. It uses Tokio timers
/// when the executor does not implement [`Execute::sleep`].
///
/// This introduces a small amount of overhead for IO operations, but it is needed
/// to implement timeouts correctly. OpenDAL used to implement this as a
/// zero-cost deadline check that only stored an [`Instant`] and compared it with
/// the current time. However, that approach does not work for all cases.
///
/// For example, a user's TCP connection could enter the
/// [Busy ESTAB](https://blog.cloudflare.com/when-tcp-sockets-refuse-to-die)
/// state. In this state, the connection emits no IO events, so the runtime never
/// polls the future again. The future hangs until Linux closes the connection
/// after it reaches the
/// [net.ipv4.tcp_retries2](https://man7.org/linux/man-pages/man7/tcp.7.html)
/// limit.
#[derive(Clone, Debug)]
pub struct TimeoutLayer {
    timeout: Duration,
    io_timeout: Duration,
    operation_timeouts: HashMap<Operation, Duration>,
}

impl Default for TimeoutLayer {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            io_timeout: Duration::from_secs(10),
            operation_timeouts: HashMap::new(),
        }
    }
}

impl TimeoutLayer {
    /// Create a new [`TimeoutLayer`] with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the default timeout for control operations such as `stat` and `rename`.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the default timeout for I/O body methods and concurrent I/O tasks.
    pub fn with_io_timeout(mut self, timeout: Duration) -> Self {
        self.io_timeout = timeout;
        self
    }

    /// Set the timeout for the specified operation.
    ///
    /// Control operations use this value in place of [`with_timeout`](Self::with_timeout).
    /// Operations with I/O bodies such as read, write, list, delete, copy, and compose
    /// use it in place of [`with_io_timeout`](Self::with_io_timeout) for each async
    /// body method and each concurrent I/O task that uses the executor's timeout.
    /// For example, a write override applies to each write, close, or abort call
    /// and each multipart upload task separately. Synchronous calls such as
    /// [`Operation::Info`] and I/O body construction have no deadline.
    ///
    /// Each call replaces any previous override for the same operation.
    pub fn with_operation_timeout(mut self, operation: Operation, timeout: Duration) -> Self {
        self.operation_timeouts.insert(operation, timeout);
        self
    }
}

impl Layer for TimeoutLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(TimeoutService {
            inner,
            config: self.clone(),
        })
    }

    fn apply_context(&self, _srv: Servicer, inner: OperationContext) -> OperationContext {
        TimeoutExecutor::wrap_context(&inner, self.io_timeout)
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct TimeoutService {
    inner: Servicer,
    config: TimeoutLayer,
}

impl TimeoutService {
    async fn timeout<F: Future<Output = Result<T>>, T>(
        &self,
        ctx: &OperationContext,
        op: Operation,
        fut: F,
    ) -> Result<T> {
        let timeout = self
            .config
            .operation_timeouts
            .get(&op)
            .copied()
            .unwrap_or(self.config.timeout);
        execute_with_timeout(
            ctx.executor().inner(),
            timeout,
            op.into_static(),
            "operation timeout reached",
            fut,
        )
        .await
    }

    fn io_context<'a>(
        &self,
        ctx: &'a OperationContext,
        op: Operation,
    ) -> (Duration, Cow<'a, OperationContext>) {
        let Some(&timeout) = self.config.operation_timeouts.get(&op) else {
            return (self.config.io_timeout, Cow::Borrowed(ctx));
        };

        // Bodies such as MultipartWriter retain the executor for their individual tasks.
        (
            timeout,
            Cow::Owned(TimeoutExecutor::wrap_context(ctx, timeout)),
        )
    }
}

impl Service for TimeoutService {
    type Reader = TimeoutWrapper<oio::Reader>;
    type Writer = TimeoutWrapper<oio::Writer>;
    type Lister = TimeoutWrapper<oio::Lister>;
    type Deleter = TimeoutWrapper<oio::Deleter>;
    type Copier = TimeoutWrapper<oio::Copier>;
    type Composer = TimeoutWrapper<oio::Composer>;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.timeout(
            ctx,
            Operation::CreateDir,
            self.inner.create_dir(ctx, path, args),
        )
        .await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let (timeout, ctx) = self.io_context(ctx, Operation::Read);
        self.inner
            .read(&ctx, path, args)
            .map(|r| TimeoutWrapper::new(r, timeout, ctx.executor().clone()))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let (timeout, ctx) = self.io_context(ctx, Operation::Write);
        self.inner
            .write(&ctx, path, args)
            .map(|r| TimeoutWrapper::new(r, timeout, ctx.executor().clone()))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        let (timeout, ctx) = self.io_context(ctx, Operation::Copy);
        self.inner
            .copy(&ctx, from, to, args)
            .map(|c| TimeoutWrapper::new(c, timeout, ctx.executor().clone()))
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        let (timeout, ctx) = self.io_context(ctx, Operation::Compose);
        self.inner
            .compose(&ctx, to, args)
            .map(|c| TimeoutWrapper::new(c, timeout, ctx.executor().clone()))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.timeout(
            ctx,
            Operation::Rename,
            self.inner.rename(ctx, from, to, args),
        )
        .await
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        self.timeout(ctx, Operation::Restore, self.inner.restore(ctx, path, args))
            .await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.timeout(ctx, Operation::Stat, self.inner.stat(ctx, path, args))
            .await
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let (timeout, ctx) = self.io_context(ctx, Operation::Delete);
        self.inner
            .delete(&ctx)
            .map(|r| TimeoutWrapper::new(r, timeout, ctx.executor().clone()))
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let (timeout, ctx) = self.io_context(ctx, Operation::List);
        self.inner
            .list(&ctx, path, args)
            .map(|r| TimeoutWrapper::new(r, timeout, ctx.executor().clone()))
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.timeout(ctx, Operation::Presign, self.inner.presign(ctx, path, args))
            .await
    }
}

struct TimeoutExecutor {
    exec: Arc<dyn Execute>,
    timeout: Duration,
}

impl TimeoutExecutor {
    fn wrap_context(ctx: &OperationContext, timeout: Duration) -> OperationContext {
        ctx.with_executor(Executor::with(Self {
            exec: ctx.executor().clone().into_inner(),
            timeout,
        }))
    }
}

impl Execute for TimeoutExecutor {
    fn execute(&self, f: BoxedStaticFuture<()>) {
        self.exec.execute(f)
    }

    fn try_execute(&self, f: BoxedStaticFuture<()>) -> Result<()> {
        self.exec.try_execute(f)
    }

    fn timeout(&self) -> Option<BoxedStaticFuture<()>> {
        Some(timer(self.exec.as_ref(), self.timeout))
    }

    fn sleep(&self, duration: Duration) -> Option<BoxedStaticFuture<()>> {
        self.exec.sleep(duration)
    }
}

#[doc(hidden)]
pub struct TimeoutWrapper<R> {
    inner: R,
    timeout: Duration,
    executor: Executor,
}

impl<R> TimeoutWrapper<R> {
    fn new(inner: R, timeout: Duration, executor: Executor) -> Self {
        Self {
            inner,
            timeout,
            executor,
        }
    }

    async fn io_timeout<F: Future<Output = Result<T>>, T>(
        executor: &Executor,
        timeout: Duration,
        op: Operation,
        fut: F,
    ) -> Result<T> {
        execute_with_timeout(
            executor.inner(),
            timeout,
            op.into_static(),
            "io operation timeout reached",
            fut,
        )
        .await
    }
}

fn timer(executor: &dyn Execute, duration: Duration) -> BoxedStaticFuture<()> {
    executor
        .sleep(duration)
        .unwrap_or_else(|| Box::pin(tokio::time::sleep(duration)))
}

async fn execute_with_timeout<F: Future<Output = Result<T>>, T>(
    executor: &dyn Execute,
    duration: Duration,
    operation: &'static str,
    message: &'static str,
    fut: F,
) -> Result<T> {
    let deadline = timer(executor, duration);
    futures::pin_mut!(fut);
    match select(fut, deadline).await {
        Either::Left((result, _)) => result,
        Either::Right(_) => Err(Error::new(ErrorKind::Unexpected, message)
            .with_operation(operation)
            .with_context("timeout", duration.as_secs_f64().to_string())
            .set_temporary()),
    }
}

impl<R: oio::ReadStream> oio::ReadStream for TimeoutWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        let fut = self.inner.read();
        Self::io_timeout(&self.executor, self.timeout, Operation::Read, fut).await
    }
}

impl<R: oio::Read> oio::Read for TimeoutWrapper<R> {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (rp, stream) = Self::io_timeout(
            &self.executor,
            self.timeout,
            Operation::Read,
            self.inner.open(range),
        )
        .await?;
        Ok((
            rp,
            Box::new(TimeoutWrapper::new(
                stream,
                self.timeout,
                self.executor.clone(),
            )) as Box<dyn oio::ReadStreamDyn>,
        ))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        Self::io_timeout(
            &self.executor,
            self.timeout,
            Operation::Read,
            self.inner.read(range),
        )
        .await
    }
}

impl<R: oio::Write> oio::Write for TimeoutWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let fut = self.inner.write(bs);
        Self::io_timeout(&self.executor, self.timeout, Operation::Write, fut).await
    }

    async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
        let fut = self.inner.copy_from(path, args, range);
        Self::io_timeout(&self.executor, self.timeout, Operation::Write, fut).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let fut = self.inner.close();
        Self::io_timeout(&self.executor, self.timeout, Operation::Write, fut).await
    }

    async fn abort(&mut self) -> Result<()> {
        let fut = self.inner.abort();
        Self::io_timeout(&self.executor, self.timeout, Operation::Write, fut).await
    }
}

impl<R: oio::List> oio::List for TimeoutWrapper<R> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let fut = self.inner.next();
        Self::io_timeout(&self.executor, self.timeout, Operation::List, fut).await
    }
}

impl<R: oio::Delete> oio::Delete for TimeoutWrapper<R> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        let fut = self.inner.delete(path, args);
        Self::io_timeout(&self.executor, self.timeout, Operation::Delete, fut).await
    }

    async fn close(&mut self) -> Result<()> {
        let fut = self.inner.close();
        Self::io_timeout(&self.executor, self.timeout, Operation::Delete, fut).await
    }
}

impl<C: oio::Copy> oio::Copy for TimeoutWrapper<C> {
    async fn next(&mut self) -> Result<Option<usize>> {
        let fut = self.inner.next();
        Self::io_timeout(&self.executor, self.timeout, Operation::Copy, fut).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let fut = self.inner.close();
        Self::io_timeout(&self.executor, self.timeout, Operation::Copy, fut).await
    }

    async fn abort(&mut self) -> Result<()> {
        let fut = self.inner.abort();
        Self::io_timeout(&self.executor, self.timeout, Operation::Copy, fut).await
    }
}

impl<C: oio::Compose> oio::Compose for TimeoutWrapper<C> {
    async fn compose(&mut self, path: &str, args: OpRead) -> Result<()> {
        let fut = self.inner.compose(path, args);
        Self::io_timeout(&self.executor, self.timeout, Operation::Compose, fut).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let fut = self.inner.close();
        Self::io_timeout(&self.executor, self.timeout, Operation::Compose, fut).await
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use futures::StreamExt;
    use tokio::time::{Instant, sleep, timeout};

    use super::*;

    #[derive(Debug, Default)]
    struct MockService {
        stat_delay: Duration,
        write_part_delay: Duration,
    }

    impl Service for MockService {
        type Reader = MockReader;
        type Writer = oio::MultipartWriter<MockMultipartUpload>;
        type Lister = MockLister;
        type Deleter = MockDeleter;
        type Copier = MockCopier;
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("mock")
        }

        fn capability(&self) -> Capability {
            Capability {
                stat: true,
                read: true,
                write: true,
                write_can_multi: true,
                delete: true,
                list: true,
                copy: true,
                ..Default::default()
            }
        }

        async fn create_dir(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpCreateDir,
        ) -> Result<RpCreateDir> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
            sleep(self.stat_delay).await;
            Ok(RpStat::new(MetadataBuilder::file(4).build()))
        }

        /// Return a reader whose reads never complete.
        fn read(&self, _ctx: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
            Ok(MockReader)
        }

        fn write(&self, ctx: &OperationContext, _: &str, args: OpWrite) -> Result<Self::Writer> {
            Ok(oio::MultipartWriter::new(
                ctx.executor().clone(),
                MockMultipartUpload(self.write_part_delay),
                args.concurrent(),
            ))
        }

        /// Return a deleter whose operations never complete.
        fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
            Ok(MockDeleter)
        }

        fn list(&self, _ctx: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
            Ok(MockLister)
        }

        fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
            Ok(MockCopier)
        }

        async fn rename(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: OpRename,
        ) -> Result<RpRename> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockReader;

    impl oio::Read for MockReader {
        async fn open(&self, _: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            Ok((RpRead::default(), Box::new(MockReader)))
        }

        async fn read(&self, _: BytesRange) -> Result<(RpRead, Buffer)> {
            pending().await
        }
    }

    impl oio::ReadStream for MockReader {
        async fn read(&mut self) -> Result<Buffer> {
            pending().await
        }
    }

    struct MockMultipartUpload(Duration);

    impl oio::MultipartWrite for MockMultipartUpload {
        async fn write_once(&self, _: u64, _: Buffer) -> Result<Metadata> {
            unreachable!("the test must upload multiple parts")
        }

        async fn initiate_part(&self) -> Result<String> {
            Ok("upload".into())
        }

        async fn write_part(
            &self,
            _: &str,
            part_number: usize,
            size: u64,
            _: Buffer,
        ) -> Result<oio::MultipartPart> {
            sleep(self.0).await;
            Ok(oio::MultipartPart {
                part_number,
                etag: part_number.to_string(),
                checksum: None,
                size: Some(size),
            })
        }

        async fn complete_part(&self, _: &str, parts: &[oio::MultipartPart]) -> Result<Metadata> {
            let size = parts.iter().map(|part| part.size.unwrap()).sum();
            Ok(MetadataBuilder::file(size).build())
        }

        async fn abort_part(&self, _: &str) -> Result<()> {
            Ok(())
        }
    }

    struct TestExecutor;

    impl Execute for TestExecutor {
        fn execute(&self, future: BoxedStaticFuture<()>) {
            tokio::spawn(future);
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockLister;

    impl oio::List for MockLister {
        async fn next(&mut self) -> Result<Option<oio::Entry>> {
            pending().await
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockDeleter;

    impl oio::Delete for MockDeleter {
        async fn delete(&mut self, _: &str, _: OpDelete) -> Result<()> {
            pending().await
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockCopier;

    impl oio::Copy for MockCopier {
        async fn next(&mut self) -> Result<Option<usize>> {
            pending().await
        }

        async fn close(&mut self) -> Result<Metadata> {
            pending().await
        }

        async fn abort(&mut self) -> Result<()> {
            pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_stat_timeout_override() {
        for (override_secs, should_succeed) in [(1, false), (5, true)] {
            let service = MockService {
                stat_delay: Duration::from_secs(3),
                ..Default::default()
            };
            let op = Operator::from_parts(OperationContext::default(), Arc::new(service)).layer(
                TimeoutLayer::new()
                    .with_timeout(Duration::from_secs(2))
                    .with_operation_timeout(Operation::Stat, Duration::from_secs(override_secs)),
            );

            let start = Instant::now();
            let result = op.stat("test").await;
            if should_succeed {
                assert_eq!(result.unwrap().content_length(), 4);
            } else {
                let err = result.unwrap_err();
                assert_eq!(err.kind(), ErrorKind::Unexpected);
                assert_eq!(err.message(), "operation timeout reached");
                assert_eq!(start.elapsed().as_secs(), override_secs);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_list_timeout_override_and_default_fallbacks() {
        let service = MockService {
            stat_delay: Duration::from_secs(6),
            ..Default::default()
        };
        let op = Operator::from_parts(OperationContext::default(), Arc::new(service)).layer(
            TimeoutLayer::new()
                .with_timeout(Duration::from_secs(4))
                .with_io_timeout(Duration::from_secs(2))
                .with_operation_timeout(Operation::List, Duration::from_secs(1)),
        );

        let mut lister = op.lister("test").await.unwrap();
        let start = Instant::now();
        let err = lister.next().await.unwrap().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert_eq!(err.message(), "io operation timeout reached");
        assert_eq!(start.elapsed().as_secs(), 1);

        let start = Instant::now();
        let err = op.stat("test").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert_eq!(err.message(), "operation timeout reached");
        assert_eq!(start.elapsed().as_secs(), 4);

        let start = Instant::now();
        let err = op.delete("test").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert_eq!(err.message(), "io operation timeout reached");
        assert_eq!(start.elapsed().as_secs(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn test_read_and_concurrent_multipart_write_timeout_overrides() {
        for (part_secs, should_succeed) in [(3, true), (6, false)] {
            let service = MockService {
                write_part_delay: Duration::from_secs(part_secs),
                ..Default::default()
            };
            let ctx = OperationContext::default().with_executor(Executor::with(TestExecutor));
            let op = Operator::from_parts(ctx, Arc::new(service)).layer(
                TimeoutLayer::new()
                    .with_io_timeout(Duration::from_secs(2))
                    .with_operation_timeout(Operation::Read, Duration::from_secs(1))
                    .with_operation_timeout(Operation::Write, Duration::from_secs(5)),
            );

            let reader = op.reader("test").await.unwrap();
            let start = Instant::now();
            let err = reader.read(0..4).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert_eq!(err.message(), "io operation timeout reached");
            assert_eq!(start.elapsed().as_secs(), 1);

            let mut writer = op.writer_with("test").concurrent(2).await.unwrap();
            writer.write("a").await.unwrap();
            let start = Instant::now();
            writer.write("b").await.unwrap();
            // Start closing after the first part's timer has started so the
            // part deadline can expire before the close deadline.
            sleep(Duration::from_secs(1)).await;
            let result = writer.close().await;
            if should_succeed {
                assert_eq!(result.unwrap().content_length(), 2);
            } else {
                let err = result.unwrap_err();
                assert_eq!(err.kind(), ErrorKind::Unexpected);
                assert_eq!(err.message(), "write part timeout");
                assert_eq!(start.elapsed().as_secs(), 5);
            }
        }
    }

    #[tokio::test]
    async fn test_delete_timeout() {
        let srv = MockService::default();
        let op = Operator::from_parts(OperationContext::default(), Arc::new(srv))
            .layer(TimeoutLayer::default().with_io_timeout(Duration::from_secs(1)));

        let fut = async {
            let res = op.delete("test").await;
            assert!(res.is_err());
            let err = res.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.to_string().contains("timeout"))
        };

        timeout(Duration::from_secs(2), fut)
            .await
            .expect("this test should not exceed 2 seconds")
    }

    #[tokio::test]
    async fn test_io_timeout() {
        let srv = MockService::default();
        let op = Operator::from_parts(OperationContext::default(), Arc::new(srv))
            .layer(TimeoutLayer::default().with_io_timeout(Duration::from_secs(1)));

        let reader = op.reader("test").await.unwrap();

        let res = reader.read(0..4).await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.to_string().contains("timeout"))
    }

    #[tokio::test]
    async fn test_list_timeout() {
        let srv = MockService::default();
        let op = Operator::from_parts(OperationContext::default(), Arc::new(srv)).layer(
            TimeoutLayer::default()
                .with_timeout(Duration::from_secs(1))
                .with_io_timeout(Duration::from_secs(1)),
        );

        let mut lister = op.lister("test").await.unwrap();

        let res = lister.next().await.unwrap();
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.to_string().contains("timeout"))
    }

    #[tokio::test]
    async fn test_delete_io_timeout() {
        use oio::Delete;

        let mut deleter =
            TimeoutWrapper::new(MockDeleter, Duration::from_secs(1), Executor::default());

        let res = deleter.delete("test", OpDelete::default()).await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.to_string().contains("timeout"));
    }

    #[tokio::test]
    async fn test_copy_io_timeout() {
        use oio::Copy;

        let service = TimeoutLayer::default()
            .with_io_timeout(Duration::from_millis(100))
            .apply_service(Arc::new(MockService::default()));
        let ctx = OperationContext::new();
        let mut copier = service.copy(&ctx, "f", "t", OpCopy::default()).unwrap();

        let err = copier.next().await.unwrap_err();
        assert!(err.to_string().contains("timeout"));
    }

    #[tokio::test]
    async fn test_list_timeout_raw() {
        use oio::List;

        let timeout_layer = TimeoutLayer::default()
            .with_timeout(Duration::from_secs(1))
            .with_io_timeout(Duration::from_secs(1));
        let service = timeout_layer.apply_service(Arc::new(MockService::default()));
        let ctx = OperationContext::new();

        let mut lister = service.list(&ctx, "test", OpList::default()).unwrap();

        let res = lister.next().await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.to_string().contains("timeout"));
    }
}
