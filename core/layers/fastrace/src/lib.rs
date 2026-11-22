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
use std::future::Future;
use std::future::poll_fn;
use std::pin::Pin;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use fastrace::prelude::*;
use futures::Stream;
use opendal_core::raw::*;
use opendal_core::*;

#[cfg(test)]
mod tests;

/// `FastraceLayer` traces OpenDAL operations with
/// [fastrace](https://docs.rs/fastrace/).
///
/// It creates spans for service calls and deferred operation bodies such as
/// readers, writers, listers, deleters, and copiers. It also traces outbound
/// HTTP requests through response body consumption and propagates trace context
/// to tasks spawned through the operation's executor.
///
/// # Examples
///
/// ## Basic Setup
///
/// ```no_run
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_fastrace::FastraceLayer;
/// #
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(FastraceLayer::new());
/// # Ok(())
/// # }
/// ```
///
/// ## Real usage
///
/// ```no_run
/// # use anyhow::Result;
/// # use fastrace::prelude::*;
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_layer_fastrace::FastraceLayer;
/// #
/// # fn main() -> Result<()> {
/// let reporter = fastrace_jaeger::JaegerReporter::new("127.0.0.1:6831".parse()?, "opendal").unwrap();
/// fastrace::set_reporter(reporter, fastrace::collector::Config::default());
///
/// {
///     let root = Span::root("op", SpanContext::random());
///     let runtime = tokio::runtime::Runtime::new()?;
///     runtime.block_on(
///         async {
///             let _ = dotenvy::dotenv();
///             let op = Operator::new(services::Memory::default())?
///                 .layer(FastraceLayer::new());
///             op.write("test", "0".repeat(16 * 1024 * 1024).into_bytes())
///                 .await?;
///             op.stat("test").await?;
///             op.read("test").await?;
///             Ok::<(), opendal_core::Error>(())
///         }
///         .in_span(Span::enter_with_parent("test", &root)),
///     )?;
/// }
///
/// fastrace::flush();
/// # Ok(())
/// # }
/// ```
///
/// # Output
///
/// OpenDAL is using [`fastrace`](https://docs.rs/fastrace/latest/fastrace/) for tracing internally.
///
/// To enable fastrace output, initialize a reporter supported by `fastrace`.
///
/// For example:
///
/// ```no_run
/// # use anyhow::Result;
/// #
/// # fn main() -> Result<()> {
/// let reporter = fastrace_jaeger::JaegerReporter::new("127.0.0.1:6831".parse()?, "opendal").unwrap();
/// fastrace::set_reporter(reporter, fastrace::collector::Config::default());
/// # Ok(())
/// # }
/// ```
///
/// For real-world usage, take a look at [`fastrace-datadog`](https://crates.io/crates/fastrace-datadog) or [`fastrace-jaeger`](https://crates.io/crates/fastrace-jaeger).
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct FastraceLayer {}

impl FastraceLayer {
    /// Create a new [`FastraceLayer`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl Layer for FastraceLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(FastraceAccessor { inner })
    }

    fn apply_context(&self, _service: Servicer, inner: OperationContext) -> OperationContext {
        let (transport, executor) = inner.into_parts();
        OperationContext::from_parts(
            HttpTransporter::new(FastraceHttpTransport { inner: transport }),
            Executor::with(FastraceExecutor {
                inner: executor.into_inner(),
            }),
        )
    }
}

struct FastraceHttpTransport {
    inner: HttpTransporter,
}

impl HttpTransport for FastraceHttpTransport {
    async fn fetch(&self, req: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        let span = Span::enter_with_local_parent("http::fetch");
        let mut fetch = pin!(self.inner.fetch(req));
        // Enter the span for each poll so the response body can take ownership of it.
        let response = poll_fn(|cx| {
            let _guard = span.set_local_parent();
            fetch.as_mut().poll(cx)
        })
        .await?;

        let (parts, body) = response.into_parts();
        let body = body.map_inner(|inner| {
            Box::new(FastraceHttpStream {
                inner,
                span: Some(span),
            })
        });
        Ok(http::Response::from_parts(parts, body))
    }
}

struct FastraceHttpStream<S> {
    inner: S,
    span: Option<Span>,
}

impl<S: Stream<Item = Result<Buffer>> + Unpin> Stream for FastraceHttpStream<S> {
    type Item = Result<Buffer>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = {
            let _guard = self.span.as_ref().map(Span::set_local_parent);
            Pin::new(&mut self.inner).poll_next(cx)
        };
        if matches!(result, Poll::Ready(None | Some(Err(_)))) {
            self.span.take();
        }
        result
    }
}

struct FastraceExecutor {
    inner: Arc<dyn Execute>,
}

impl Execute for FastraceExecutor {
    fn execute(&self, f: BoxedStaticFuture<()>) {
        let span = Span::enter_with_local_parent("executor::execute");
        self.inner.execute(Box::pin(f.in_span(span)));
    }

    fn try_execute(&self, f: BoxedStaticFuture<()>) -> Result<()> {
        let span = Span::enter_with_local_parent("executor::execute");
        self.inner.try_execute(Box::pin(f.in_span(span)))
    }

    fn timeout(&self) -> Option<BoxedStaticFuture<()>> {
        self.inner.timeout()
    }

    fn sleep(&self, duration: Duration) -> Option<BoxedStaticFuture<()>> {
        self.inner.sleep(duration)
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct FastraceAccessor {
    inner: Servicer,
}

impl Service for FastraceAccessor {
    // Operations with returned bodies continue after the service call returns,
    // so wrap those bodies to trace deferred IO as well.
    type Reader = FastraceWrapper<oio::Reader>;
    type Writer = FastraceWrapper<oio::Writer>;
    type Lister = FastraceWrapper<oio::Lister>;
    type Deleter = FastraceWrapper<oio::Deleter>;
    type Copier = FastraceWrapper<oio::Copier>;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let span = Span::enter_with_local_parent(Operation::CreateDir.into_static());
        async { self.inner.create_dir(ctx, path, args).await }
            .in_span(span)
            .await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let span = Span::enter_with_local_parent(Operation::Read.into_static());
        let _guard = span.set_local_parent();
        self.inner
            .read(ctx, path, args)
            .map(|reader| FastraceWrapper::new(span, reader))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let span = Span::enter_with_local_parent(Operation::Write.into_static());
        let _guard = span.set_local_parent();
        self.inner
            .write(ctx, path, args)
            .map(|writer| FastraceWrapper::new(span, writer))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        let span = Span::enter_with_local_parent(Operation::Copy.into_static());
        let _guard = span.set_local_parent();
        self.inner
            .copy(ctx, from, to, args)
            .map(|copier| FastraceWrapper::new(span, copier))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        let span = Span::enter_with_local_parent(Operation::Rename.into_static());
        async { self.inner.rename(ctx, from, to, args).await }
            .in_span(span)
            .await
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        let span = Span::enter_with_local_parent(Operation::Restore.into_static());
        async { self.inner.restore(ctx, path, args).await }
            .in_span(span)
            .await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let span = Span::enter_with_local_parent(Operation::Stat.into_static());
        async { self.inner.stat(ctx, path, args).await }
            .in_span(span)
            .await
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let span = Span::enter_with_local_parent(Operation::Delete.into_static());
        let _guard = span.set_local_parent();
        self.inner
            .delete(ctx)
            .map(|deleter| FastraceWrapper::new(span, deleter))
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let span = Span::enter_with_local_parent(Operation::List.into_static());
        let _guard = span.set_local_parent();
        self.inner
            .list(ctx, path, args)
            .map(|lister| FastraceWrapper::new(span, lister))
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        let span = Span::enter_with_local_parent(Operation::Presign.into_static());
        async { self.inner.presign(ctx, path, args).await }
            .in_span(span)
            .await
    }
}

#[doc(hidden)]
// Keep the operation span with the returned body so later body methods can
// attach child spans without relying on local span state.
pub struct FastraceWrapper<R> {
    span: Arc<Span>,
    inner: R,
}

impl<R> FastraceWrapper<R> {
    fn new(span: Span, inner: R) -> Self {
        Self {
            span: Arc::new(span),
            inner,
        }
    }

    fn with_span(span: Arc<Span>, inner: R) -> Self {
        Self { span, inner }
    }
}

impl<R: oio::ReadStream> oio::ReadStream for FastraceWrapper<R> {
    fn read(&mut self) -> impl Future<Output = Result<Buffer>> + MaybeSend {
        let span = Span::enter_with_parent(Operation::Read.into_static(), &self.span);
        let _guard = span.set_local_parent();
        self.inner.read().in_span(span)
    }
}

impl<R: oio::Read> oio::Read for FastraceWrapper<R> {
    fn open(
        &self,
        range: BytesRange,
    ) -> impl Future<Output = Result<(RpRead, Box<dyn oio::ReadStreamDyn>)>> + MaybeSend {
        let span = Span::enter_with_parent(Operation::Read.into_static(), &self.span);
        let _guard = span.set_local_parent();
        let reader_span = self.span.clone();
        let open = self.inner.open(range).in_span(span);
        async move {
            let (rp, stream) = open.await?;
            Ok((
                rp,
                Box::new(FastraceWrapper::with_span(reader_span, stream))
                    as Box<dyn oio::ReadStreamDyn>,
            ))
        }
    }

    fn read(
        &self,
        range: BytesRange,
    ) -> impl Future<Output = Result<(RpRead, Buffer)>> + MaybeSend {
        let span = Span::enter_with_parent(Operation::Read.into_static(), &self.span);
        let _guard = span.set_local_parent();
        self.inner.read(range).in_span(span)
    }
}

impl<R: oio::Write> oio::Write for FastraceWrapper<R> {
    fn write(&mut self, bs: Buffer) -> impl Future<Output = Result<()>> + MaybeSend {
        let span = Span::enter_with_parent(Operation::Write.into_static(), &self.span);
        let _guard = span.set_local_parent();
        self.inner.write(bs).in_span(span)
    }

    fn copy_from(
        &mut self,
        path: &str,
        args: OpRead,
        range: BytesRange,
    ) -> impl Future<Output = Result<()>> + MaybeSend {
        let span = Span::enter_with_parent(Operation::Write.into_static(), &self.span);
        async move { self.inner.copy_from(path, args, range).await }.in_span(span)
    }

    fn abort(&mut self) -> impl Future<Output = Result<()>> + MaybeSend {
        let span = Span::enter_with_parent(Operation::Write.into_static(), &self.span);
        let _guard = span.set_local_parent();
        self.inner.abort().in_span(span)
    }

    fn close(&mut self) -> impl Future<Output = Result<Metadata>> + MaybeSend {
        let span = Span::enter_with_parent(Operation::Write.into_static(), &self.span);
        let _guard = span.set_local_parent();
        self.inner.close().in_span(span)
    }
}

impl<R: oio::List> oio::List for FastraceWrapper<R> {
    fn next(&mut self) -> impl Future<Output = Result<Option<oio::Entry>>> + MaybeSend {
        let span = Span::enter_with_parent(Operation::List.into_static(), &self.span);
        let _guard = span.set_local_parent();
        self.inner.next().in_span(span)
    }
}

impl<R: oio::Delete> oio::Delete for FastraceWrapper<R> {
    fn delete<'a>(
        &'a mut self,
        path: &'a str,
        args: OpDelete,
    ) -> impl Future<Output = Result<()>> + MaybeSend + 'a {
        let span = Span::enter_with_parent(Operation::Delete.into_static(), &self.span);
        let _guard = span.set_local_parent();
        self.inner.delete(path, args).in_span(span)
    }

    fn close(&mut self) -> impl Future<Output = Result<()>> + MaybeSend {
        let span = Span::enter_with_parent(Operation::Delete.into_static(), &self.span);
        let _guard = span.set_local_parent();
        self.inner.close().in_span(span)
    }
}

impl<C: oio::Copy> oio::Copy for FastraceWrapper<C> {
    fn next(&mut self) -> impl Future<Output = Result<Option<usize>>> + MaybeSend {
        let span = Span::enter_with_parent(Operation::Copy.into_static(), &self.span);
        let _guard = span.set_local_parent();
        self.inner.next().in_span(span)
    }

    fn close(&mut self) -> impl Future<Output = Result<Metadata>> + MaybeSend {
        let span = Span::enter_with_parent(Operation::Copy.into_static(), &self.span);
        let _guard = span.set_local_parent();
        self.inner.close().in_span(span)
    }

    fn abort(&mut self) -> impl Future<Output = Result<()>> + MaybeSend {
        let span = Span::enter_with_parent(Operation::Copy.into_static(), &self.span);
        let _guard = span.set_local_parent();
        self.inner.abort().in_span(span)
    }
}
