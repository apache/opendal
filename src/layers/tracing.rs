// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::io;
use std::io::Read;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use futures::AsyncRead;
use futures::FutureExt;
use tracing::Span;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Add [tracing](https://docs.rs/tracing/) for every operations.
///
/// # Examples
///
/// ## Basic Setup
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::TracingLayer;
/// use opendal::services;
/// use opendal::Operator;
///
/// let _ = Operator::create(services::Memory::default())
///     .expect("must init")
///     .layer(TracingLayer)
///     .finish();
/// ```
///
/// ## Real usage
///
/// ```no_run
/// use std::error::Error;
///
/// use anyhow::Result;
/// use opendal::layers::TracingLayer;
/// use opendal::services;
/// use opendal::Operator;
/// use opentelemetry::global;
/// use tracing::span;
/// use tracing_subscriber::prelude::*;
/// use tracing_subscriber::EnvFilter;
///
/// fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
///     let tracer = opentelemetry_jaeger::new_pipeline()
///         .with_service_name("opendal_example")
///         .install_simple()?;
///     let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
///     tracing_subscriber::registry()
///         .with(EnvFilter::from_default_env())
///         .with(opentelemetry)
///         .try_init()?;
///
///     let runtime = tokio::runtime::Runtime::new()?;
///
///     runtime.block_on(async {
///         let root = span!(tracing::Level::INFO, "app_start", work_units = 2);
///         let _enter = root.enter();
///
///         let _ = dotenvy::dotenv();
///         let op = Operator::from_env::<services::S3>()
///             .expect("init operator must succeed")
///             .layer(TracingLayer)
///             .finish();
///
///         op.object("test")
///             .write("0".repeat(16 * 1024 * 1024).into_bytes())
///             .await
///             .expect("must succeed");
///         op.object("test").stat().await.expect("must succeed");
///         op.object("test").read().await.expect("must succeed");
///     });
///
///     // Shut down the current tracer provider. This will invoke the shutdown
///     // method on all span processors. span processors should export remaining
///     // spans before return.
///     global::shutdown_tracer_provider();
///     Ok(())
/// }
/// ```
///
/// # Output
///
/// OpenDAL is using [`tracing`](https://docs.rs/tracing/latest/tracing/) for tracing internally.
///
/// To enable tracing output, please init one of the subscribers that `tracing` supports.
///
/// For example:
///
/// ```ignore
/// extern crate tracing;
///
/// let my_subscriber = FooSubscriber::new();
/// tracing::subscriber::set_global_default(my_subscriber)
///     .expect("setting tracing default failed");
/// ```
///
/// For real-world usage, please take a look at [`tracing-opentelemetry`](https://crates.io/crates/tracing-opentelemetry).
pub struct TracingLayer;

impl<A: Accessor> Layer<A> for TracingLayer {
    type LayeredAccessor = TracingAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        TracingAccessor { inner }
    }
}

#[derive(Debug)]
pub struct TracingAccessor<A> {
    inner: A,
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for TracingAccessor<A> {
    type Inner = A;
    type Reader = TracingWrapper<A::Reader>;
    type BlockingReader = TracingWrapper<A::BlockingReader>;
    type Pager = TracingWrapper<A::Pager>;
    type BlockingPager = TracingWrapper<A::BlockingPager>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    #[tracing::instrument(level = "debug")]
    fn metadata(&self) -> AccessorMetadata {
        self.inner.metadata()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner.create(path, args).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner
            .read(path, args)
            .map(|v| v.map(|(rp, r)| (rp, TracingWrapper::new(Span::current(), r))))
            .await
    }

    #[tracing::instrument(level = "debug", skip(self, r))]
    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
        let r = Box::new(TracingWrapper::new(Span::current(), r));
        self.inner.write(path, args, r).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(path, args).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.delete(path, args).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.inner
            .list(path, args)
            .map(|v| v.map(|(rp, s)| (rp, TracingWrapper::new(Span::current(), s))))
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        self.inner
            .scan(path, args)
            .map(|v| v.map(|(rp, s)| (rp, TracingWrapper::new(Span::current(), s))))
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.inner.batch(args).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> Result<RpCreateMultipart> {
        self.inner.create_multipart(path, args).await
    }

    #[tracing::instrument(level = "debug", skip(self, r))]
    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: input::Reader,
    ) -> Result<RpWriteMultipart> {
        let r = Box::new(TracingWrapper::new(Span::current(), r));
        self.inner.write_multipart(path, args, r).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> Result<RpCompleteMultipart> {
        self.inner.complete_multipart(path, args).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> Result<RpAbortMultipart> {
        self.inner.abort_multipart(path, args).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner.blocking_create(path, args)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, TracingWrapper::new(Span::current(), r)))
    }

    #[tracing::instrument(level = "debug", skip(self, r))]
    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
        r: input::BlockingReader,
    ) -> Result<RpWrite> {
        self.inner.blocking_write(path, args, r)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.blocking_stat(path, args)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.blocking_delete(path, args)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner
            .blocking_list(path, args)
            .map(|(rp, it)| (rp, TracingWrapper::new(Span::current(), it)))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        self.inner
            .blocking_scan(path, args)
            .map(|(rp, it)| (rp, TracingWrapper::new(Span::current(), it)))
    }
}

pub struct TracingWrapper<R> {
    span: Span,
    inner: R,
}

impl<R> TracingWrapper<R> {
    fn new(span: Span, inner: R) -> Self {
        Self { span, inner }
    }
}

impl<R: output::Read> output::Read for TracingWrapper<R> {
    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        skip_all)]
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        self.inner.poll_read(cx, buf)
    }

    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        skip_all)]
    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<io::Result<u64>> {
        self.inner.poll_seek(cx, pos)
    }

    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        skip_all)]
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Bytes>>> {
        self.inner.poll_next(cx)
    }
}

impl<R: input::Read> AsyncRead for TracingWrapper<R> {
    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        fields(size = buf.len())
        skip_all)]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<R: output::BlockingRead> output::BlockingRead for TracingWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }

    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }

    fn next(&mut self) -> Option<io::Result<Bytes>> {
        self.inner.next()
    }
}

impl<R: input::BlockingRead> Read for TracingWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

#[async_trait]
impl<R: output::Page> output::Page for TracingWrapper<R> {
    #[tracing::instrument(parent = &self.span, level = "debug", skip_all)]
    async fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        self.inner.next_page().await
    }
}

impl<R: output::BlockingPage> output::BlockingPage for TracingWrapper<R> {
    #[tracing::instrument(parent = &self.span, level = "debug", skip_all)]
    fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        self.inner.next_page()
    }
}
