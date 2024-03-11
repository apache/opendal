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
use tracing::Span;

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
/// let _ = Operator::new(services::Memory::default())
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
///         op.stat("test").await.expect("must succeed");
///         op.read("test").await.expect("must succeed");
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<A: Accessor> LayeredAccessor for TracingAccessor<A> {
    type Inner = A;
    type Reader = TracingWrapper<A::Reader>;
    type BlockingReader = TracingWrapper<A::BlockingReader>;
    type Writer = TracingWrapper<A::Writer>;
    type BlockingWriter = TracingWrapper<A::BlockingWriter>;
    type Lister = TracingWrapper<A::Lister>;
    type BlockingLister = TracingWrapper<A::BlockingLister>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    #[tracing::instrument(level = "debug")]
    fn metadata(&self) -> AccessorInfo {
        self.inner.info()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner.create_dir(path, args).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner
            .read(path, args)
            .map(|v| v.map(|(rp, r)| (rp, TracingWrapper::new(Span::current(), r))))
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner
            .write(path, args)
            .await
            .map(|(rp, r)| (rp, TracingWrapper::new(Span::current(), r)))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner().copy(from, to, args).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner().rename(from, to, args).await
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
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner
            .list(path, args)
            .map(|v| v.map(|(rp, s)| (rp, TracingWrapper::new(Span::current(), s))))
            .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.inner.batch(args).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner.blocking_create_dir(path, args)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, TracingWrapper::new(Span::current(), r)))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner
            .blocking_write(path, args)
            .map(|(rp, r)| (rp, TracingWrapper::new(Span::current(), r)))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner().blocking_copy(from, to, args)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner().blocking_rename(from, to, args)
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
    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.inner
            .blocking_list(path, args)
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

impl<R: oio::Read> oio::Read for TracingWrapper<R> {
    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        skip_all)]
    async fn next_v2(&mut self, size: usize) -> Result<Bytes> {
        self.inner.next_v2(size).await
    }

    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        skip_all)]
    async fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        self.inner.seek(pos).await
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for TracingWrapper<R> {
    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        skip_all)]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf)
    }

    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        skip_all)]
    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        self.inner.seek(pos)
    }

    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        skip_all)]
    fn next(&mut self) -> Option<Result<Bytes>> {
        self.inner.next()
    }
}

impl<R: oio::Write> oio::Write for TracingWrapper<R> {
    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        skip_all)]
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        self.inner.poll_write(cx, bs)
    }

    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        skip_all)]
    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.inner.poll_abort(cx)
    }

    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        skip_all)]
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.inner.poll_close(cx)
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for TracingWrapper<R> {
    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        skip_all)]
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        self.inner.write(bs)
    }

    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        skip_all)]
    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }
}

impl<R: oio::List> oio::List for TracingWrapper<R> {
    #[tracing::instrument(parent = &self.span, level = "debug", skip_all)]
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<oio::Entry>>> {
        self.inner.poll_next(cx)
    }
}

impl<R: oio::BlockingList> oio::BlockingList for TracingWrapper<R> {
    #[tracing::instrument(parent = &self.span, level = "debug", skip_all)]
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.inner.next()
    }
}
