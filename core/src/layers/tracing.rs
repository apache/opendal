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
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use futures::StreamExt;
use tracing::{span, Level, Span};

use crate::raw::*;
use crate::*;

/// Add [tracing](https://docs.rs/tracing/) for every operation.
///
/// # Examples
///
/// ## Basic Setup
///
/// ```no_run
/// # use opendal::layers::TracingLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opendal::Result;
///
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(TracingLayer)
///     .finish();
/// Ok(())
/// # }
/// ```
///
/// ## Real usage
///
/// ```no_run
/// # use anyhow::Result;
/// # use opendal::layers::TracingLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opentelemetry::KeyValue;
/// # use opentelemetry_sdk::trace;
/// # use opentelemetry_sdk::Resource;
/// # use tracing_subscriber::prelude::*;
/// # use tracing_subscriber::EnvFilter;
///
/// # fn main() -> Result<()> {
/// use opentelemetry::trace::TracerProvider;
/// let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
///     .with_simple_exporter(opentelemetry_otlp::SpanExporter::builder().with_tonic().build()?)
///     .with_resource(Resource::builder().with_attributes(vec![
///         KeyValue::new("service.name", "opendal_example"),
///     ]).build())
///     .build();
/// let tracer = tracer_provider.tracer("opendal_tracer");
/// let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
///
/// tracing_subscriber::registry()
///     .with(EnvFilter::from_default_env())
///     .with(opentelemetry)
///     .try_init()?;
///
/// {
///     let runtime = tokio::runtime::Runtime::new()?;
///     runtime.block_on(async {
///         let root = tracing::span!(tracing::Level::INFO, "app_start", work_units = 2);
///         let _enter = root.enter();
///
///         let _ = dotenvy::dotenv();
///         let op = Operator::new(services::Memory::default())?
///             .layer(TracingLayer)
///             .finish();
///
///         op.write("test", "0".repeat(16 * 1024 * 1024).into_bytes())
///             .await?;
///         op.stat("test").await?;
///         op.read("test").await?;
///         Ok::<(), opendal::Error>(())
///     })?;
/// }
///
/// // Shut down the current tracer provider.
/// // This will invoke the shutdown method on all span processors.
/// // span processors should export remaining spans before return.
/// tracer_provider.shutdown()?;
///
/// Ok(())
/// # }
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
/// ```no_run
/// # use tracing::dispatcher;
/// # use tracing::Event;
/// # use tracing::Metadata;
/// # use tracing::span::Attributes;
/// # use tracing::span::Id;
/// # use tracing::span::Record;
/// # use tracing::subscriber::Subscriber;
///
/// # pub struct FooSubscriber;
/// # impl Subscriber for FooSubscriber {
/// #   fn enabled(&self, _: &Metadata) -> bool { false }
/// #   fn new_span(&self, _: &Attributes) -> Id { Id::from_u64(0) }
/// #   fn record(&self, _: &Id, _: &Record) {}
/// #   fn record_follows_from(&self, _: &Id, _: &Id) {}
/// #   fn event(&self, _: &Event) {}
/// #   fn enter(&self, _: &Id) {}
/// #   fn exit(&self, _: &Id) {}
/// # }
/// # impl FooSubscriber { fn new() -> Self { FooSubscriber } }
///
/// let my_subscriber = FooSubscriber::new();
/// tracing::subscriber::set_global_default(my_subscriber).expect("setting tracing default failed");
/// ```
///
/// For real-world usage, please take a look at [`tracing-opentelemetry`](https://crates.io/crates/tracing-opentelemetry).
pub struct TracingLayer;

impl<A: Access> Layer<A> for TracingLayer {
    type LayeredAccess = TracingAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();

        // Update http client with metrics http fetcher.
        info.update_http_client(|client| {
            HttpClient::with(TracingHttpFetcher {
                inner: client.into_inner(),
            })
        });

        TracingAccessor { inner }
    }
}

pub struct TracingHttpFetcher {
    inner: HttpFetcher,
}

impl HttpFetch for TracingHttpFetcher {
    async fn fetch(&self, req: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        let span = span!(Level::DEBUG, "http::fetch", ?req);

        let resp = {
            let _enter = span.enter();
            self.inner.fetch(req).await?
        };

        let (parts, body) = resp.into_parts();
        let body = body.map_inner(|s| Box::new(TracingStream { inner: s, span }));
        Ok(http::Response::from_parts(parts, body))
    }
}

pub struct TracingStream<S> {
    inner: S,
    span: Span,
}

impl<S> Stream for TracingStream<S>
where
    S: Stream<Item = Result<Buffer>> + Unpin + 'static,
{
    type Item = Result<Buffer>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let _enter = self.span.clone().entered();
        self.inner.poll_next_unpin(cx)
    }
}

#[derive(Debug)]
pub struct TracingAccessor<A> {
    inner: A,
}

impl<A: Access> LayeredAccess for TracingAccessor<A> {
    type Inner = A;
    type Reader = TracingWrapper<A::Reader>;
    type Writer = TracingWrapper<A::Writer>;
    type Lister = TracingWrapper<A::Lister>;
    type Deleter = TracingWrapper<A::Deleter>;
    type BlockingReader = TracingWrapper<A::BlockingReader>;
    type BlockingWriter = TracingWrapper<A::BlockingWriter>;
    type BlockingLister = TracingWrapper<A::BlockingLister>;
    type BlockingDeleter = TracingWrapper<A::BlockingDeleter>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner.create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let span = span!(Level::DEBUG, "read", path, ?args);

        let (rp, r) = {
            let _enter = span.enter();
            self.inner.read(path, args).await?
        };

        Ok((rp, TracingWrapper::new(span, r)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let span = span!(Level::DEBUG, "write", path, ?args);

        let (rp, r) = {
            let _enter = span.enter();
            self.inner.write(path, args).await?
        };

        Ok((rp, TracingWrapper::new(span, r)))
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

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let span = span!(Level::DEBUG, "delete");

        let (rp, r) = {
            let _enter = span.enter();
            self.inner.delete().await?
        };

        Ok((rp, TracingWrapper::new(span, r)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let span = span!(Level::DEBUG, "list", path, ?args);

        let (rp, r) = {
            let _enter = span.enter();
            self.inner.list(path, args).await?
        };

        Ok((rp, TracingWrapper::new(span, r)))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner.blocking_create_dir(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let span = span!(Level::DEBUG, "read", path, ?args);

        let (rp, r) = {
            let _enter = span.enter();
            self.inner.blocking_read(path, args)?
        };

        Ok((rp, TracingWrapper::new(span, r)))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let span = span!(Level::DEBUG, "write", path, ?args);

        let (rp, r) = {
            let _enter = span.enter();
            self.inner.blocking_write(path, args)?
        };

        Ok((rp, TracingWrapper::new(span, r)))
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

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        let span = span!(Level::DEBUG, "delete");

        let (rp, r) = {
            let _enter = span.enter();
            self.inner.blocking_delete()?
        };

        Ok((rp, TracingWrapper::new(span, r)))
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let span = span!(Level::DEBUG, "list", path, ?args);

        let (rp, r) = {
            let _enter = span.enter();
            self.inner.blocking_list(path, args)?
        };

        Ok((rp, TracingWrapper::new(span, r)))
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
    async fn read(&mut self) -> Result<Buffer> {
        let _enter = self.span.enter();

        self.inner.read().await
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for TracingWrapper<R> {
    fn read(&mut self) -> Result<Buffer> {
        let _enter = self.span.enter();

        self.inner.read()
    }
}

impl<R: oio::Write> oio::Write for TracingWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let _enter = self.span.enter();

        self.inner.write(bs).await
    }

    async fn abort(&mut self) -> Result<()> {
        let _enter = self.span.enter();

        self.inner.abort().await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let _enter = self.span.enter();

        self.inner.close().await
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for TracingWrapper<R> {
    fn write(&mut self, bs: Buffer) -> Result<()> {
        let _enter = self.span.enter();

        self.inner.write(bs)
    }

    fn close(&mut self) -> Result<Metadata> {
        let _enter = self.span.enter();

        self.inner.close()
    }
}

impl<R: oio::List> oio::List for TracingWrapper<R> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let _enter = self.span.enter();

        self.inner.next().await
    }
}

impl<R: oio::BlockingList> oio::BlockingList for TracingWrapper<R> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        let _enter = self.span.enter();

        self.inner.next()
    }
}

impl<R: oio::Delete> oio::Delete for TracingWrapper<R> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        let _enter = self.span.enter();

        self.inner.delete(path, args)
    }

    async fn flush(&mut self) -> Result<usize> {
        let _enter = self.span.enter();

        self.inner.flush().await
    }
}

impl<R: oio::BlockingDelete> oio::BlockingDelete for TracingWrapper<R> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        let _enter = self.span.enter();

        self.inner.delete(path, args)
    }

    fn flush(&mut self) -> Result<usize> {
        let _enter = self.span.enter();

        self.inner.flush()
    }
}
