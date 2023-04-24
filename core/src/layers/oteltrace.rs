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

use std::io;
use std::task;

use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use opentelemetry::global;
use opentelemetry::global::BoxedSpan;
use opentelemetry::trace::FutureExt as TraceFutureExt;
use opentelemetry::trace::Span;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::trace::Tracer;
use opentelemetry::Context;
use opentelemetry::KeyValue;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Add [opentelemetry::trace](https://docs.rs/opentelemetry/latest/opentelemetry/trace/index.html) for every operations.
///
/// Examples
///
/// ## Basic Setup
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::OtelTraceLayer;
/// use opendal::services;
/// use opendal::Operator;
///
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(OtelTraceLayer)
///     .finish();
/// ```
pub struct OtelTraceLayer;

impl<A: Accessor> Layer<A> for OtelTraceLayer {
    type LayeredAccessor = OtelTraceAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        OtelTraceAccessor { inner }
    }
}

#[derive(Debug)]
pub struct OtelTraceAccessor<A> {
    inner: A,
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for OtelTraceAccessor<A> {
    type Inner = A;
    type Reader = OtelTraceWrapper<A::Reader>;
    type BlockingReader = OtelTraceWrapper<A::BlockingReader>;
    type Writer = OtelTraceWrapper<A::Writer>;
    type BlockingWriter = OtelTraceWrapper<A::BlockingWriter>;
    type Pager = OtelTraceWrapper<A::Pager>;
    type BlockingPager = OtelTraceWrapper<A::BlockingPager>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn metadata(&self) -> AccessorInfo {
        let tracer = global::tracer("opendal");
        tracer.in_span("metadata", |_cx| self.inner.info())
    }

    async fn create_dir(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("create");
        span.set_attribute(KeyValue::new("path", path.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        let cx = Context::current_with_span(span);
        self.inner.create_dir(path, args).with_context(cx).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("read");
        span.set_attribute(KeyValue::new("path", path.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        self.inner
            .read(path, args)
            .map(|v| v.map(|(rp, r)| (rp, OtelTraceWrapper::new(span, r))))
            .await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("write");
        span.set_attribute(KeyValue::new("path", path.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        self.inner
            .write(path, args)
            .await
            .map(|(rp, r)| (rp, OtelTraceWrapper::new(span, r)))
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("copy");
        span.set_attribute(KeyValue::new("from", from.to_string()));
        span.set_attribute(KeyValue::new("to", to.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        let cx = Context::current_with_span(span);
        self.inner().copy(from, to, args).with_context(cx).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("rename");
        span.set_attribute(KeyValue::new("from", from.to_string()));
        span.set_attribute(KeyValue::new("to", to.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        let cx = Context::current_with_span(span);
        self.inner().rename(from, to, args).with_context(cx).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("stat");
        span.set_attribute(KeyValue::new("path", path.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        let cx = Context::current_with_span(span);
        self.inner().stat(path, args).with_context(cx).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("delete");
        span.set_attribute(KeyValue::new("path", path.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        let cx = Context::current_with_span(span);
        self.inner().delete(path, args).with_context(cx).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("list");
        span.set_attribute(KeyValue::new("path", path.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        self.inner
            .list(path, args)
            .map(|v| v.map(|(rp, s)| (rp, OtelTraceWrapper::new(span, s))))
            .await
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("scan");
        span.set_attribute(KeyValue::new("path", path.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        self.inner
            .scan(path, args)
            .map(|v| v.map(|(rp, s)| (rp, OtelTraceWrapper::new(span, s))))
            .await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("batch");
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        let cx = Context::current_with_span(span);
        self.inner().batch(args).with_context(cx).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("presign");
        span.set_attribute(KeyValue::new("path", path.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        let cx = Context::current_with_span(span);
        self.inner().presign(path, args).with_context(cx).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let tracer = global::tracer("opendal");
        tracer.in_span("blocking_create_dir", |cx| {
            let span = cx.span(); // let mut span = cx.();
            span.set_attribute(KeyValue::new("path", path.to_string()));
            span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
            self.inner().blocking_create_dir(path, args)
        })
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("blocking_read");
        span.set_attribute(KeyValue::new("path", path.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, OtelTraceWrapper::new(span, r)))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("blocking_write");
        span.set_attribute(KeyValue::new("path", path.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        self.inner
            .blocking_write(path, args)
            .map(|(rp, r)| (rp, OtelTraceWrapper::new(span, r)))
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let tracer = global::tracer("opendal");
        tracer.in_span("blocking_copy", |cx| {
            let span = cx.span();
            span.set_attribute(KeyValue::new("from", from.to_string()));
            span.set_attribute(KeyValue::new("to", to.to_string()));
            span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
            self.inner().blocking_copy(from, to, args)
        })
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let tracer = global::tracer("opendal");
        tracer.in_span("blocking_rename", |cx| {
            let span = cx.span();
            span.set_attribute(KeyValue::new("from", from.to_string()));
            span.set_attribute(KeyValue::new("to", to.to_string()));
            span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
            self.inner().blocking_rename(from, to, args)
        })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let tracer = global::tracer("opendal");
        tracer.in_span("blocking_stat", |cx| {
            let span = cx.span();
            span.set_attribute(KeyValue::new("path", path.to_string()));
            span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
            self.inner().blocking_stat(path, args)
        })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let tracer = global::tracer("opendal");
        tracer.in_span("blocking_delete", |cx| {
            let span = cx.span();
            span.set_attribute(KeyValue::new("path", path.to_string()));
            span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
            self.inner().blocking_delete(path, args)
        })
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("blocking_list");
        span.set_attribute(KeyValue::new("path", path.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        self.inner
            .blocking_list(path, args)
            .map(|(rp, it)| (rp, OtelTraceWrapper::new(span, it)))
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("blocking_scan");
        span.set_attribute(KeyValue::new("path", path.to_string()));
        span.set_attribute(KeyValue::new("args", format!("{:?}", args)));
        self.inner
            .blocking_scan(path, args)
            .map(|(rp, it)| (rp, OtelTraceWrapper::new(span, it)))
    }
}

pub struct OtelTraceWrapper<R> {
    _span: BoxedSpan,
    inner: R,
}

impl<R> OtelTraceWrapper<R> {
    fn new(_span: BoxedSpan, inner: R) -> Self {
        Self { _span, inner }
    }
}

impl<R: oio::Read> oio::Read for OtelTraceWrapper<R> {
    fn poll_read(
        &mut self,
        cx: &mut task::Context<'_>,
        buf: &mut [u8],
    ) -> task::Poll<Result<usize>> {
        self.inner.poll_read(cx, buf)
    }

    fn poll_seek(
        &mut self,
        cx: &mut task::Context<'_>,
        pos: io::SeekFrom,
    ) -> task::Poll<Result<u64>> {
        self.inner.poll_seek(cx, pos)
    }

    fn poll_next(&mut self, cx: &mut task::Context<'_>) -> task::Poll<Option<Result<Bytes>>> {
        self.inner.poll_next(cx)
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for OtelTraceWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf)
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64> {
        self.inner.seek(pos)
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        self.inner.next()
    }
}

#[async_trait]
impl<R: oio::Write> oio::Write for OtelTraceWrapper<R> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.inner.write(bs).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for OtelTraceWrapper<R> {
    fn write(&mut self, bs: Bytes) -> Result<()> {
        self.inner.write(bs)
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }
}

#[async_trait]
impl<R: oio::Page> oio::Page for OtelTraceWrapper<R> {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        self.inner.next().await
    }
}

impl<R: oio::BlockingPage> oio::BlockingPage for OtelTraceWrapper<R> {
    fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        self.inner.next()
    }
}
