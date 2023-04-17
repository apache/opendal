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

use async_trait::async_trait;
use bytes::Bytes;
use opentelemetry::global;
use opentelemetry::global::BoxedSpan;
use opentelemetry::trace::FutureExt;
use opentelemetry::trace::Span;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::trace::Tracer;
use opentelemetry::Context;

use crate::ops::*;
use crate::raw::*;
use crate::*;
use opentelemetry::KeyValue;

pub struct OtelTraceLayer;

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
        let mut span = tracer.start("metadata");
        let ret = self.inner.info();
        span.end();
        ret
    }

    async fn create_dir(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let tracer = global::tracer("opendal");
        let span = tracer.start("create");
        span.set_attribute(KeyValue::new("path", path));
        span.set_attribute(KeyValue::new("args", args));
        let cx = Context::current_with_span(span);
        self.inner.create(path, args).with_context(cx).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        todo!()
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        todo!()
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let tracer = global::tracer("opendal");
        let span = tracer.start("copy");
        span.set_attribute(KeyValue::new("from", from));
        span.set_attribute(KeyValue::new("to", to));
        span.set_attribute(KeyValue::new("args", args));
        let cx = Context::current_with_span(span);
        self.inner().copy(from, to, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let tracer = global::tracer("opendal");
        let span = tracer.start("rename");
        span.set_attribute(KeyValue::new("from", from));
        span.set_attribute(KeyValue::new("to", to));
        span.set_attribute(KeyValue::new("args", args));
        let cx = Context::current_with_span(span);
        self.inner().rename(from, to, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let tracer = global::tracer("opendal");
        let span = tracer.start("stat");
        span.set_attribute("path", path);
        span.set_attribute("args", args);
        let cx = Context::current_with_span(span);
        self.inner().stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let tracer = global::tracer("opendal");
        let span = tracer.start("delete");
        span.set_attribute(KeyValue::new("path", path));
        span.set_attribute(KeyValue::new("args", args));
        let cx = Context::current_with_span(span);
        self.inner().delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        todo!()
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        todo!()
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let tracer = global::tracer("opendal");
        let span = tracer.start("batch");
        span.set_attribute(KeyValue::new("args", args));
        let cx = Context::current_with_span(span);
        self.inner().batch(args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let tracer = global::tracer("opendal");
        let span = tracer.start("presign");
        span.set_attribute(KeyValue::new("path", path));
        span.set_attribute(KeyValue::new("args", args));
        let cx = Context::current_with_span(span);
        self.inner().presign(path, args).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("blocking_create_dir");
        span.set_attribute(KeyValue::new("path", path));
        span.set_attribute(KeyValue::new("args", args));
        let ret = self.inner().blocking_create(path, args);
        span.end();
        ret
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        todo!()
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        todo!()
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("blocking_copy");
        span.set_attribute(KeyValue::new("from", from));
        span.set_attribute(KeyValue::new("to", to));
        span.set_attribute(KeyValue::new("args", args));
        let ret = self.inner().blocking_copy(from, to, args);
        span.end();
        ret
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("blocking_rename");
        span.set_attribute(KeyValue::new("from", from));
        span.set_attribute(KeyValue::new("to", to));
        span.set_attribute(KeyValue::new("args", args));
        let ret = self.inner().blocking_rename(from, to, args);
        span.end();
        ret
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("blocking_rename");
        span.set_attribute(KeyValue::new("path", path));
        span.set_attribute(KeyValue::new("args", args));
        let ret = self.inner().blocking_stat(path, args);
        span.end();
        ret
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let tracer = global::tracer("opendal");
        let mut span = tracer.start("blocking_delete");
        span.set_attribute(KeyValue::new("path", path));
        span.set_attribute(KeyValue::new("args", args));
        let ret = self.inner().blocking_delete(path, args);
        span.end();
        ret
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        todo!()
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        todo!()
    }
}

pub struct OtelTraceWrapper<R> {
    span: BoxedSpan,
    inner: R,
}

impl<R> OtelTraceWrapper<R> {
    fn new(span: BoxedSpan, inner: R) -> Self {
        Self { span, inner }
    }
}

impl<R: oio::Read> oio::Read for OtelTraceWrapper<R> {
    fn poll_read(
        &mut self,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<Result<usize>> {
        todo!()
    }

    fn poll_seek(
        &mut self,
        cx: &mut std::task::Context<'_>,
        pos: std::io::SeekFrom,
    ) -> std::task::Poll<Result<u64>> {
        todo!()
    }

    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Bytes>>> {
        todo!()
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for OtelTraceWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        todo!()
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64> {
        todo!()
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        todo!()
    }
}

#[async_trait]
impl<R: oio::Write> oio::Write for OtelTraceWrapper<R> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        todo!()
    }

    async fn append(&mut self, bs: Bytes) -> Result<()> {
        todo!()
    }

    async fn abort(&mut self) -> Result<()> {
        todo!()
    }

    async fn close(&mut self) -> Result<()> {
        todo!()
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for OtelTraceWrapper<R> {
    fn write(&mut self, bs: Bytes) -> Result<()> {
        todo!()
    }

    fn append(&mut self, bs: Bytes) -> Result<()> {
        todo!()
    }

    fn close(&mut self) -> Result<()> {
        todo!()
    }
}

#[async_trait]
impl<R: oio::Page> oio::Page for OtelTraceWrapper<R> {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        todo!()
    }
}

impl<R: oio::BlockingPage> oio::BlockingPage for OtelTraceWrapper<R> {
    fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        todo!()
    }
}
