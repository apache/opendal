// Copyright 2022 Datafuse Labs.
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
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use futures::AsyncRead;
use tracing::Span;

use super::util::set_accessor_for_object_iterator;
use super::util::set_accessor_for_object_steamer;
use crate::ops::*;
use crate::*;

/// TracingLayer will add tracing for OpenDAL.
///
/// # Examples
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::TracingLayer;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::from_env(Scheme::Fs)
///     .expect("must init")
///     .layer(TracingLayer);
/// ```
pub struct TracingLayer;

impl Layer for TracingLayer {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(TracingAccessor { inner })
    }
}

#[derive(Debug, Clone)]
struct TracingAccessor {
    inner: Arc<dyn Accessor>,
}

#[async_trait]
impl Accessor for TracingAccessor {
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        Some(self.inner.clone())
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
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, BytesReader)> {
        self.inner.read(path, args).await.map(|(rp, r)| {
            (
                rp,
                Box::new(TracingReader::new(Span::current(), r)) as BytesReader,
            )
        })
    }

    #[tracing::instrument(level = "debug", skip(self, r))]
    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite> {
        let r = Box::new(TracingReader::new(Span::current(), r));
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
    async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer> {
        self.inner
            .list(path, args)
            .await
            .map(|s| Box::new(TracingStreamer::new(Span::current(), s)) as ObjectStreamer)
            .map(|s| set_accessor_for_object_steamer(s, self.clone()))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args)
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
        r: BytesReader,
    ) -> Result<RpWriteMultipart> {
        let r = Box::new(TracingReader::new(Span::current(), r));
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
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, BlockingBytesReader)> {
        self.inner.blocking_read(path, args).map(|(rp, r)| {
            (
                rp,
                Box::new(BlockingTracingReader::new(Span::current(), r)) as BlockingBytesReader,
            )
        })
    }

    #[tracing::instrument(level = "debug", skip(self, r))]
    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<RpWrite> {
        self.inner.blocking_write(path, args, r)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        self.inner.blocking_stat(path, args)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.blocking_delete(path, args)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn blocking_list(&self, path: &str, args: OpList) -> Result<ObjectIterator> {
        self.inner
            .blocking_list(path, args)
            .map(|it| Box::new(TracingInterator::new(Span::current(), it)) as ObjectIterator)
            .map(|s| set_accessor_for_object_iterator(s, self.clone()))
    }
}

struct TracingReader {
    span: Span,
    inner: BytesReader,
}

impl TracingReader {
    fn new(span: Span, inner: BytesReader) -> Self {
        Self { span, inner }
    }
}

impl AsyncRead for TracingReader {
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
        Pin::new(&mut (*self.inner)).poll_read(cx, buf)
    }
}

struct BlockingTracingReader {
    span: Span,
    inner: BlockingBytesReader,
}

impl BlockingTracingReader {
    fn new(span: Span, inner: BlockingBytesReader) -> Self {
        Self { span, inner }
    }
}

impl Read for BlockingTracingReader {
    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        fields(size = buf.len())
        skip_all)]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

struct TracingStreamer {
    span: Span,
    inner: ObjectStreamer,
}

impl TracingStreamer {
    fn new(span: Span, streamer: ObjectStreamer) -> Self {
        Self {
            span,
            inner: streamer,
        }
    }
}

impl futures::Stream for TracingStreamer {
    type Item = Result<ObjectEntry>;

    #[tracing::instrument(parent = &self.span, level = "debug", skip_all)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut (*self.inner)).poll_next(cx)
    }
}

struct TracingInterator {
    span: Span,
    inner: ObjectIterator,
}

impl TracingInterator {
    fn new(span: Span, inner: ObjectIterator) -> Self {
        Self { span, inner }
    }
}

impl Iterator for TracingInterator {
    type Item = Result<ObjectEntry>;

    #[tracing::instrument(parent = &self.span, level = "debug", skip_all)]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
