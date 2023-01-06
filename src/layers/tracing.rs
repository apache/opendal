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
use bytes::Bytes;
use futures::AsyncRead;
use tracing::Span;

use crate::raw::*;
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
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, output::Reader)> {
        self.inner.read(path, args).await.map(|(rp, r)| {
            (
                rp,
                Box::new(TracingReader::new(Span::current(), r)) as output::Reader,
            )
        })
    }

    #[tracing::instrument(level = "debug", skip(self, r))]
    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
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
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, ObjectPager)> {
        self.inner.list(path, args).await.map(|(rp, s)| {
            (
                rp,
                Box::new(TracingPager::new(Span::current(), s)) as ObjectPager,
            )
        })
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
        r: input::Reader,
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
    fn blocking_read(
        &self,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, BlockingOutputBytesReader)> {
        self.inner.blocking_read(path, args).map(|(rp, r)| {
            (
                rp,
                Box::new(BlockingTracingReader::new(Span::current(), r))
                    as BlockingOutputBytesReader,
            )
        })
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
    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, BlockingObjectPager)> {
        self.inner.blocking_list(path, args).map(|(rp, it)| {
            (
                rp,
                Box::new(BlockingTracingPager::new(Span::current(), it)) as BlockingObjectPager,
            )
        })
    }
}

struct TracingReader<R> {
    span: Span,
    inner: R,
}

impl<R> TracingReader<R> {
    fn new(span: Span, inner: R) -> Self {
        Self { span, inner }
    }
}

impl output::Read for TracingReader<output::Reader> {
    fn inner(&mut self) -> Option<&mut output::Reader> {
        Some(&mut self.inner)
    }

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
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Bytes>>> {
        self.inner.poll_next(cx)
    }
}

impl<R: input::Read> AsyncRead for TracingReader<R> {
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

struct BlockingTracingReader<R> {
    span: Span,
    inner: R,
}

impl<R> BlockingTracingReader<R> {
    fn new(span: Span, inner: R) -> Self {
        Self { span, inner }
    }
}

impl<R: input::BlockingRead> Read for BlockingTracingReader<R> {
    #[tracing::instrument(
        parent = &self.span,
        level = "trace",
        fields(size = buf.len())
        skip_all)]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

struct TracingPager {
    span: Span,
    inner: ObjectPager,
}

impl TracingPager {
    fn new(span: Span, streamer: ObjectPager) -> Self {
        Self {
            span,
            inner: streamer,
        }
    }
}

#[async_trait]
impl ObjectPage for TracingPager {
    #[tracing::instrument(parent = &self.span, level = "debug", skip_all)]
    async fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        self.inner.next_page().await
    }
}

struct BlockingTracingPager {
    span: Span,
    inner: BlockingObjectPager,
}

impl BlockingTracingPager {
    fn new(span: Span, inner: BlockingObjectPager) -> Self {
        Self { span, inner }
    }
}

impl BlockingObjectPage for BlockingTracingPager {
    #[tracing::instrument(parent = &self.span, level = "debug", skip_all)]
    fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        self.inner.next_page()
    }
}
