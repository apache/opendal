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
use std::io::Read;
use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use futures::AsyncRead;
use tracing::Span;

use crate::multipart::ObjectPart;
use crate::ops::OpAbortMultipart;
use crate::ops::OpCompleteMultipart;
use crate::ops::OpCreate;
use crate::ops::OpCreateMultipart;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpPresign;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::OpWriteMultipart;
use crate::ops::PresignedRequest;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BlockingBytesReader;
use crate::BytesReader;
use crate::DirEntry;
use crate::DirIterator;
use crate::DirStreamer;
use crate::Layer;
use crate::ObjectMetadata;

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

#[derive(Debug)]
struct TracingAccessor {
    inner: Arc<dyn Accessor>,
}

#[async_trait]
impl Accessor for TracingAccessor {
    #[tracing::instrument(level = "debug")]
    fn metadata(&self) -> AccessorMetadata {
        self.inner.metadata()
    }

    #[tracing::instrument(level = "debug")]
    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        self.inner.create(path, args).await
    }

    #[tracing::instrument(level = "debug")]
    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        self.inner
            .read(path, args)
            .await
            .map(|r| Box::new(TracingReader::new(Span::current(), r)) as BytesReader)
    }

    #[tracing::instrument(level = "debug", skip(r))]
    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        let r = Box::new(TracingReader::new(Span::current(), r));
        self.inner.write(path, args, r).await
    }

    #[tracing::instrument(level = "debug")]
    async fn stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        self.inner.stat(path, args).await
    }

    #[tracing::instrument(level = "debug")]
    async fn delete(&self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.delete(path, args).await
    }

    #[tracing::instrument(level = "debug")]
    async fn list(&self, path: &str, args: OpList) -> Result<DirStreamer> {
        self.inner
            .list(path, args)
            .await
            .map(|s| Box::new(TracingStreamer::new(Span::current(), s)) as DirStreamer)
    }

    #[tracing::instrument(level = "debug")]
    fn presign(&self, path: &str, args: OpPresign) -> Result<PresignedRequest> {
        self.inner.presign(path, args)
    }

    #[tracing::instrument(level = "debug")]
    async fn create_multipart(&self, path: &str, args: OpCreateMultipart) -> Result<String> {
        self.inner.create_multipart(path, args).await
    }

    #[tracing::instrument(level = "debug", skip(r))]
    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: BytesReader,
    ) -> Result<ObjectPart> {
        let r = Box::new(TracingReader::new(Span::current(), r));
        self.inner.write_multipart(path, args, r).await
    }

    #[tracing::instrument(level = "debug")]
    async fn complete_multipart(&self, path: &str, args: OpCompleteMultipart) -> Result<()> {
        self.inner.complete_multipart(path, args).await
    }

    #[tracing::instrument(level = "debug")]
    async fn abort_multipart(&self, path: &str, args: OpAbortMultipart) -> Result<()> {
        self.inner.abort_multipart(path, args).await
    }

    #[tracing::instrument(level = "debug")]
    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<()> {
        self.inner.blocking_create(path, args)
    }

    #[tracing::instrument(level = "debug")]
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<BlockingBytesReader> {
        self.inner.blocking_read(path, args).map(|r| {
            Box::new(BlockingTracingReader::new(Span::current(), r)) as BlockingBytesReader
        })
    }

    #[tracing::instrument(level = "debug", skip(r))]
    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<u64> {
        self.inner.blocking_write(path, args, r)
    }

    #[tracing::instrument(level = "debug")]
    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        self.inner.blocking_stat(path, args)
    }

    #[tracing::instrument(level = "debug")]
    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.blocking_delete(path, args)
    }

    #[tracing::instrument(level = "debug")]
    fn blocking_list(&self, path: &str, args: OpList) -> Result<DirIterator> {
        self.inner
            .blocking_list(path, args)
            .map(|it| Box::new(TracingInterator::new(Span::current(), it)) as DirIterator)
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
    ) -> Poll<Result<usize>> {
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
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf)
    }
}

struct TracingStreamer {
    span: Span,
    inner: DirStreamer,
}

impl TracingStreamer {
    fn new(span: Span, streamer: DirStreamer) -> Self {
        Self {
            span,
            inner: streamer,
        }
    }
}

impl futures::Stream for TracingStreamer {
    type Item = Result<DirEntry>;

    #[tracing::instrument(parent = &self.span, level = "debug", skip_all)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut (*self.inner)).poll_next(cx)
    }
}

struct TracingInterator {
    span: Span,
    inner: DirIterator,
}

impl TracingInterator {
    fn new(span: Span, inner: DirIterator) -> Self {
        Self { span, inner }
    }
}

impl Iterator for TracingInterator {
    type Item = Result<DirEntry>;

    #[tracing::instrument(parent = &self.span, level = "debug", skip_all)]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
