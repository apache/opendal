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
    #[tracing::instrument]
    fn metadata(&self) -> AccessorMetadata {
        self.inner.metadata()
    }

    #[tracing::instrument]
    async fn create(&self, args: &OpCreate) -> Result<()> {
        self.inner.create(args).await
    }

    #[tracing::instrument]
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        self.inner
            .read(args)
            .await
            .map(|r| Box::new(TracingReader::new(Span::current(), r)) as BytesReader)
    }

    #[tracing::instrument(skip(r))]
    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let r = Box::new(TracingReader::new(Span::current(), r));
        self.inner.write(args, r).await
    }

    #[tracing::instrument]
    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        self.inner.stat(args).await
    }

    #[tracing::instrument]
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        self.inner.delete(args).await
    }

    #[tracing::instrument]
    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        self.inner
            .list(args)
            .await
            .map(|s| Box::new(TracingStreamer::new(Span::current(), s)) as DirStreamer)
    }

    #[tracing::instrument]
    fn presign(&self, args: &OpPresign) -> Result<PresignedRequest> {
        self.inner.presign(args)
    }

    #[tracing::instrument]
    async fn create_multipart(&self, args: &OpCreateMultipart) -> Result<String> {
        self.inner.create_multipart(args).await
    }

    #[tracing::instrument(skip(r))]
    async fn write_multipart(&self, args: &OpWriteMultipart, r: BytesReader) -> Result<ObjectPart> {
        let r = Box::new(TracingReader::new(Span::current(), r));
        self.inner.write_multipart(args, r).await
    }

    #[tracing::instrument]
    async fn complete_multipart(&self, args: &OpCompleteMultipart) -> Result<()> {
        self.inner.complete_multipart(args).await
    }

    #[tracing::instrument]
    async fn abort_multipart(&self, args: &OpAbortMultipart) -> Result<()> {
        self.inner.abort_multipart(args).await
    }

    #[tracing::instrument]
    fn blocking_create(&self, args: &OpCreate) -> Result<()> {
        self.inner.blocking_create(args)
    }

    #[tracing::instrument]
    fn blocking_read(&self, args: &OpRead) -> Result<BlockingBytesReader> {
        self.inner.blocking_read(args).map(|r| {
            Box::new(BlockingTracingReader::new(Span::current(), r)) as BlockingBytesReader
        })
    }

    #[tracing::instrument(skip(r))]
    fn blocking_write(&self, args: &OpWrite, r: BlockingBytesReader) -> Result<u64> {
        self.inner.blocking_write(args, r)
    }

    #[tracing::instrument]
    fn blocking_stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        self.inner.blocking_stat(args)
    }

    #[tracing::instrument]
    fn blocking_delete(&self, args: &OpDelete) -> Result<()> {
        self.inner.blocking_delete(args)
    }

    #[tracing::instrument]
    fn blocking_list(&self, args: &OpList) -> Result<DirIterator> {
        self.inner
            .blocking_list(args)
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
    #[tracing::instrument(parent=&self.span, skip(self))]
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
    #[tracing::instrument(parent = & self.span, skip(self))]
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

    #[tracing::instrument(parent=&self.span, skip(self))]
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

    #[tracing::instrument(parent =&self.span, skip(self))]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
