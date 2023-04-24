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
use minitrace::prelude::*;

use crate::ops::*;
use crate::raw::oio::PageOperation;
use crate::raw::oio::ReadOperation;
use crate::raw::oio::WriteOperation;
use crate::raw::*;
use crate::*;

/// Add [minitrace](https://docs.rs/minitrace/) for every operations.
///
/// # Examples
///
/// ## Basic Setup
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::MinitraceLayer;
/// use opendal::services;
/// use opendal::Operator;
///
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(MinitraceLayer)
///     .finish();
/// ```
///
/// ## Real usage
///
/// ```no_run
/// use std::error::Error;
///
/// use anyhow::Result;
/// use futures::executor::block_on;
/// use opendal::layers::MinitraceLayer;
/// use opendal::services;
/// use opendal::Operator;
///
/// fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
///     let collector = {
///         let (span, collector) = minitrace::Span::root("op");
///         let _g = span.set_local_parent();
///         let runtime = tokio::runtime::Runtime::new()?;
///
///         runtime.block_on(async {
///             let _ = dotenvy::dotenv();
///             let op = Operator::from_env::<services::Memory>()
///                 .expect("init operator must succeed")
///                 .layer(MinitraceLayer)
///                 .finish();
///
///             op.write("test", "0".repeat(16 * 1024 * 1024).into_bytes())
///                 .await
///                 .expect("must succeed");
///             op.stat("test").await.expect("must succeed");
///             op.read("test").await.expect("must succeed");
///         });
///         collector
///     };
///
///     let spans = block_on(collector.collect());
///
///     let bytes =
///         minitrace_jaeger::encode("opendal".to_owned(), rand::random(), 0, 0, &spans).unwrap();
///     minitrace_jaeger::report_blocking("127.0.0.1:6831".parse().unwrap(), &bytes)
///         .expect("report error");
///
///     Ok(())
/// }
/// ```
///
/// # Output
///
/// OpenDAL is using [`minitrace`](https://docs.rs/minitrace/latest/minitrace/) for tracing internally.
///
/// To enable minitrace output, please init one of the reporter that `minitrace` supports.
///
/// For example:
///
/// ```ignore
/// extern crate minitrace_jaeger;
///
/// let spans = block_on(collector.collect());
///
/// let bytes =
///     minitrace_jaeger::encode("opendal".to_owned(), rand::random(), 0, 0, &spans).unwrap();
/// minitrace_jaeger::report_blocking("127.0.0.1:6831".parse().unwrap(), &bytes).expect("report error");
/// ```
///
/// For real-world usage, please take a look at [`minitrace-datadog`](https://crates.io/crates/minitrace-datadog) or [`minitrace-jaeger`](https://crates.io/crates/minitrace-jaeger) .
pub struct MinitraceLayer;

impl<A: Accessor> Layer<A> for MinitraceLayer {
    type LayeredAccessor = MinitraceAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        MinitraceAccessor { inner }
    }
}

#[derive(Debug)]
pub struct MinitraceAccessor<A> {
    inner: A,
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for MinitraceAccessor<A> {
    type Inner = A;
    type Reader = MinitraceWrapper<A::Reader>;
    type BlockingReader = MinitraceWrapper<A::BlockingReader>;
    type Writer = MinitraceWrapper<A::Writer>;
    type BlockingWriter = MinitraceWrapper<A::BlockingWriter>;
    type Pager = MinitraceWrapper<A::Pager>;
    type BlockingPager = MinitraceWrapper<A::BlockingPager>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    #[trace("metadata")]
    fn metadata(&self) -> AccessorInfo {
        self.inner.info()
    }

    #[trace("create", enter_on_poll = true)]
    async fn create_dir(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner.create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let span = Span::enter_with_local_parent("read");
        self.inner
            .read(path, args)
            .map(|v| v.map(|(rp, r)| (rp, MinitraceWrapper::new(span, r))))
            .await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let span = Span::enter_with_local_parent("write");
        self.inner
            .write(path, args)
            .map(|v| v.map(|(rp, r)| (rp, MinitraceWrapper::new(span, r))))
            .await
    }

    #[trace("copy", enter_on_poll = true)]
    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner().copy(from, to, args).await
    }

    #[trace("rename", enter_on_poll = true)]
    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner().rename(from, to, args).await
    }

    #[trace("stat", enter_on_poll = true)]
    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(path, args).await
    }

    #[trace("delete", enter_on_poll = true)]
    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let span = Span::enter_with_local_parent("list");
        self.inner
            .list(path, args)
            .map(|v| v.map(|(rp, s)| (rp, MinitraceWrapper::new(span, s))))
            .await
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        let span = Span::enter_with_local_parent("scan");
        self.inner
            .scan(path, args)
            .map(|v| v.map(|(rp, s)| (rp, MinitraceWrapper::new(span, s))))
            .await
    }

    #[trace("presign", enter_on_poll = true)]
    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args).await
    }

    #[trace("batch", enter_on_poll = true)]
    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.inner.batch(args).await
    }

    #[trace("blocking_create")]
    fn blocking_create_dir(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner.blocking_create_dir(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let span = Span::enter_with_local_parent("blocking_read");
        self.inner.blocking_read(path, args).map(|(rp, r)| {
            (
                rp,
                MinitraceWrapper::new(Span::enter_with_parent("ReadOperation", &span), r),
            )
        })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let span = Span::enter_with_local_parent("blocking_write");
        self.inner.blocking_write(path, args).map(|(rp, r)| {
            (
                rp,
                MinitraceWrapper::new(Span::enter_with_parent("WriteOperation", &span), r),
            )
        })
    }

    #[trace("blocking_copy")]
    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner().blocking_copy(from, to, args)
    }

    #[trace("blocking_rename")]
    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner().blocking_rename(from, to, args)
    }

    #[trace("blocking_stat")]
    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.blocking_stat(path, args)
    }

    #[trace("blocking_delete")]
    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.blocking_delete(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        let span = Span::enter_with_local_parent("blocking_list");
        self.inner.blocking_list(path, args).map(|(rp, it)| {
            (
                rp,
                MinitraceWrapper::new(Span::enter_with_parent("PageOperation", &span), it),
            )
        })
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        let span = Span::enter_with_local_parent("blocking_scan");
        self.inner.blocking_scan(path, args).map(|(rp, it)| {
            (
                rp,
                MinitraceWrapper::new(Span::enter_with_parent("PageOperation", &span), it),
            )
        })
    }
}

pub struct MinitraceWrapper<R> {
    span: Span,
    inner: R,
}

impl<R> MinitraceWrapper<R> {
    fn new(span: Span, inner: R) -> Self {
        Self { span, inner }
    }
}

impl<R: oio::Read> oio::Read for MinitraceWrapper<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        let _span = Span::enter_with_parent(ReadOperation::Read.into_static(), &self.span);
        self.inner.poll_read(cx, buf)
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
        let _span = Span::enter_with_parent(ReadOperation::Seek.into_static(), &self.span);
        self.inner.poll_seek(cx, pos)
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let _span = Span::enter_with_parent(ReadOperation::Next.into_static(), &self.span);
        self.inner.poll_next(cx)
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for MinitraceWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let _span = Span::enter_with_parent(ReadOperation::BlockingRead.into_static(), &self.span);
        self.inner.read(buf)
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        let _span = Span::enter_with_parent(ReadOperation::BlockingSeek.into_static(), &self.span);
        self.inner.seek(pos)
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        let _span = Span::enter_with_parent(ReadOperation::BlockingNext.into_static(), &self.span);
        self.inner.next()
    }
}

#[async_trait]
impl<R: oio::Write> oio::Write for MinitraceWrapper<R> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.inner
            .write(bs)
            .in_span(Span::enter_with_parent(
                WriteOperation::Write.into_static(),
                &self.span,
            ))
            .await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner
            .abort()
            .in_span(Span::enter_with_parent(
                WriteOperation::Abort.into_static(),
                &self.span,
            ))
            .await
    }

    async fn close(&mut self) -> Result<()> {
        self.inner
            .close()
            .in_span(Span::enter_with_parent(
                WriteOperation::Close.into_static(),
                &self.span,
            ))
            .await
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for MinitraceWrapper<R> {
    fn write(&mut self, bs: Bytes) -> Result<()> {
        let _span =
            Span::enter_with_parent(WriteOperation::BlockingWrite.into_static(), &self.span);
        self.inner.write(bs)
    }

    fn close(&mut self) -> Result<()> {
        let _span =
            Span::enter_with_parent(WriteOperation::BlockingClose.into_static(), &self.span);
        self.inner.close()
    }
}

#[async_trait]
impl<R: oio::Page> oio::Page for MinitraceWrapper<R> {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        self.inner
            .next()
            .in_span(Span::enter_with_parent(
                PageOperation::Next.into_static(),
                &self.span,
            ))
            .await
    }
}

impl<R: oio::BlockingPage> oio::BlockingPage for MinitraceWrapper<R> {
    fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        let _span = Span::enter_with_parent(PageOperation::BlockingNext.into_static(), &self.span);
        self.inner.next()
    }
}
