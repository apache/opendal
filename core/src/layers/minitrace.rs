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

use crate::raw::oio::ListOperation;
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
/// use minitrace::collector::Config;
/// use minitrace::prelude::*;
/// use opendal::layers::MinitraceLayer;
/// use opendal::services;
/// use opendal::Operator;
///
/// fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
///     let reporter =
///         minitrace_jaeger::JaegerReporter::new("127.0.0.1:6831".parse().unwrap(), "opendal")
///             .unwrap();
///     minitrace::set_reporter(reporter, Config::default());
///
///     {
///         let root = Span::root("op", SpanContext::random());
///         let runtime = tokio::runtime::Runtime::new()?;
///         runtime.block_on(
///             async {
///                 let _ = dotenvy::dotenv();
///                 let op = Operator::new(services::Memory::default())
///                     .expect("init operator must succeed")
///                     .layer(MinitraceLayer)
///                     .finish();
///                 op.write("test", "0".repeat(16 * 1024 * 1024).into_bytes())
///                     .await
///                     .expect("must succeed");
///                 op.stat("test").await.expect("must succeed");
///                 op.read("test").await.expect("must succeed");
///             }
///             .in_span(Span::enter_with_parent("test", &root)),
///         );
///     }
///
///     minitrace::flush();
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
/// ```no_run
/// extern crate minitrace_jaeger;
///
/// use minitrace::collector::Config;
///
/// let reporter =
///     minitrace_jaeger::JaegerReporter::new("127.0.0.1:6831".parse().unwrap(), "opendal")
///         .unwrap();
/// minitrace::set_reporter(reporter, Config::default());
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<A: Accessor> LayeredAccessor for MinitraceAccessor<A> {
    type Inner = A;
    type Reader = MinitraceWrapper<A::Reader>;
    type BlockingReader = MinitraceWrapper<A::BlockingReader>;
    type Writer = MinitraceWrapper<A::Writer>;
    type BlockingWriter = MinitraceWrapper<A::BlockingWriter>;
    type Lister = MinitraceWrapper<A::Lister>;
    type BlockingLister = MinitraceWrapper<A::BlockingLister>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    #[trace]
    fn metadata(&self) -> AccessorInfo {
        self.inner.info()
    }

    #[trace(name = "create", enter_on_poll = true)]
    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner.create_dir(path, args).await
    }

    #[trace(enter_on_poll = true)]
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner
            .read(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        MinitraceWrapper::new(Span::enter_with_local_parent("ReadOperation"), r),
                    )
                })
            })
            .await
    }

    #[trace(enter_on_poll = true)]
    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner
            .write(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        MinitraceWrapper::new(Span::enter_with_local_parent("WriteOperation"), r),
                    )
                })
            })
            .await
    }

    #[trace(enter_on_poll = true)]
    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner().copy(from, to, args).await
    }

    #[trace(enter_on_poll = true)]
    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner().rename(from, to, args).await
    }

    #[trace(enter_on_poll = true)]
    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(path, args).await
    }

    #[trace(enter_on_poll = true)]
    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.delete(path, args).await
    }

    #[trace(enter_on_poll = true)]
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner
            .list(path, args)
            .map(|v| {
                v.map(|(rp, s)| {
                    (
                        rp,
                        MinitraceWrapper::new(Span::enter_with_local_parent("ListOperation"), s),
                    )
                })
            })
            .await
    }

    #[trace(enter_on_poll = true)]
    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args).await
    }

    #[trace(enter_on_poll = true)]
    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.inner.batch(args).await
    }

    #[trace]
    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner.blocking_create_dir(path, args)
    }

    #[trace]
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args).map(|(rp, r)| {
            (
                rp,
                MinitraceWrapper::new(Span::enter_with_local_parent("ReadOperation"), r),
            )
        })
    }

    #[trace]
    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args).map(|(rp, r)| {
            (
                rp,
                MinitraceWrapper::new(Span::enter_with_local_parent("WriteOperation"), r),
            )
        })
    }

    #[trace]
    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner().blocking_copy(from, to, args)
    }

    #[trace]
    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner().blocking_rename(from, to, args)
    }

    #[trace]
    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.blocking_stat(path, args)
    }

    #[trace]
    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.blocking_delete(path, args)
    }

    #[trace]
    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.inner.blocking_list(path, args).map(|(rp, it)| {
            (
                rp,
                MinitraceWrapper::new(Span::enter_with_local_parent("PageOperation"), it),
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
    #[trace(enter_on_poll = true)]
    async fn read(&mut self, size: usize) -> Result<Bytes> {
        self.inner.read(size).await
    }

    #[trace(enter_on_poll = true)]
    async fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        self.inner.seek(pos).await
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for MinitraceWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(ReadOperation::BlockingRead.into_static());
        self.inner.read(buf)
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(ReadOperation::BlockingSeek.into_static());
        self.inner.seek(pos)
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(ReadOperation::BlockingNext.into_static());
        self.inner.next()
    }
}

impl<R: oio::Write> oio::Write for MinitraceWrapper<R> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(WriteOperation::Write.into_static());
        self.inner.poll_write(cx, bs)
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(WriteOperation::Abort.into_static());
        self.inner.poll_abort(cx)
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(WriteOperation::Close.into_static());
        self.inner.poll_close(cx)
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for MinitraceWrapper<R> {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(WriteOperation::BlockingWrite.into_static());
        self.inner.write(bs)
    }

    fn close(&mut self) -> Result<()> {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(WriteOperation::BlockingClose.into_static());
        self.inner.close()
    }
}

impl<R: oio::List> oio::List for MinitraceWrapper<R> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<oio::Entry>>> {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(ListOperation::Next.into_static());
        self.inner.poll_next(cx)
    }
}

impl<R: oio::BlockingList> oio::BlockingList for MinitraceWrapper<R> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(ListOperation::BlockingNext.into_static());
        self.inner.next()
    }
}
