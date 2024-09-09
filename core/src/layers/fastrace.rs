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
use std::future::Future;
use std::sync::Arc;

use fastrace::prelude::*;
use futures::FutureExt;

use crate::raw::*;
use crate::*;

/// Add [fastrace](https://docs.rs/fastrace/) for every operation.
///
/// # Examples
///
/// ## Basic Setup
///
/// ```no_run
/// # use opendal::layers::FastraceLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opendal::Result;
///
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(FastraceLayer)
///     .finish();
/// Ok(())
/// # }
/// ```
///
/// ## Real usage
///
/// ```no_run
/// # use anyhow::Result;
/// # use fastrace::prelude::*;
/// # use opendal::layers::FastraceLayer;
/// # use opendal::services;
/// # use opendal::Operator;
///
/// # fn main() -> Result<()> {
/// let reporter = fastrace_jaeger::JaegerReporter::new("127.0.0.1:6831".parse()?, "opendal")?;
/// fastrace::set_reporter(reporter, fastrace::collector::Config::default());
///
/// {
///     let root = Span::root("op", SpanContext::random());
///     let runtime = tokio::runtime::Runtime::new()?;
///     runtime.block_on(
///         async {
///             let _ = dotenvy::dotenv();
///             let op = Operator::new(services::Memory::default())?
///                 .layer(FastraceLayer)
///                 .finish();
///             op.write("test", "0".repeat(16 * 1024 * 1024).into_bytes()).await?;
///             op.stat("test").await?;
///             op.read("test").await?;
///         }
///         .in_span(Span::enter_with_parent("test", &root)),
///     )?;
/// }
///
/// fastrace::flush();
///
/// Ok(())
/// # }
/// ```
///
/// # Output
///
/// OpenDAL is using [`fastrace`](https://docs.rs/fastrace/latest/fastrace/) for tracing internally.
///
/// To enable fastrace output, please init one of the reporter that `fastrace` supports.
///
/// For example:
///
/// ```no_run
/// # use anyhow::Result;
///
/// # fn main() -> Result<()> {
/// let reporter = fastrace_jaeger::JaegerReporter::new("127.0.0.1:6831".parse()?, "opendal")?;
/// fastrace::set_reporter(reporter, fastrace::collector::Config::default());
/// Ok(())
/// # }
/// ```
///
/// For real-world usage, please take a look at [`fastrace-datadog`](https://crates.io/crates/fastrace-datadog) or [`fastrace-jaeger`](https://crates.io/crates/fastrace-jaeger) .
pub struct FastraceLayer;

impl<A: Access> Layer<A> for FastraceLayer {
    type LayeredAccess = FastraceAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        FastraceAccessor { inner }
    }
}

#[derive(Debug)]
pub struct FastraceAccessor<A> {
    inner: A,
}

impl<A: Access> LayeredAccess for FastraceAccessor<A> {
    type Inner = A;
    type Reader = FastraceWrapper<A::Reader>;
    type BlockingReader = FastraceWrapper<A::BlockingReader>;
    type Writer = FastraceWrapper<A::Writer>;
    type BlockingWriter = FastraceWrapper<A::BlockingWriter>;
    type Lister = FastraceWrapper<A::Lister>;
    type BlockingLister = FastraceWrapper<A::BlockingLister>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    #[trace]
    fn metadata(&self) -> Arc<AccessorInfo> {
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
                        FastraceWrapper::new(Span::enter_with_local_parent("ReadOperation"), r),
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
                        FastraceWrapper::new(Span::enter_with_local_parent("WriteOperation"), r),
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
                        FastraceWrapper::new(Span::enter_with_local_parent("ListOperation"), s),
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
                FastraceWrapper::new(Span::enter_with_local_parent("ReadOperation"), r),
            )
        })
    }

    #[trace]
    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args).map(|(rp, r)| {
            (
                rp,
                FastraceWrapper::new(Span::enter_with_local_parent("WriteOperation"), r),
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
                FastraceWrapper::new(Span::enter_with_local_parent("PageOperation"), it),
            )
        })
    }
}

pub struct FastraceWrapper<R> {
    span: Span,
    inner: R,
}

impl<R> FastraceWrapper<R> {
    fn new(span: Span, inner: R) -> Self {
        Self { span, inner }
    }
}

impl<R: oio::Read> oio::Read for FastraceWrapper<R> {
    #[trace(enter_on_poll = true)]
    async fn read(&mut self) -> Result<Buffer> {
        self.inner.read().await
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for FastraceWrapper<R> {
    fn read(&mut self) -> Result<Buffer> {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(Operation::BlockingReaderRead.into_static());
        self.inner.read()
    }
}

impl<R: oio::Write> oio::Write for FastraceWrapper<R> {
    fn write(&mut self, bs: Buffer) -> impl Future<Output = Result<()>> + MaybeSend {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(Operation::WriterWrite.into_static());
        self.inner.write(bs)
    }

    fn abort(&mut self) -> impl Future<Output = Result<()>> + MaybeSend {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(Operation::WriterAbort.into_static());
        self.inner.abort()
    }

    fn close(&mut self) -> impl Future<Output = Result<()>> + MaybeSend {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(Operation::WriterClose.into_static());
        self.inner.close()
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for FastraceWrapper<R> {
    fn write(&mut self, bs: Buffer) -> Result<()> {
        let _g = self.span.set_local_parent();
        let _span =
            LocalSpan::enter_with_local_parent(Operation::BlockingWriterWrite.into_static());
        self.inner.write(bs)
    }

    fn close(&mut self) -> Result<()> {
        let _g = self.span.set_local_parent();
        let _span =
            LocalSpan::enter_with_local_parent(Operation::BlockingWriterClose.into_static());
        self.inner.close()
    }
}

impl<R: oio::List> oio::List for FastraceWrapper<R> {
    #[trace(enter_on_poll = true)]
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.inner.next().await
    }
}

impl<R: oio::BlockingList> oio::BlockingList for FastraceWrapper<R> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        let _g = self.span.set_local_parent();
        let _span = LocalSpan::enter_with_local_parent(Operation::BlockingListerNext.into_static());
        self.inner.next()
    }
}
