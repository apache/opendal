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
use minitrace::trace;

use crate::ops::*;
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
    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner.create(path, args).await
    }

    #[trace("read", enter_on_poll = true)]
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner
            .read(path, args)
            .map(|v| v.map(|(rp, r)| (rp, MinitraceWrapper::new(r))))
            .await
    }

    #[trace("write", enter_on_poll = true)]
    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner
            .write(path, args)
            .await
            .map(|(rp, r)| (rp, MinitraceWrapper::new(r)))
    }

    #[trace("stat", enter_on_poll = true)]
    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(path, args).await
    }

    #[trace("delete", enter_on_poll = true)]
    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.delete(path, args).await
    }

    #[trace("list", enter_on_poll = true)]
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.inner
            .list(path, args)
            .map(|v| v.map(|(rp, s)| (rp, MinitraceWrapper::new(s))))
            .await
    }

    #[trace("scan", enter_on_poll = true)]
    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        self.inner
            .scan(path, args)
            .map(|v| v.map(|(rp, s)| (rp, MinitraceWrapper::new(s))))
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
    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner.blocking_create(path, args)
    }

    #[trace("blocking_read")]
    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, MinitraceWrapper::new(r)))
    }

    #[trace("blocking_write")]
    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner
            .blocking_write(path, args)
            .map(|(rp, r)| (rp, MinitraceWrapper::new(r)))
    }

    #[trace("blocking_stat")]
    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.blocking_stat(path, args)
    }

    #[trace("blocking_delete")]
    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.blocking_delete(path, args)
    }

    #[trace("blocking_list")]
    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner
            .blocking_list(path, args)
            .map(|(rp, it)| (rp, MinitraceWrapper::new(it)))
    }

    #[trace("blocking_scan")]
    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        self.inner
            .blocking_scan(path, args)
            .map(|(rp, it)| (rp, MinitraceWrapper::new(it)))
    }
}

pub struct MinitraceWrapper<R> {
    inner: R,
}

impl<R> MinitraceWrapper<R> {
    fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: oio::Read> oio::Read for MinitraceWrapper<R> {
    #[trace("poll_read")]
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        self.inner.poll_read(cx, buf)
    }

    #[trace("poll_seek")]
    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
        self.inner.poll_seek(cx, pos)
    }

    #[trace("poll_next")]
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        self.inner.poll_next(cx)
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for MinitraceWrapper<R> {
    #[trace("read")]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf)
    }

    #[trace("seek")]
    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        self.inner.seek(pos)
    }

    #[trace("next")]
    fn next(&mut self) -> Option<Result<Bytes>> {
        self.inner.next()
    }
}

#[async_trait]
impl<R: oio::Write> oio::Write for MinitraceWrapper<R> {
    #[trace("write", enter_on_poll = true)]
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.inner.write(bs).await
    }

    #[trace("append", enter_on_poll = true)]
    async fn append(&mut self, bs: Bytes) -> Result<()> {
        self.inner.append(bs).await
    }

    #[trace("close", enter_on_poll = true)]
    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for MinitraceWrapper<R> {
    #[trace("write")]
    fn write(&mut self, bs: Bytes) -> Result<()> {
        self.inner.write(bs)
    }

    #[trace("append")]
    fn append(&mut self, bs: Bytes) -> Result<()> {
        self.inner.append(bs)
    }

    #[trace("close")]
    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }
}

#[async_trait]
impl<R: oio::Page> oio::Page for MinitraceWrapper<R> {
    #[trace("next", enter_on_poll = true)]
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        self.inner.next().await
    }
}

impl<R: oio::BlockingPage> oio::BlockingPage for MinitraceWrapper<R> {
    #[trace("next")]
    fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        self.inner.next()
    }
}
