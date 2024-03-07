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
use std::io::SeekFrom;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::raw::*;
use crate::*;

/// Add concurrent request limit.
///
/// # Notes
///
/// Users can control how many concurrent connections could be established
/// between OpenDAL and underlying storage services.
///
/// # Examples
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::ConcurrentLimitLayer;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(ConcurrentLimitLayer::new(1024))
///     .finish();
/// ```
#[derive(Clone)]
pub struct ConcurrentLimitLayer {
    permits: usize,
}

impl ConcurrentLimitLayer {
    /// Create a new ConcurrentLimitLayer will specify permits
    pub fn new(permits: usize) -> Self {
        Self { permits }
    }
}

impl<A: Accessor> Layer<A> for ConcurrentLimitLayer {
    type LayeredAccessor = ConcurrentLimitAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        ConcurrentLimitAccessor {
            inner,
            semaphore: Arc::new(Semaphore::new(self.permits)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConcurrentLimitAccessor<A: Accessor> {
    inner: A,
    semaphore: Arc<Semaphore>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<A: Accessor> LayeredAccessor for ConcurrentLimitAccessor<A> {
    type Inner = A;
    type Reader = ConcurrentLimitWrapper<A::Reader>;
    type BlockingReader = ConcurrentLimitWrapper<A::BlockingReader>;
    type Writer = ConcurrentLimitWrapper<A::Writer>;
    type BlockingWriter = ConcurrentLimitWrapper<A::BlockingWriter>;
    type Lister = ConcurrentLimitWrapper<A::Lister>;
    type BlockingLister = ConcurrentLimitWrapper<A::BlockingLister>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, ConcurrentLimitWrapper::new(r, permit)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .list(path, args)
            .await
            .map(|(rp, s)| (rp, ConcurrentLimitWrapper::new(s, permit)))
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.batch(args).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let _permit = self
            .semaphore
            .try_acquire()
            .expect("semaphore must be valid");

        self.inner.blocking_create_dir(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .expect("semaphore must be valid");

        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, ConcurrentLimitWrapper::new(r, permit)))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .expect("semaphore must be valid");

        self.inner
            .blocking_write(path, args)
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let _permit = self
            .semaphore
            .try_acquire()
            .expect("semaphore must be valid");

        self.inner.blocking_stat(path, args)
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let _permit = self
            .semaphore
            .try_acquire()
            .expect("semaphore must be valid");

        self.inner.blocking_delete(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .expect("semaphore must be valid");

        self.inner
            .blocking_list(path, args)
            .map(|(rp, it)| (rp, ConcurrentLimitWrapper::new(it, permit)))
    }
}

pub struct ConcurrentLimitWrapper<R> {
    inner: R,

    // Hold on this permit until this reader has been dropped.
    _permit: OwnedSemaphorePermit,
}

impl<R> ConcurrentLimitWrapper<R> {
    fn new(inner: R, permit: OwnedSemaphorePermit) -> Self {
        Self {
            inner,
            _permit: permit,
        }
    }
}

impl<R: oio::Read> oio::Read for ConcurrentLimitWrapper<R> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf).await
    }

    async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.inner.seek(pos).await
    }

    async fn next_v2(&mut self, size: usize) -> Result<Bytes> {
        self.inner.next_v2(size).await
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for ConcurrentLimitWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf)
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.inner.seek(pos)
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        self.inner.next()
    }
}

impl<R: oio::Write> oio::Write for ConcurrentLimitWrapper<R> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        self.inner.poll_write(cx, bs)
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.inner.poll_close(cx)
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.inner.poll_abort(cx)
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for ConcurrentLimitWrapper<R> {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        self.inner.write(bs)
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }
}

impl<R: oio::List> oio::List for ConcurrentLimitWrapper<R> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<oio::Entry>>> {
        self.inner.poll_next(cx)
    }
}

impl<R: oio::BlockingList> oio::BlockingList for ConcurrentLimitWrapper<R> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.inner.next()
    }
}
