// Copyright 2022 Datafuse Labs
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
use std::io::SeekFrom;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Add concurrent request limit.
///
/// # Notes
///
/// Users can control how many concurrent connections could be established
/// betweet OpenDAL and underlying storage services.
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
/// let _ = Operator::create(services::Memory::default())
///     .expect("must init")
///     .layer(ConcurrentLimitLayer::new(1024))
///     .finish();
/// ```
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

#[async_trait]
impl<A: Accessor> LayeredAccessor for ConcurrentLimitAccessor<A> {
    type Inner = A;
    type Reader = ConcurrentLimitWrapper<A::Reader>;
    type BlockingReader = ConcurrentLimitWrapper<A::BlockingReader>;
    type Pager = ConcurrentLimitWrapper<A::Pager>;
    type BlockingPager = ConcurrentLimitWrapper<A::BlockingPager>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.create(path, args).await
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

    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.write(path, args, r).await
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

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
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

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .scan(path, args)
            .await
            .map(|(rp, s)| (rp, ConcurrentLimitWrapper::new(s, permit)))
    }

    async fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> Result<RpCreateMultipart> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.create_multipart(path, args).await
    }

    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: input::Reader,
    ) -> Result<RpWriteMultipart> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.write_multipart(path, args, r).await
    }

    async fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> Result<RpCompleteMultipart> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.complete_multipart(path, args).await
    }

    async fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> Result<RpAbortMultipart> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.abort_multipart(path, args).await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.batch(args).await
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let _permit = self
            .semaphore
            .try_acquire()
            .expect("semaphore must be valid");

        self.inner.blocking_create(path, args)
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

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
        r: input::BlockingReader,
    ) -> Result<RpWrite> {
        let _permit = self
            .semaphore
            .try_acquire()
            .expect("semaphore must be valid");

        self.inner.blocking_write(path, args, r)
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

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .expect("semaphore must be valid");

        self.inner
            .blocking_list(path, args)
            .map(|(rp, it)| (rp, ConcurrentLimitWrapper::new(it, permit)))
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .expect("semaphore must be valid");

        self.inner
            .blocking_scan(path, args)
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

impl<R: output::Read> output::Read for ConcurrentLimitWrapper<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        self.inner.poll_read(cx, buf)
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<std::io::Result<u64>> {
        self.inner.poll_seek(cx, pos)
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<std::io::Result<Bytes>>> {
        self.inner.poll_next(cx)
    }
}

impl<R: output::BlockingRead> output::BlockingRead for ConcurrentLimitWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }

    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }

    fn next(&mut self) -> Option<std::io::Result<Bytes>> {
        self.inner.next()
    }
}

#[async_trait]
impl<R: output::Page> output::Page for ConcurrentLimitWrapper<R> {
    async fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        self.inner.next_page().await
    }
}

impl<R: output::BlockingPage> output::BlockingPage for ConcurrentLimitWrapper<R> {
    fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        self.inner.next_page()
    }
}
