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
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use super::util::set_accessor_for_object_iterator;
use super::util::set_accessor_for_object_steamer;
use crate::ops::*;
use crate::*;

/// ConcurrentLimitLayer will add concurrent limit for OpenDAL.
///
/// # Examples
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::ConcurrentLimitLayer;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::from_env(Scheme::Fs)
///     .expect("must init")
///     .layer(ConcurrentLimitLayer::new(1024));
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

impl Layer for ConcurrentLimitLayer {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(ConcurrentLimitAccessor {
            inner,
            semaphore: Arc::new(Semaphore::new(self.permits)),
        })
    }
}

#[derive(Debug, Clone)]
struct ConcurrentLimitAccessor {
    inner: Arc<dyn Accessor>,
    semaphore: Arc<Semaphore>,
}

#[async_trait]
impl Accessor for ConcurrentLimitAccessor {
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        Some(self.inner.clone())
    }

    fn metadata(&self) -> AccessorMetadata {
        self.inner.metadata()
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.create(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, BytesReader)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner.read(path, args).await.map(|(rp, r)| {
            (
                rp,
                Box::new(ConcurrentLimitReader::new(r, permit)) as BytesReader,
            )
        })
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.write(path, args, r).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<()> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .list(path, args)
            .await
            .map(|s| Box::new(ConcurrentLimitStreamer::new(s, permit)) as ObjectStreamer)
            .map(|s| set_accessor_for_object_steamer(s, self.clone()))
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<PresignedRequest> {
        self.inner.presign(path, args)
    }

    async fn create_multipart(&self, path: &str, args: OpCreateMultipart) -> Result<String> {
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
        r: BytesReader,
    ) -> Result<ObjectPart> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.write_multipart(path, args, r).await
    }

    async fn complete_multipart(&self, path: &str, args: OpCompleteMultipart) -> Result<()> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.complete_multipart(path, args).await
    }

    async fn abort_multipart(&self, path: &str, args: OpAbortMultipart) -> Result<()> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.abort_multipart(path, args).await
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<()> {
        let _permit = self
            .semaphore
            .try_acquire()
            .expect("semaphore must be valid");

        self.inner.blocking_create(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<BlockingBytesReader> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .expect("semaphore must be valid");

        self.inner
            .blocking_read(path, args)
            .map(|r| Box::new(BlockingConcurrentLimitReader::new(r, permit)) as BlockingBytesReader)
    }

    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<u64> {
        let _permit = self
            .semaphore
            .try_acquire()
            .expect("semaphore must be valid");

        self.inner.blocking_write(path, args, r)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        let _permit = self
            .semaphore
            .try_acquire()
            .expect("semaphore must be valid");

        self.inner.blocking_stat(path, args)
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<()> {
        let _permit = self
            .semaphore
            .try_acquire()
            .expect("semaphore must be valid");

        self.inner.blocking_delete(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<ObjectIterator> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .expect("semaphore must be valid");

        self.inner
            .blocking_list(path, args)
            .map(|it| Box::new(ConcurrentLimitInterator::new(it, permit)) as ObjectIterator)
            .map(|s| set_accessor_for_object_iterator(s, self.clone()))
    }
}

struct ConcurrentLimitReader {
    inner: BytesReader,

    // Hold on this permit until this reader has been dropped.
    _permit: OwnedSemaphorePermit,
}

impl ConcurrentLimitReader {
    fn new(inner: BytesReader, permit: OwnedSemaphorePermit) -> Self {
        Self {
            inner,
            _permit: permit,
        }
    }
}

impl AsyncRead for ConcurrentLimitReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut (*self.inner)).poll_read(cx, buf)
    }
}

struct BlockingConcurrentLimitReader {
    inner: BlockingBytesReader,

    // Hold on this permit until this reader has been dropped.
    _permit: OwnedSemaphorePermit,
}

impl BlockingConcurrentLimitReader {
    fn new(inner: BlockingBytesReader, permit: OwnedSemaphorePermit) -> Self {
        Self {
            inner,
            _permit: permit,
        }
    }
}

impl Read for BlockingConcurrentLimitReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

struct ConcurrentLimitStreamer {
    inner: ObjectStreamer,

    // Hold on this permit until this streamer has been dropped.
    _permit: OwnedSemaphorePermit,
}

impl ConcurrentLimitStreamer {
    fn new(inner: ObjectStreamer, permit: OwnedSemaphorePermit) -> Self {
        Self {
            inner,
            _permit: permit,
        }
    }
}

impl futures::Stream for ConcurrentLimitStreamer {
    type Item = Result<ObjectEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut (*self.inner)).poll_next(cx)
    }
}

struct ConcurrentLimitInterator {
    inner: ObjectIterator,

    // Hold on this permit until this iterator has been dropped.
    _permit: OwnedSemaphorePermit,
}

impl ConcurrentLimitInterator {
    fn new(inner: ObjectIterator, permit: OwnedSemaphorePermit) -> Self {
        Self {
            inner,
            _permit: permit,
        }
    }
}

impl Iterator for ConcurrentLimitInterator {
    type Item = Result<ObjectEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
