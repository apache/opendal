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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::io;
use futures::AsyncRead;
use futures::{ready, FutureExt};

use super::policy::{CacheReadEntry, CacheReadEntryIterator};
use super::*;
use crate::raw::*;
use crate::*;

#[derive(Debug, Clone)]
pub struct CacheAccessor {
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,
    policy: Arc<dyn CachePolicy>,
}

impl CacheAccessor {
    pub fn new(
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        policy: Arc<dyn CachePolicy>,
    ) -> Self {
        CacheAccessor {
            inner,
            cache,
            policy,
        }
    }
}

#[async_trait]
impl Accessor for CacheAccessor {
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        Some(self.inner.clone())
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner.create(path, args).await
    }

    async fn read(&self, path: &str, mut args: OpRead) -> Result<(RpRead, BytesReader)> {
        // Cache read must contain valid size.
        if args.range().size().is_none() || args.range().offset().is_none() {
            let rp = self.inner.stat(path, OpStat::default()).await?;
            let total_size = rp.into_metadata().content_length();
            let bcr = BytesContentRange::from_bytes_range(total_size, args.range());
            let br = bcr.to_bytes_range().expect("bytes range must be valid");
            args = args.with_range(br)
        }

        let (offset, size) = (
            args.range().offset().expect("offset must be valid"),
            args.range().size().expect("size must be valid"),
        );

        let it = self.policy.on_read(path, offset, size).await;

        Ok((
            RpRead::new(size),
            Box::new(CacheReader::new(
                self.inner.clone(),
                self.cache.clone(),
                path,
                it,
            )) as BytesReader,
        ))
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite> {
        self.inner.write(path, args, r).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.delete(path, args).await
    }
}

struct CacheReader {
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,

    path: String,
    it: CacheReadEntryIterator,
    state: CacheState,
}

enum CacheState {
    Idle,
    Polling(BoxFuture<'static, Result<(RpRead, BytesReader)>>),
    Reading((RpRead, BytesReader)),
}

impl CacheReader {
    fn new(
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        it: CacheReadEntryIterator,
    ) -> Self {
        Self {
            inner,
            cache,
            path: path.to_string(),
            it,
            state: CacheState::Idle,
        }
    }
}

impl AsyncRead for CacheReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let cache = self.cache.clone();
        let inner = self.inner.clone();
        let path = self.path.clone();

        match &mut self.state {
            CacheState::Idle => {
                let entry = match self.it.next() {
                    Some(entry) => entry,
                    None => return Poll::Ready(Ok(0)),
                };

                let fut = Box::pin(read_cache_entry(inner, cache, path, entry));
                self.state = CacheState::Polling(fut);
                self.poll_read(cx, buf)
            }
            CacheState::Polling(fut) => {
                let r = ready!(fut.poll_unpin(cx))?;
                self.state = CacheState::Reading(r);
                self.poll_read(cx, buf)
            }
            CacheState::Reading((_, r)) => match ready!(Pin::new(r).poll_read(cx, buf)) {
                Ok(n) if n == 0 => {
                    self.state = CacheState::Idle;
                    self.poll_read(cx, buf)
                }
                Ok(n) => Poll::Ready(Ok(n)),
                Err(err) => Poll::Ready(Err(err)),
            },
        }
    }
}

async fn read_cache_entry(
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,
    path: String,
    entry: CacheReadEntry,
) -> Result<(RpRead, BytesReader)> {
    let ((rp, r), cache_hit) = if entry.read_cache {
        match cache.read(&entry.cache_path, entry.cache_read_op()).await {
            Ok(v) => (v, true),
            Err(_) => (inner.read(&path, entry.inner_read_op()).await?, false),
        }
    } else {
        (inner.read(&path, entry.inner_read_op()).await?, false)
    };
    // If cache is hit, we don't need to fill the cache anymore.
    if cache_hit {
        return Ok((rp, r));
    }

    match entry.fill_method {
        CacheFillMethod::Skip => (),
        CacheFillMethod::Sync => {
            if let Ok((rp, r)) = inner.read(&path, entry.cache_fill_op()).await {
                let length = rp.into_metadata().content_length();

                // Ignore error happened during writing cache.
                let _ = cache
                    .write(&entry.cache_path, OpWrite::new(length), r)
                    .await;
            }
        }
        CacheFillMethod::Async => {
            let path = path.to_string();

            // # Notes
            //
            // If a `JoinHandle` is dropped, then the task continues running
            // in the background and its return value is lost.
            //
            // It's safe to just drop the handle here.
            //
            // # Todo
            //
            // We can support other runtime in the future.
            let _ = tokio::spawn(async move {
                let (rp, r) = inner.read(&path, entry.cache_fill_op()).await?;
                let length = rp.into_metadata().content_length();
                cache.write(&path, OpWrite::new(length), r).await?;

                Ok::<(), Error>(())
            });
        }
    }

    Ok((rp, r))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::memory;
    use crate::Operator;

    #[tokio::test]
    async fn test_whole_content_cache() -> anyhow::Result<()> {
        let op = Operator::new(memory::Builder::default().build()?);

        let cache_layer = CacheLayer::new(Arc::new(memory::Builder::default().build()?).into());
        let cached_op = op.clone().layer(cache_layer);

        // Write a new object into op.
        op.object("test_exist")
            .write("Hello, World!".as_bytes())
            .await?;

        // Read from cached op.
        let data = cached_op.object("test_exist").read().await?;
        assert_eq!(data.len(), 13);

        // Wait for https://github.com/datafuselabs/opendal/issues/957
        // // Write into cache op.
        // cached_op
        //     .object("test_exist")
        //     .write("Hello, Xuanwo!".as_bytes())
        //     .await?;
        // // op and cached op should have same data.
        // let data = op.object("test_exist").read().await?;
        // assert_eq!(data.len(), 14);
        // let data = cached_op.object("test_exist").read().await?;
        // assert_eq!(data.len(), 14);

        // Read not exist object.
        let data = cached_op.object("test_not_exist").read().await;
        assert_eq!(data.unwrap_err().kind(), ErrorKind::ObjectNotFound);

        Ok(())
    }

    #[tokio::test]
    async fn test_fixed_content_cache() -> anyhow::Result<()> {
        let op = Operator::new(memory::Builder::default().build()?);

        let cache_layer = CacheLayer::new(Arc::new(memory::Builder::default().build()?).into());
        let cached_op = op.clone().layer(cache_layer);

        // Write a new object into op.
        op.object("test_exist")
            .write("Hello, World!".as_bytes())
            .await?;

        // Read from cached op.
        let data = cached_op.object("test_exist").read().await?;
        assert_eq!(data.len(), 13);

        // Wait for https://github.com/datafuselabs/opendal/issues/957
        // Write into cache op.
        // cached_op
        //     .object("test_exist")
        //     .write("Hello, Xuanwo!".as_bytes())
        //     .await?;
        // // op and cached op should have same data.
        // let data = op.object("test_exist").read().await?;
        // assert_eq!(data.len(), 14);
        // let data = cached_op.object("test_exist").read().await?;
        // assert_eq!(data.len(), 14);

        // Read part of data
        let data = cached_op.object("test_exist").range_read(5..).await?;
        assert_eq!(data.len(), 8);
        assert_eq!(data, ", World!".as_bytes());

        // Write a new object into op.
        op.object("test_new")
            .write("Hello, OpenDAL!".as_bytes())
            .await?;

        // Read part of data
        let data = cached_op.object("test_new").range_read(6..).await?;
        assert_eq!(data.len(), 9);
        assert_eq!(data, " OpenDAL!".as_bytes());

        // Read not exist object.
        let data = cached_op.object("test_not_exist").read().await;
        assert_eq!(data.unwrap_err().kind(), ErrorKind::ObjectNotFound);

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_cache() -> anyhow::Result<()> {
        let op = Operator::new(memory::Builder::default().build()?);

        let cache_layer = CacheLayer::new(Arc::new(memory::Builder::default().build()?).into());
        let cached_op = op.clone().layer(cache_layer);

        // Write a new object into op.
        op.object("test_exist")
            .write("Hello, World!".as_bytes())
            .await?;
        // Stat from cached op.
        let meta = cached_op.object("test_exist").metadata().await?;
        assert_eq!(meta.content_length(), 13);

        // Write into cache op.
        cached_op
            .object("test_exist")
            .write("Hello, Xuanwo!".as_bytes())
            .await?;
        // op and cached op should have same data.
        let meta = op.object("test_exist").metadata().await?;
        assert_eq!(meta.content_length(), 14);
        let meta = cached_op.object("test_exist").metadata().await?;
        assert_eq!(meta.content_length(), 14);

        // Stat not exist object.
        let meta = cached_op.object("test_not_exist").metadata().await;
        assert_eq!(meta.unwrap_err().kind(), ErrorKind::ObjectNotFound);

        Ok(())
    }
}
