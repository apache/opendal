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
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::io;
use futures::ready;
use futures::AsyncRead;
use futures::FutureExt;

use super::policy::CacheReadEntry;
use super::policy::CacheReadEntryIterator;
use super::policy::CacheUpdateMethod;
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

    #[inline]
    async fn update_cache(&self, path: &str, op: Operation) {
        let it = self.policy.on_update(path, op).await;
        for entry in it {
            match entry.update_method {
                CacheUpdateMethod::Skip => continue,
                CacheUpdateMethod::Delete => {
                    let _ = self
                        .cache
                        .delete(&entry.cache_path, OpDelete::default())
                        .await;
                }
            }
        }
    }
}

#[async_trait]
impl Accessor for CacheAccessor {
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        Some(self.inner.clone())
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.update_cache(path, Operation::Create).await;

        self.inner.create(path, args).await
    }

    async fn read(&self, path: &str, mut args: OpRead) -> Result<(RpRead, BytesReader)> {
        let total_size = if let Some(total_size) = args.total_size_hint() {
            total_size
        } else {
            let rp = self.inner.stat(path, OpStat::default()).await?;
            rp.into_metadata().content_length()
        };

        let bcr = BytesContentRange::from_bytes_range(total_size, args.range());
        let br = bcr.to_bytes_range().expect("bytes range must be valid");
        args = args.with_range(br);

        let (offset, size) = (
            args.range().offset().expect("offset must be valid"),
            args.range().size().expect("size must be valid"),
        );

        let it = self.policy.on_read(path, offset, size, total_size).await;

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
        self.update_cache(path, Operation::Write).await;

        self.inner.write(path, args, r).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.update_cache(path, Operation::Delete).await;

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
            // let cache = entry.cache_path.to_string();

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
                cache
                    .write(&entry.cache_path, OpWrite::new(length), r)
                    .await?;

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
    async fn test_default_content_cache() -> anyhow::Result<()> {
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

        // Write into cache op.
        cached_op
            .object("test_exist")
            .write("Hello, Xuanwo!".as_bytes())
            .await?;
        // op and cached op should have same data.
        let data = op.object("test_exist").read().await?;
        assert_eq!(data.len(), 14);
        let data = cached_op.object("test_exist").read().await?;
        assert_eq!(data.len(), 14);

        // Read not exist object.
        let data = cached_op.object("test_not_exist").read().await;
        assert_eq!(data.unwrap_err().kind(), ErrorKind::ObjectNotFound);

        Ok(())
    }
}
