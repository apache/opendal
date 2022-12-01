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
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::io;
use futures::io::Cursor;
use futures::ready;
use futures::AsyncRead;
use futures::AsyncReadExt;
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
    // If we don't need to fill the cache, we can read with inner_read_cache
    // directly.
    if entry.fill_method == CacheFillMethod::Skip {
        let (rp, r, _) = read_for_load_cache(&inner, &cache, &path, &entry).await?;

        return Ok((rp, r));
    }

    // If we need to fill cache in sync way, we can fill cache first
    // and try to load from cache.
    if entry.fill_method == CacheFillMethod::Sync {
        let (rp, mut r, cache_hit) = read_for_fill_cache(&inner, &cache, &path, &entry).await?;
        if cache_hit {
            return Ok((rp, r));
        }

        let meta = rp.into_metadata();
        let size = meta.content_length();
        // if the size is small enough, we can load in memory to avoid
        // load from cache again. Otherwise, we will fallback to write
        // in to cache first and than read from cache.
        //
        // TODO: make this a config value.
        if size < 8 * 1024 * 1024 {
            let mut bs = Vec::with_capacity(size as usize);
            r.read_to_end(&mut bs).await.map_err(|err| {
                Error::new(ErrorKind::Unexpected, "read from underlying storage")
                    .set_source(err)
                    .set_temporary()
            })?;
            let bs = Bytes::from(bs);

            // Ignore error happened during writing cache.
            let _ = cache
                .write(
                    &entry.cache_path,
                    OpWrite::new(size),
                    Box::new(Cursor::new(bs.clone())),
                )
                .await;

            // Make sure the reading range has been applied on cache.
            let bs = entry.cache_read_range.apply_on_bytes(bs);

            return Ok((RpRead::new(bs.len() as u64), Box::new(Cursor::new(bs))));
        } else {
            // Ignore error happened during writing cache.
            let _ = cache.write(&entry.cache_path, OpWrite::new(size), r).await;

            let (rp, r, _) = read_for_load_cache(&inner, &cache, &path, &entry).await?;

            return Ok((rp, r));
        }
    }

    // If we need to fill cache in async way.
    let (rp, r, cache_hit) = read_for_load_cache(&inner, &cache, &path, &entry).await?;
    if cache_hit {
        return Ok((rp, r));
    }

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
    let moved_inner = inner.clone();
    let moved_cache = cache.clone();
    let moved_path = path.clone();
    let moved_entry = entry.clone();
    let _ = tokio::spawn(async move {
        let (rp, r) = moved_inner
            .read(&moved_path, moved_entry.cache_fill_op())
            .await?;
        let length = rp.into_metadata().content_length();
        moved_cache
            .write(&moved_entry.cache_path, OpWrite::new(length), r)
            .await?;

        Ok::<(), Error>(())
    });

    Ok((rp, r))
}

/// Read for loading cache.
///
/// This function is used to load cache.
async fn read_for_load_cache(
    inner: &Arc<dyn Accessor>,
    cache: &Arc<dyn Accessor>,
    path: &str,
    entry: &CacheReadEntry,
) -> Result<(RpRead, BytesReader, bool)> {
    if !entry.read_cache {
        let (rp, r) = inner.read(path, entry.inner_read_op()).await?;

        return Ok((rp, r, false));
    }

    let res = match cache.read(&entry.cache_path, entry.cache_read_op()).await {
        Ok((rp, r)) => (rp, r, true),
        Err(_) => {
            let (rp, r) = inner.read(path, entry.inner_read_op()).await?;
            (rp, r, false)
        }
    };

    Ok(res)
}

/// Read for filling cache.
///
/// This function is used to read data that can by cached.
///
/// - If the cache is exist, we will return the real content.
/// - If the cache is missing or not read from cache, we will return the data
///   for filling cache.
async fn read_for_fill_cache(
    inner: &Arc<dyn Accessor>,
    cache: &Arc<dyn Accessor>,
    path: &str,
    entry: &CacheReadEntry,
) -> Result<(RpRead, BytesReader, bool)> {
    if !entry.read_cache {
        let (rp, r) = inner.read(path, entry.cache_fill_op()).await?;

        return Ok((rp, r, false));
    }

    // If cache does exists.
    if let Ok((rp, r)) = cache.read(&entry.cache_path, entry.cache_read_op()).await {
        return Ok((rp, r, true));
    }

    let (rp, r) = inner.read(path, entry.cache_fill_op()).await?;

    Ok((rp, r, false))
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
