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
use std::io::ErrorKind;
use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::AsyncRead;
use futures::FutureExt;

use super::util::set_accessor_for_object_iterator;
use super::util::set_accessor_for_object_steamer;
use crate::ops::BytesContentRange;
use crate::ops::BytesRange;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::BlockingBytesReader;
use crate::BytesReader;
use crate::Layer;
use crate::ObjectIterator;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::ObjectReader;
use crate::ObjectStreamer;

/// ContentCacheLayer will add content data cache support for OpenDAL.
///
/// # Notes
///
/// This layer only maintains its own states. Users should care about the cache
/// consistency by themselves. For example, in the following situations, users
/// could get out-dated metadata cache:
///
/// - Users have operations on underlying operator directly.
/// - Other nodes have operations on underlying storage directly.
/// - Concurrent read/write/delete on the same path.
///
/// To make sure content cache consistent across the cluster, please make sure
/// all nodes in the cluster use the same cache services like redis or tikv.
///
/// # Examples
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::ContentCacheLayer;
/// use opendal::layers::ContentCacheStrategy;
/// use opendal::services::memory;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::from_env(Scheme::Fs)
///     .expect("must init")
///     .layer(ContentCacheLayer::new(
///         memory::Builder::default().build().expect("must init"),
///         ContentCacheStrategy::Whole,
///     ));
/// ```
#[derive(Debug, Clone)]
pub struct ContentCacheLayer {
    cache: Arc<dyn Accessor>,
    strategy: ContentCacheStrategy,
}

impl ContentCacheLayer {
    /// Create a new metadata cache layer.
    pub fn new(acc: impl Accessor + 'static, strategy: ContentCacheStrategy) -> Self {
        Self {
            cache: Arc::new(acc),
            strategy,
        }
    }
}

impl Layer for ContentCacheLayer {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(ContentCacheAccessor {
            inner,
            cache: self.cache.clone(),
            strategy: self.strategy.clone(),
        })
    }
}

/// The strategy of content cache.
#[derive(Debug, Clone)]
pub enum ContentCacheStrategy {
    /// Always cache the whole object content.
    Whole,
    /// Cache the object content in parts with fixed size.
    Fixed(u64),
}

#[derive(Debug, Clone)]
struct ContentCacheAccessor {
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,

    strategy: ContentCacheStrategy,
}

#[async_trait]
impl Accessor for ContentCacheAccessor {
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        Some(self.inner.clone())
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        self.cache.delete(path, OpDelete::new()).await?;
        self.inner.create(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<ObjectReader> {
        match self.strategy {
            ContentCacheStrategy::Whole => self.new_whole_cache_reader(path, args).await,
            ContentCacheStrategy::Fixed(step) => {
                self.new_fixed_cache_reader(path, args, step).await
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        self.cache.delete(path, OpDelete::new()).await?;
        self.inner.write(path, args, r).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<()> {
        self.cache.delete(path, OpDelete::new()).await?;
        self.inner.delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer> {
        self.inner
            .list(path, args)
            .await
            .map(|s| set_accessor_for_object_steamer(s, self.clone()))
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<()> {
        self.cache.blocking_delete(path, OpDelete::new())?;
        self.inner.blocking_create(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<BlockingBytesReader> {
        match self.cache.blocking_read(path, args.clone()) {
            Ok(r) => Ok(r),
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let meta = self.inner.blocking_stat(path, OpStat::new())?;
                let r = if meta.mode().is_file() {
                    let size = meta.content_length();
                    let reader = self.inner.blocking_read(path, OpRead::new())?;
                    self.cache
                        .blocking_write(path, OpWrite::new(size), reader)?;
                    self.cache.blocking_read(path, args)?
                } else {
                    self.inner.blocking_read(path, args)?
                };
                Ok(r)
            }
            Err(err) => Err(err),
        }
    }

    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<u64> {
        self.cache.blocking_delete(path, OpDelete::new())?;
        self.inner.blocking_write(path, args, r)
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<()> {
        self.cache.blocking_delete(path, OpDelete::new())?;
        self.inner.blocking_delete(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<ObjectIterator> {
        self.inner
            .blocking_list(path, args)
            .map(|s| set_accessor_for_object_iterator(s, self.clone()))
    }
}

impl ContentCacheAccessor {
    /// Create a new whole cache reader.
    async fn new_whole_cache_reader(&self, path: &str, args: OpRead) -> Result<ObjectReader> {
        match self.cache.read(path, args.clone()).await {
            Ok(r) => Ok(r),
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let r = self.inner.read(path, OpRead::new()).await?;

                let length = r.content_length();
                self.cache
                    .write(path, OpWrite::new(length), r.into_reader())
                    .await?;
                self.cache.read(path, args).await
            }
            Err(err) => Err(err),
        }
    }

    async fn new_fixed_cache_reader(
        &self,
        path: &str,
        args: OpRead,
        step: u64,
    ) -> Result<ObjectReader> {
        let range = args.range();
        let it = match (range.offset(), range.size()) {
            (Some(offset), Some(size)) => FixedCacheRangeIterator::new(offset, size, step),
            _ => {
                let meta = self.inner.stat(path, OpStat::new()).await?;
                let bcr = BytesContentRange::from_bytes_range(meta.content_length(), range);
                let br = bcr.to_bytes_range().expect("bytes range must be valid");
                FixedCacheRangeIterator::new(
                    br.offset().expect("offset must be valid"),
                    br.size().expect("size must be valid"),
                    step,
                )
            }
        };

        let length = it.size();
        let r = FixedCacheReader::new(self.inner.clone(), self.cache.clone(), path, it);
        Ok(ObjectReader::new(Box::new(r))
            .with_meta(ObjectMetadata::new(ObjectMode::FILE).with_content_length(length)))
    }
}

#[derive(Copy, Clone, Debug)]
struct FixedCacheRangeIterator {
    offset: u64,
    size: u64,
    step: u64,

    cur: u64,
}

impl FixedCacheRangeIterator {
    fn new(offset: u64, size: u64, step: u64) -> Self {
        Self {
            offset,
            size,
            step,

            cur: offset,
        }
    }

    fn size(&self) -> u64 {
        self.size
    }

    /// Cache index is the file index across the whole file.
    fn cache_index(&self) -> u64 {
        self.cur / self.step
    }

    /// Cache range is the range that we need to read from cache file.
    fn cache_range(&self) -> BytesRange {
        let skipped_rem = self.cur % self.step;
        let to_read = self.size + self.offset - self.cur;
        if to_read >= (self.step - skipped_rem) {
            (skipped_rem..self.step).into()
        } else {
            (skipped_rem..skipped_rem + to_read).into()
        }
    }

    /// Total range is the range that we need to read from underlying storage.
    ///
    /// # Note
    ///
    /// We will always read `step` bytes from underlying storage.
    fn total_range(&self) -> BytesRange {
        let idx = self.cur / self.step;
        (self.step * idx..self.step * (idx + 1)).into()
    }
}

impl Iterator for FixedCacheRangeIterator {
    /// Item with return (cache_idx, cache_range, total_range)
    type Item = (u64, BytesRange, BytesRange);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur >= self.offset + self.size {
            None
        } else {
            let (cache_index, cache_range, total_range) =
                (self.cache_index(), self.cache_range(), self.total_range());
            self.cur += cache_range.size().expect("cache range size must be valid");
            Some((cache_index, cache_range, total_range))
        }
    }
}

enum FixedCacheState {
    Iterating(FixedCacheRangeIterator),
    Fetching(
        (
            FixedCacheRangeIterator,
            BoxFuture<'static, Result<ObjectReader>>,
        ),
    ),
    Reading((FixedCacheRangeIterator, ObjectReader)),
}

fn format_cache_path(path: &str, idx: u64) -> String {
    format!("{path}-{idx}")
}

struct FixedCacheReader {
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,
    state: FixedCacheState,

    path: String,
}

impl FixedCacheReader {
    fn new(
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        it: FixedCacheRangeIterator,
    ) -> Self {
        Self {
            inner,
            cache,
            state: FixedCacheState::Iterating(it),
            path: path.to_string(),
        }
    }
}

impl AsyncRead for FixedCacheReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        let cache = self.cache.clone();
        let inner = self.inner.clone();
        let path = self.path.clone();

        match &mut self.state {
            FixedCacheState::Iterating(it) => {
                let range = it.next();
                match range {
                    None => Poll::Ready(Ok(0)),
                    Some((idx, cache_range, total_range)) => {
                        let cache_path = format_cache_path(&path, idx);
                        let fut = async move {
                            match cache
                                .read(&cache_path, OpRead::new().with_range(cache_range))
                                .await
                            {
                                Ok(r) => Ok(r),
                                Err(err) if err.kind() == ErrorKind::NotFound => {
                                    let r = inner
                                        .read(&path, OpRead::new().with_range(total_range))
                                        .await?;
                                    let size = r.content_length();
                                    cache
                                        .write(&path, OpWrite::new(size), r.into_reader())
                                        .await?;
                                    cache
                                        .read(&path, OpRead::new().with_range(cache_range))
                                        .await
                                }
                                Err(err) => Err(err),
                            }
                        };
                        self.state = FixedCacheState::Fetching((*it, Box::pin(fut)));
                        self.poll_read(cx, buf)
                    }
                }
            }
            FixedCacheState::Fetching((it, fut)) => {
                let r = ready!(fut.poll_unpin(cx))?;
                self.state = FixedCacheState::Reading((*it, r));
                self.poll_read(cx, buf)
            }
            FixedCacheState::Reading((it, r)) => {
                let n = match ready!(Pin::new(r).poll_read(cx, buf)) {
                    Ok(n) => n,
                    Err(err) => return Poll::Ready(Err(err)),
                };

                if n == 0 {
                    self.state = FixedCacheState::Iterating(*it);
                    self.poll_read(cx, buf)
                } else {
                    Poll::Ready(Ok(n))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::memory;
    use crate::Operator;

    #[tokio::test]
    async fn test_whole_content_cache() -> anyhow::Result<()> {
        let op = Operator::new(memory::Builder::default().build()?);

        let cache_layer = ContentCacheLayer::new(
            memory::Builder::default().build()?,
            ContentCacheStrategy::Whole,
        );
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
        assert_eq!(data.unwrap_err().kind(), ErrorKind::NotFound);

        Ok(())
    }

    #[tokio::test]
    async fn test_fixed_content_cache() -> anyhow::Result<()> {
        let op = Operator::new(memory::Builder::default().build()?);

        let cache_layer = ContentCacheLayer::new(
            memory::Builder::default().build()?,
            ContentCacheStrategy::Fixed(5),
        );
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

        // Read part of data
        let data = cached_op.object("test_exist").range_read(5..).await?;
        assert_eq!(data.len(), 9);
        assert_eq!(data, ", Xuanwo!".as_bytes());

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
        assert_eq!(data.unwrap_err().kind(), ErrorKind::NotFound);

        Ok(())
    }

    #[test]
    fn test_fixed_cache_range_iterator() {
        let cases = vec![
            (
                "first part",
                0,
                1,
                1000,
                vec![(0, BytesRange::from(0..1), BytesRange::from(0..1000))],
            ),
            (
                "first part with offset",
                900,
                1,
                1000,
                vec![(0, BytesRange::from(900..901), BytesRange::from(0..1000))],
            ),
            (
                "first part with edge case",
                900,
                100,
                1000,
                vec![(0, BytesRange::from(900..1000), BytesRange::from(0..1000))],
            ),
            (
                "two parts",
                900,
                101,
                1000,
                vec![
                    (0, BytesRange::from(900..1000), BytesRange::from(0..1000)),
                    (1, BytesRange::from(0..1), BytesRange::from(1000..2000)),
                ],
            ),
            (
                "second part",
                1001,
                1,
                1000,
                vec![(1, BytesRange::from(1..2), BytesRange::from(1000..2000))],
            ),
        ];

        for (name, offset, size, step, expected) in cases {
            let it = FixedCacheRangeIterator::new(offset, size, step);
            let actual: Vec<_> = it.collect();

            assert_eq!(expected, actual, "{name}")
        }
    }
}
