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

use std::cmp::min;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::io;
use futures::io::Cursor;
use futures::ready;
use futures::AsyncRead;
use futures::AsyncReadExt;
use futures::FutureExt;

use super::*;
use crate::raw::*;
use crate::*;

/// Create a new whole cache reader.
pub async fn new_whole_cache_reader(
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,
    path: &str,
    args: OpRead,
    cache_fill: CacheFillMethod,
) -> Result<(RpRead, BytesReader)> {
    let ((rp, r), cache_hit) = match cache.read(path, args.clone()).await {
        Ok(v) => (v, true),
        Err(_) => (inner.read(path, args.clone()).await?, false),
    };
    // If cache is hit, we don't need to fill the cache anymore.
    if cache_hit {
        return Ok((rp, r));
    }

    match cache_fill {
        CacheFillMethod::Skip => Ok((rp, r)),
        CacheFillMethod::Sync => {
            let length = rp.into_metadata().content_length();

            // Ignore error happened during writing cache.
            let _ = cache.write(path, OpWrite::new(length), r).await;
            match cache.read(path, args.clone()).await {
                Ok(v) => Ok(v),
                Err(_) => inner.read(path, args).await,
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
                let (rp, r) = inner.read(&path, OpRead::new()).await?;
                let length = rp.into_metadata().content_length();
                cache.write(&path, OpWrite::new(length), r).await?;

                Ok::<(), Error>(())
            });
            Ok((rp, r))
        }
    }
}

/// Create a new whole cache reader.
pub async fn new_fixed_cache_reader(
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,
    path: &str,
    args: OpRead,
    step: u64,
    cache_fill: CacheFillMethod,
) -> Result<(RpRead, BytesReader)> {
    let range = args.range();
    let it = match (range.offset(), range.size()) {
        (Some(offset), Some(size)) => FixedCacheRangeIterator::new(offset, size, step),
        _ => {
            let meta = inner.stat(path, OpStat::new()).await?.into_metadata();
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
    let r = FixedCacheReader::new(inner, cache, path, it, cache_fill);
    Ok((RpRead::new(length), Box::new(r)))
}

pub struct FixedCacheReader {
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,
    state: FixedCacheState,

    path: String,
    cache_fill: CacheFillMethod,
}

impl FixedCacheReader {
    fn new(
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        it: FixedCacheRangeIterator,
        cache_fill: CacheFillMethod,
    ) -> Self {
        Self {
            inner,
            cache,
            state: FixedCacheState::Iterating(it),
            path: path.to_string(),
            cache_fill,
        }
    }
}

async fn read_cache_without_fill(
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,
    path: String,
    idx: u64,
    cache_range: BytesRange,
    actual_range: BytesRange,
) -> Result<(RpRead, BytesReader)> {
    let cache_path = format_content_cache_path(&path, idx);

    match cache
        .read(&cache_path, OpRead::new().with_range(cache_range))
        .await
    {
        Ok(r) => Ok(r),
        Err(_) => {
            inner
                .read(&path, OpRead::new().with_range(actual_range))
                .await
        }
    }
}

async fn read_cache_sync_fill(
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,
    path: String,
    idx: u64,
    cache_range: BytesRange,
    total_range: BytesRange,
    actual_range: BytesRange,
) -> Result<(RpRead, BytesReader)> {
    let cache_path = format_content_cache_path(&path, idx);

    match cache
        .read(&cache_path, OpRead::new().with_range(cache_range))
        .await
    {
        Ok(r) => Ok(r),
        Err(err) if err.kind() == ErrorKind::ObjectNotFound => {
            let (rp, mut r) = inner
                .read(&path, OpRead::new().with_range(total_range))
                .await?;
            let size = rp.into_metadata().content_length();
            let mut bs = Vec::with_capacity(size as usize);
            r.read_to_end(&mut bs).await.map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "read from inner storage during read_cache_sync_fill",
                )
                .with_operation(Operation::Read.into_static())
                .set_source(err)
            })?;
            let bs = Bytes::from(bs);

            // Ignore errors returned by cache.
            let _ = cache
                .write(
                    &cache_path,
                    OpWrite::new(size),
                    Box::new(Cursor::new(bs.clone())),
                )
                .await;

            let bs = cache_range.apply_on_bytes(bs);
            Ok((
                RpRead::new(bs.len() as u64),
                Box::new(Cursor::new(bs)) as BytesReader,
            ))
        }
        Err(_) => {
            inner
                .read(&path, OpRead::new().with_range(actual_range))
                .await
        }
    }
}

async fn read_cache_async_fill(
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,
    path: String,
    idx: u64,
    cache_range: BytesRange,
    total_range: BytesRange,
    actual_range: BytesRange,
) -> Result<(RpRead, BytesReader)> {
    let cache_path = format_content_cache_path(&path, idx);

    match cache
        .read(&cache_path, OpRead::new().with_range(cache_range))
        .await
    {
        Ok(r) => Ok(r),
        Err(_) => {
            let path = path.to_string();

            let (rp, r) = inner
                .read(&path, OpRead::new().with_range(actual_range))
                .await?;

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
                let (rp, r) = inner
                    .read(&path, OpRead::new().with_range(total_range))
                    .await?;
                let length = rp.into_metadata().content_length();
                cache.write(&cache_path, OpWrite::new(length), r).await?;

                Ok::<(), Error>(())
            });

            Ok((rp, r))
        }
    }
}

impl AsyncRead for FixedCacheReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let cache = self.cache.clone();
        let inner = self.inner.clone();
        let path = self.path.clone();
        let cache_fill = self.cache_fill;

        match &mut self.state {
            FixedCacheState::Iterating(it) => {
                let range = it.next();
                match range {
                    None => Poll::Ready(Ok(0)),
                    Some((idx, cache_range, total_range, actual_range)) => {
                        let fut: BoxFuture<'static, Result<(RpRead, BytesReader)>> =
                            match cache_fill {
                                CacheFillMethod::Skip => Box::pin(read_cache_without_fill(
                                    inner,
                                    cache,
                                    path,
                                    idx,
                                    cache_range,
                                    actual_range,
                                )),
                                CacheFillMethod::Sync => Box::pin(read_cache_sync_fill(
                                    inner,
                                    cache,
                                    path,
                                    idx,
                                    cache_range,
                                    total_range,
                                    actual_range,
                                )),
                                CacheFillMethod::Async => Box::pin(read_cache_async_fill(
                                    inner,
                                    cache,
                                    path,
                                    idx,
                                    cache_range,
                                    total_range,
                                    actual_range,
                                )),
                            };

                        self.state = FixedCacheState::Fetching((*it, fut));
                        self.poll_read(cx, buf)
                    }
                }
            }
            FixedCacheState::Fetching((it, fut)) => {
                let r = ready!(fut.poll_unpin(cx))?;
                self.state = FixedCacheState::Reading((*it, r));
                self.poll_read(cx, buf)
            }
            FixedCacheState::Reading((it, (_, r))) => {
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

enum FixedCacheState {
    Iterating(FixedCacheRangeIterator),
    Fetching(
        (
            FixedCacheRangeIterator,
            BoxFuture<'static, Result<(RpRead, BytesReader)>>,
        ),
    ),
    Reading((FixedCacheRangeIterator, (RpRead, BytesReader))),
}

/// Build the path for OpenDAL Content Cache.
fn format_content_cache_path(path: &str, idx: u64) -> String {
    format!("{path}.occ_{idx}")
}

#[derive(Copy, Clone, Debug)]
pub struct FixedCacheRangeIterator {
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

    /// Actual range is the range that read from underlying storage
    /// without padding with step.
    ///
    /// We can read this range if we don't need to fill the cache.
    fn actual_range(&self) -> BytesRange {
        let idx = self.cur / self.step;
        (self.cur..min(self.step * (idx + 1), self.size + self.offset)).into()
    }
}

impl Iterator for FixedCacheRangeIterator {
    /// Item with return (cache_idx, cache_range, total_range, acutal_range)
    type Item = (u64, BytesRange, BytesRange, BytesRange);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur >= self.offset + self.size {
            None
        } else {
            let (cache_index, cache_range, total_range, actual_range) = (
                self.cache_index(),
                self.cache_range(),
                self.total_range(),
                self.actual_range(),
            );
            self.cur += cache_range.size().expect("cache range size must be valid");
            Some((cache_index, cache_range, total_range, actual_range))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_cache_range_iterator() {
        let cases = vec![
            (
                "first part",
                0,
                1,
                1000,
                vec![(
                    0,
                    BytesRange::from(0..1),
                    BytesRange::from(0..1000),
                    BytesRange::from(0..1),
                )],
            ),
            (
                "first part with offset",
                900,
                1,
                1000,
                vec![(
                    0,
                    BytesRange::from(900..901),
                    BytesRange::from(0..1000),
                    BytesRange::from(900..901),
                )],
            ),
            (
                "first part with edge case",
                900,
                100,
                1000,
                vec![(
                    0,
                    BytesRange::from(900..1000),
                    BytesRange::from(0..1000),
                    BytesRange::from(900..1000),
                )],
            ),
            (
                "two parts",
                900,
                101,
                1000,
                vec![
                    (
                        0,
                        BytesRange::from(900..1000),
                        BytesRange::from(0..1000),
                        BytesRange::from(900..1000),
                    ),
                    (
                        1,
                        BytesRange::from(0..1),
                        BytesRange::from(1000..2000),
                        BytesRange::from(1000..1001),
                    ),
                ],
            ),
            (
                "second part",
                1001,
                1,
                1000,
                vec![(
                    1,
                    BytesRange::from(1..2),
                    BytesRange::from(1000..2000),
                    BytesRange::from(1001..1002),
                )],
            ),
        ];

        for (name, offset, size, step, expected) in cases {
            let it = FixedCacheRangeIterator::new(offset, size, step);
            let actual: Vec<_> = it.collect();

            assert_eq!(expected, actual, "{name}")
        }
    }
}
