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

use std::sync::Arc;

use foyer::Error as FoyerError;

use opendal_core::Buffer;
use opendal_core::Result;
use opendal_core::raw::Access;
use opendal_core::raw::BytesContentRange;
use opendal_core::raw::BytesRange;
use opendal_core::raw::OpRead;
use opendal_core::raw::OpStat;
use opendal_core::raw::RpRead;
use opendal_core::raw::oio::Read;

use crate::CachedBuffer;
use crate::FoyerKey;
use crate::FoyerValue;
use crate::Inner;
use crate::error::{FetchSizeTooLarge, extract_err};

pub struct FullReader<A: Access> {
    inner: Arc<Inner<A>>,
    size_limit: std::ops::Range<usize>,
}

impl<A: Access> FullReader<A> {
    pub fn new(inner: Arc<Inner<A>>, size_limit: std::ops::Range<usize>) -> Self {
        Self { inner, size_limit }
    }

    /// Read data from cache or underlying storage.
    /// Caches the ENTIRE object, then slices to requested range.
    pub async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Buffer)> {
        let path_str = path.to_string();
        let version = args.version().map(|v| v.to_string());
        let original_args = args.clone();

        // Extract range bounds before async block to avoid lifetime issues
        let (range_start, range_end) = {
            let r = args.range();
            let start = r.offset();
            let end = r.size().map(|size| start + size);
            (start, end)
        };

        // Use fetch to read data from cache or fallback to remote. fetch() can automatically
        // handle the thundering herd problem by ensuring only one request is made for a given
        // key.
        //
        // Please note that we only cache the object if it's smaller than size_limit. And we'll
        // fetch the ENTIRE object from remote to put it into cache, then slice it to the requested
        // range.
        let result = self
            .inner
            .cache
            .fetch(
                FoyerKey::Full {
                    path: path_str.clone(),
                    version: version.clone(),
                },
                || {
                    let inner = self.inner.clone();
                    let size_limit = self.size_limit.clone();
                    let path_clone = path_str.clone();
                    async move {
                        // read the metadata first, if it's too large, do not cache
                        let metadata = inner
                            .accessor
                            .stat(&path_clone, OpStat::default())
                            .await
                            .map_err(FoyerError::other)?
                            .into_metadata();

                        let size = metadata.content_length() as usize;
                        if !size_limit.contains(&size) {
                            return Err(FoyerError::other(FetchSizeTooLarge));
                        }

                        // fetch the ENTIRE object from remote.
                        let (_, mut reader) = inner
                            .accessor
                            .read(
                                &path_clone,
                                OpRead::default().with_range(BytesRange::new(0, None)),
                            )
                            .await
                            .map_err(FoyerError::other)?;
                        let buffer = reader.read_all().await.map_err(FoyerError::other)?;

                        Ok(FoyerValue::Buffer(CachedBuffer(buffer)))
                    }
                },
            )
            .await;

        // If got entry from cache, slice it to the requested range. If it's larger than size_limit,
        // we'll simply forward the request to the underlying accessor with user's given range.
        match result {
            Ok(entry) => match entry.value() {
                FoyerValue::Buffer(cached) => {
                    let full_buffer = cached.0.clone();
                    let full_size = full_buffer.len() as u64;
                    let end = range_end.unwrap_or(full_size);
                    let sliced = full_buffer.slice(range_start as usize..end as usize);
                    let rp = make_rp_read(range_start, sliced.len() as u64, Some(full_size));
                    return Ok((rp, sliced));
                }
                _ => {
                    // fallback to underlying accessor if cached value mismatch, this should not
                    // happen, but if it does, let's simply regard it as a cache miss and fetch the
                    // data again.
                }
            },
            Err(e) => match e.downcast::<FetchSizeTooLarge>() {
                Ok(_) => {
                    // fallback to underlying accessor if object size is too big
                }
                Err(e) => return Err(extract_err(e)),
            },
        };

        let (_, mut reader) = self.inner.accessor.read(path, original_args).await?;
        let buffer = reader.read_all().await?;
        let rp = make_rp_read(range_start, buffer.len() as u64, None);
        Ok((rp, buffer))
    }
}

/// Build RpRead with range information.
///
/// - `range_start`: Start offset of the returned data
/// - `buffer_len`: Length of the returned buffer
/// - `total_size`: Total object size if known (for cache hit), None for fallback path
fn make_rp_read(range_start: u64, buffer_len: u64, total_size: Option<u64>) -> RpRead {
    if buffer_len == 0 {
        return RpRead::new().with_size(Some(0));
    }

    let range_end = range_start + buffer_len - 1;
    let mut range = BytesContentRange::default().with_range(range_start, range_end);
    if let Some(size) = total_size {
        range = range.with_size(size);
    }

    RpRead::new()
        .with_size(Some(buffer_len))
        .with_range(Some(range))
}
