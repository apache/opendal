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

use opendal_core::Buffer;
use opendal_core::EntryMode;
use opendal_core::Error;
use opendal_core::Metadata;
use opendal_core::Result;
use opendal_core::raw::Access;
use opendal_core::raw::BytesRange;
use opendal_core::raw::OpRead;
use opendal_core::raw::OpStat;
use opendal_core::raw::RpRead;
use opendal_core::raw::oio::ReadStream;

use crate::FoyerKey;
use crate::FoyerValue;
use crate::Inner;
use crate::error::{FetchError, extract_err};

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

        // Use get_or_fetch to read data from cache or fallback to remote. It can automatically
        // handle the thundering herd problem by ensuring only one request is made for a given
        // key.
        //
        // Please note that we only cache the object if it's smaller than size_limit. And we'll
        // fetch the ENTIRE object from remote to put it into cache, then slice it to the requested
        // range.
        let key = FoyerKey {
            path: path_str.clone(),
            version: version.clone(),
        };

        if self
            .inner
            .deleted_keys
            .lock()
            .expect("deleted keys lock poisoned")
            .contains(&key)
        {
            let (rp, mut reader) = self.inner.accessor.read(path, original_args).await?;
            let buffer = reader.read_all().await?;
            return Ok((rp, buffer));
        }

        let result = self
            .inner
            .cache
            .get_or_fetch(&key, || {
                let inner = self.inner.clone();
                let size_limit = self.size_limit.clone();
                let path_clone = path_str.clone();
                async move {
                    // read the metadata first, if it's too large, do not cache
                    let metadata = inner
                        .accessor
                        .stat(&path_clone, OpStat::default())
                        .await
                        .map_err(FetchError::from_error)?
                        .into_metadata();

                    let size = metadata.content_length() as usize;
                    if !size_limit.contains(&size) {
                        return Err(FetchError::SizeTooLarge);
                    }

                    // fetch the ENTIRE object from remote.
                    let (_, mut reader) = inner
                        .accessor
                        .read(
                            &path_clone,
                            OpRead::default().with_range(BytesRange::new(0, None)),
                        )
                        .await
                        .map_err(FetchError::from_error)?;
                    let buffer = reader.read_all().await.map_err(FetchError::from_error)?;

                    Ok(FoyerValue(buffer))
                }
            })
            .await;

        // If got entry from cache, slice it to the requested range. If it's larger than size_limit,
        // we'll simply forward the request to the underlying accessor with user's given range.
        match result {
            Ok(entry) => {
                let end = range_end.unwrap_or(entry.len() as u64);
                let buffer = entry.slice(range_start as usize..end as usize);
                let rp = RpRead::new(
                    Metadata::new(EntryMode::FILE).with_content_length(entry.len() as _),
                );
                Ok((rp, buffer))
            }
            Err(e) => match e.downcast_ref::<FetchError>() {
                Some(FetchError::SizeTooLarge) => {
                    let (rp, mut reader) = self.inner.accessor.read(path, original_args).await?;
                    let buffer = reader.read_all().await?;
                    Ok((rp, buffer))
                }
                Some(FetchError::Source { kind, message }) => {
                    Err(Error::new(*kind, message.clone()))
                }
                None => Err(extract_err(e)),
            },
        }
    }
}
