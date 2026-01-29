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
use opendal_core::Result;
use opendal_core::raw::Access;
use opendal_core::raw::BytesContentRange;
use opendal_core::raw::BytesRange;
use opendal_core::raw::OpRead;
use opendal_core::raw::OpStat;
use opendal_core::raw::RpRead;
use opendal_core::raw::oio::Read;

use crate::FoyerKey;
use crate::FoyerValue;
use crate::Inner;
use crate::cached_metadata::CachedMetadata;
use crate::chunk_utils::split_range_into_chunks;

/// ChunkedReader reads objects in fixed-size chunks, caching each chunk independently.
///
/// This enables efficient partial reads of large objects by only fetching and caching
/// the chunks that cover the requested range.
pub struct ChunkedReader<A: Access> {
    inner: Arc<Inner<A>>,
    chunk_size: usize,
}

impl<A: Access> ChunkedReader<A> {
    pub fn new(inner: Arc<Inner<A>>, chunk_size: usize) -> Self {
        Self { inner, chunk_size }
    }

    /// Read data from cache or underlying storage using chunked caching.
    ///
    /// The algorithm:
    /// 1. Fetch object metadata (cached) to determine content_length
    /// 2. Compute which chunks cover the requested range
    /// 3. Fetch each chunk (from cache or backend)
    /// 4. Assemble and slice the result to match the requested range
    pub async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Buffer)> {
        let version = args.version().map(|v| v.to_string());

        // Extract range info from args
        let range_start = args.range().offset();
        let range_size = args.range().size();

        // Step 1: Fetch metadata
        let metadata = self.fetch_metadata(path, version.clone()).await?;
        let object_size = metadata.content_length;

        // Step 2: Compute chunks needed
        let chunks = split_range_into_chunks(range_start, range_size, self.chunk_size, object_size);

        if chunks.is_empty() {
            let rp = RpRead::new().with_size(Some(0));
            return Ok((rp, Buffer::new()));
        }

        // Step 3: Fetch all needed chunks and assemble
        let mut result_bufs = Vec::with_capacity(chunks.len());
        let mut total_len = 0u64;

        for chunk_info in &chunks {
            let chunk_data = self
                .fetch_chunk(path, version.clone(), chunk_info.chunk_index, object_size)
                .await?;

            // Slice the chunk to get only the needed portion
            let end = (chunk_info.offset_in_chunk + chunk_info.length_in_chunk).min(chunk_data.len());
            let sliced = chunk_data.slice(chunk_info.offset_in_chunk..end);
            total_len += sliced.len() as u64;
            result_bufs.push(sliced);
        }

        // Step 4: Assemble result
        let range_end = range_start + total_len;
        let range = BytesContentRange::default()
            .with_range(range_start, range_end.saturating_sub(1))
            .with_size(object_size);

        let buffer: Buffer = result_bufs
            .into_iter()
            .flat_map(|b| b.to_bytes())
            .collect::<Vec<_>>()
            .into();

        let rp = RpRead::new()
            .with_size(Some(buffer.len() as u64))
            .with_range(Some(range));

        Ok((rp, buffer))
    }

    /// Fetch object metadata from cache or backend.
    ///
    /// Uses simple get/insert pattern: check cache first, on miss stat from backend
    /// and insert into cache.
    async fn fetch_metadata(
        &self,
        path: &str,
        version: Option<String>,
    ) -> Result<CachedMetadata> {
        let key = FoyerKey::Metadata {
            path: path.to_string(),
            chunk_size: self.chunk_size,
            version: version.clone(),
        };

        // Try cache first
        if let Ok(Some(entry)) = self.inner.cache.get(&key).await {
            let bytes = entry.value().0.to_vec();
            if let Ok(cached) = bincode::deserialize::<CachedMetadata>(&bytes) {
                return Ok(cached);
            }
            // Deserialization failed - cache entry is corrupted, fall through to re-fetch
        }

        // Cache miss - fetch from backend
        let metadata = self
            .inner
            .accessor
            .stat(path, OpStat::default())
            .await?
            .into_metadata();

        let cached = CachedMetadata {
            content_length: metadata.content_length(),
            version: metadata.version().map(|v| v.to_string()),
            etag: metadata.etag().map(|v| v.to_string()),
        };

        // Serialize and insert into cache
        if let Ok(encoded) = bincode::serialize(&cached) {
            self.inner
                .cache
                .insert(key, FoyerValue(Buffer::from(encoded)));
        }

        Ok(cached)
    }

    /// Fetch a single chunk from cache or backend.
    ///
    /// Uses simple get/insert pattern: check cache first, on miss read from backend
    /// and insert into cache.
    async fn fetch_chunk(
        &self,
        path: &str,
        version: Option<String>,
        chunk_index: u64,
        object_size: u64,
    ) -> Result<Buffer> {
        let key = FoyerKey::Chunk {
            path: path.to_string(),
            chunk_size: self.chunk_size,
            chunk_index,
            version: version.clone(),
        };

        // Try cache first
        if let Ok(Some(entry)) = self.inner.cache.get(&key).await {
            return Ok(entry.value().0.clone());
        }

        // Cache miss - fetch from backend
        let chunk_start = chunk_index * self.chunk_size as u64;
        let chunk_end = ((chunk_index + 1) * self.chunk_size as u64).min(object_size);
        let range = BytesRange::new(chunk_start, Some(chunk_end - chunk_start));

        let (_, mut reader) = self
            .inner
            .accessor
            .read(path, OpRead::default().with_range(range))
            .await?;
        let buffer = reader.read_all().await?;

        // Insert into cache
        self.inner
            .cache
            .insert(key, FoyerValue(buffer.clone()));

        Ok(buffer)
    }
}
