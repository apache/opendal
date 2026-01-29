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

/// Cached metadata for an object in chunked mode.
///
/// This stores essential information needed for chunk-based reading:
/// - `content_length`: Total size of the object
/// - `version`: Object version (if versioning is enabled)
/// - `etag`: Entity tag for cache validation
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CachedMetadata {
    content_length: u64,
    version: Option<String>,
    etag: Option<String>,
}

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

/// Information about a chunk needed to satisfy a read request.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ChunkInfo {
    /// The index of this chunk (0-based)
    chunk_index: u64,
    /// Offset within this chunk where the requested data starts
    offset_in_chunk: usize,
    /// Length of data to read from this chunk
    length_in_chunk: usize,
}

/// Align a range to chunk boundaries.
///
/// Returns the aligned (start, end) positions that cover the original range.
/// The end position is exclusive.
///
/// # Arguments
/// * `range_start` - Start of the requested range
/// * `range_end` - End of the requested range (exclusive)
/// * `chunk_size` - Size of each chunk
/// * `object_size` - Total size of the object
///
/// # Returns
/// (aligned_start, aligned_end) - Both aligned to chunk boundaries, clamped to object_size
#[allow(dead_code)]
fn align_range(range_start: u64, range_end: u64, chunk_size: usize, object_size: u64) -> (u64, u64) {
    let chunk_size = chunk_size as u64;

    // Align start down to chunk boundary
    let aligned_start = (range_start / chunk_size) * chunk_size;

    // Align end up to chunk boundary, but don't exceed object size
    let aligned_end = if range_end == 0 {
        0
    } else {
        let chunks_needed = range_end.div_ceil(chunk_size);
        (chunks_needed * chunk_size).min(object_size)
    };

    (aligned_start, aligned_end)
}

/// Split a range into individual chunk read operations.
///
/// # Arguments
/// * `range_start` - Start of the requested range (inclusive)
/// * `range_size` - Size of the requested range, or None for "read to end"
/// * `chunk_size` - Size of each chunk
/// * `object_size` - Total size of the object
///
/// # Returns
/// A vector of `ChunkInfo` describing each chunk that needs to be fetched
/// and how to slice the data from each chunk.
fn split_range_into_chunks(
    range_start: u64,
    range_size: Option<u64>,
    chunk_size: usize,
    object_size: u64,
) -> Vec<ChunkInfo> {
    if object_size == 0 {
        return vec![];
    }

    let chunk_size_u64 = chunk_size as u64;
    let range_end = match range_size {
        Some(size) => (range_start + size).min(object_size),
        None => object_size,
    };

    if range_start >= range_end {
        return vec![];
    }

    let first_chunk_index = range_start / chunk_size_u64;
    let last_chunk_index = (range_end - 1) / chunk_size_u64;

    let mut chunks = Vec::with_capacity((last_chunk_index - first_chunk_index + 1) as usize);

    for chunk_index in first_chunk_index..=last_chunk_index {
        let chunk_start = chunk_index * chunk_size_u64;
        let chunk_end = ((chunk_index + 1) * chunk_size_u64).min(object_size);

        // Calculate the intersection of [range_start, range_end) with [chunk_start, chunk_end)
        let read_start = range_start.max(chunk_start);
        let read_end = range_end.min(chunk_end);

        if read_start < read_end {
            chunks.push(ChunkInfo {
                chunk_index,
                offset_in_chunk: (read_start - chunk_start) as usize,
                length_in_chunk: (read_end - read_start) as usize,
            });
        }
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_range_basic() {
        // Range within a single chunk
        let (start, end) = align_range(100, 200, 1024, 4096);
        assert_eq!(start, 0);
        assert_eq!(end, 1024);

        // Range spanning multiple chunks
        let (start, end) = align_range(1000, 3000, 1024, 4096);
        assert_eq!(start, 0);
        assert_eq!(end, 3072);
    }

    #[test]
    fn test_align_range_at_boundaries() {
        // Start at chunk boundary
        let (start, end) = align_range(1024, 2000, 1024, 4096);
        assert_eq!(start, 1024);
        assert_eq!(end, 2048);

        // End at chunk boundary
        let (start, end) = align_range(100, 2048, 1024, 4096);
        assert_eq!(start, 0);
        assert_eq!(end, 2048);

        // Both at boundaries
        let (start, end) = align_range(1024, 2048, 1024, 4096);
        assert_eq!(start, 1024);
        assert_eq!(end, 2048);
    }

    #[test]
    fn test_align_range_clamp_to_object_size() {
        // Range extends beyond object
        let (start, end) = align_range(3000, 5000, 1024, 4096);
        assert_eq!(start, 2048);
        assert_eq!(end, 4096);

        // Object smaller than chunk
        let (start, end) = align_range(0, 500, 1024, 500);
        assert_eq!(start, 0);
        assert_eq!(end, 500);
    }

    #[test]
    fn test_split_range_single_chunk() {
        // Read within a single chunk
        let chunks = split_range_into_chunks(100, Some(200), 1024, 4096);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].offset_in_chunk, 100);
        assert_eq!(chunks[0].length_in_chunk, 200);
    }

    #[test]
    fn test_split_range_multiple_chunks() {
        // Read spanning 3 chunks
        let chunks = split_range_into_chunks(500, Some(2000), 1024, 4096);
        assert_eq!(chunks.len(), 3);

        // First chunk: 500-1024 (524 bytes)
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].offset_in_chunk, 500);
        assert_eq!(chunks[0].length_in_chunk, 524);

        // Second chunk: 1024-2048 (1024 bytes)
        assert_eq!(chunks[1].chunk_index, 1);
        assert_eq!(chunks[1].offset_in_chunk, 0);
        assert_eq!(chunks[1].length_in_chunk, 1024);

        // Third chunk: 2048-2500 (452 bytes)
        assert_eq!(chunks[2].chunk_index, 2);
        assert_eq!(chunks[2].offset_in_chunk, 0);
        assert_eq!(chunks[2].length_in_chunk, 452);
    }

    #[test]
    fn test_split_range_read_to_end() {
        // Read from offset to end of object
        let chunks = split_range_into_chunks(1500, None, 1024, 3000);
        assert_eq!(chunks.len(), 2);

        // First chunk: 1500-2048 (548 bytes)
        assert_eq!(chunks[0].chunk_index, 1);
        assert_eq!(chunks[0].offset_in_chunk, 476);
        assert_eq!(chunks[0].length_in_chunk, 548);

        // Second chunk: 2048-3000 (952 bytes)
        assert_eq!(chunks[1].chunk_index, 2);
        assert_eq!(chunks[1].offset_in_chunk, 0);
        assert_eq!(chunks[1].length_in_chunk, 952);
    }

    #[test]
    fn test_split_range_entire_object() {
        // Read entire object
        let chunks = split_range_into_chunks(0, None, 1024, 2500);
        assert_eq!(chunks.len(), 3);

        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].offset_in_chunk, 0);
        assert_eq!(chunks[0].length_in_chunk, 1024);

        assert_eq!(chunks[1].chunk_index, 1);
        assert_eq!(chunks[1].offset_in_chunk, 0);
        assert_eq!(chunks[1].length_in_chunk, 1024);

        assert_eq!(chunks[2].chunk_index, 2);
        assert_eq!(chunks[2].offset_in_chunk, 0);
        assert_eq!(chunks[2].length_in_chunk, 452);
    }

    #[test]
    fn test_split_range_empty_object() {
        let chunks = split_range_into_chunks(0, None, 1024, 0);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_range_empty_range() {
        // Zero-length range
        let chunks = split_range_into_chunks(100, Some(0), 1024, 4096);
        assert!(chunks.is_empty());

        // Start equals end
        let chunks = split_range_into_chunks(1024, Some(0), 1024, 4096);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_range_beyond_object() {
        // Range starts beyond object
        let chunks = split_range_into_chunks(5000, Some(100), 1024, 4096);
        assert!(chunks.is_empty());

        // Range partially beyond object
        let chunks = split_range_into_chunks(4000, Some(500), 1024, 4096);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_index, 3);
        assert_eq!(chunks[0].offset_in_chunk, 928);
        assert_eq!(chunks[0].length_in_chunk, 96);
    }

    #[test]
    fn test_split_range_exact_chunk_boundaries() {
        // Read exactly one full chunk
        let chunks = split_range_into_chunks(1024, Some(1024), 1024, 4096);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_index, 1);
        assert_eq!(chunks[0].offset_in_chunk, 0);
        assert_eq!(chunks[0].length_in_chunk, 1024);

        // Read exactly two full chunks
        let chunks = split_range_into_chunks(0, Some(2048), 1024, 4096);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].length_in_chunk, 1024);
        assert_eq!(chunks[1].length_in_chunk, 1024);
    }

    #[test]
    fn test_split_range_object_not_chunk_aligned() {
        // Object size is not a multiple of chunk size
        let chunks = split_range_into_chunks(0, None, 1024, 1500);
        assert_eq!(chunks.len(), 2);

        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].length_in_chunk, 1024);

        assert_eq!(chunks[1].chunk_index, 1);
        assert_eq!(chunks[1].offset_in_chunk, 0);
        assert_eq!(chunks[1].length_in_chunk, 476); // Last chunk is smaller
    }

    #[test]
    fn test_split_range_last_partial_chunk() {
        // Read that ends in the middle of the last partial chunk
        let chunks = split_range_into_chunks(1000, Some(400), 1024, 1500);
        assert_eq!(chunks.len(), 2);

        // First chunk: 1000-1024
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].offset_in_chunk, 1000);
        assert_eq!(chunks[0].length_in_chunk, 24);

        // Second chunk: 1024-1400
        assert_eq!(chunks[1].chunk_index, 1);
        assert_eq!(chunks[1].offset_in_chunk, 0);
        assert_eq!(chunks[1].length_in_chunk, 376);
    }
}
