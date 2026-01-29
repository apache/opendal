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

/// Information about a chunk needed to satisfy a read request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkInfo {
    /// The index of this chunk (0-based)
    pub chunk_index: u64,
    /// Offset within this chunk where the requested data starts
    pub offset_in_chunk: usize,
    /// Length of data to read from this chunk
    pub length_in_chunk: usize,
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
pub fn align_range(range_start: u64, range_end: u64, chunk_size: usize, object_size: u64) -> (u64, u64) {
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
pub fn split_range_into_chunks(
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
