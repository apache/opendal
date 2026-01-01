- Proposal Name: `foyer_chunked`
- Start Date: 2026-01-01
- RFC PR: [apache/opendal#6370](https://github.com/apache/opendal/pull/7127)
- Tracking Issue: [apache/opendal#6372](https://github.com/apache/opendal/issues/6372)

## Summary

Introduce chunked-cache support for `FoyerLayer`.

## Motivation

In https://github.com/apache/opendal/pull/6366, we introduced the first version of `FoyerLayer`, which allows users to add hybrid caching capability (memory + disk) out of the box.

The current implementation caches the entire object as a single unit in the hybrid cache. While this approach works well for small objects and is straightforward to implement correctly, it faces limitations when dealing with large objects:

- **Memory pressure**: Caching large objects (e.g., multi-GB files) requires to serialize/deserialize the entire object from cache in a single operation, which is likely to be memory consuming.
- **Bandwidth waste**: Reading a small portion of a large cached object requires loading the entire object from disk cache into memory, wasting I/O bandwidth.
- **Cache efficiency**: Large objects have lower reuse probability compared to frequently accessed smaller chunks within them.

Many real-world workloads exhibit partial read patterns - reading specific ranges of large files rather than entire files. For example, querying Parquet files often only reads specific column chunks, and video streaming typically accesses sequential segments.

To address these issues, we propose introducing chunked cache support to FoyerLayer. By splitting large objects into fixed-size chunks and caching them independently, we can achieve better cache utilization and support efficient range reads.

## Guide-level explanation

Chunked cache mode allows `FoyerLayer` to cache large objects more efficiently by splitting them into fixed-size chunks instead of caching entire objects.

### When to use chunked cache

Use chunked cache when:
- You work with large objects (hundreds of MBs to GBs) that are rarely read entirely
- Your workload performs frequent range reads or partial reads
- You want to maximize cache hit rates for large files with limited cache capacity

Continue using whole-object cache mode when:
- Most objects are small (< 10 MB)
- Objects are typically read in full
- You prioritize simplicity over fine-grained cache control

### How it works from a user perspective

When chunked cache is enabled:

1. **Configuration**: Set the chunk size (e.g., 64 MB) when creating the `FoyerLayer`. This determines the granularity of caching.

2. **Transparent operation**: Reading works exactly as before from the API perspective. Call `read()` or `read_with()` with any range.

3. **Efficient caching**: When you read a range (e.g., bytes 100 MB - 150 MB from a file with 64 MB chunk size), the system:
   - Calculates which chunks overlap with the requested range (chunk 1: 64-128 MB, chunk 2: 128-192 MB)
   - Fetches and caches these complete chunks in the hybrid cache
   - Returns only the exact range you requested (100-150 MB) by slicing the cached chunks
   - Future reads to bytes 64-100 MB or 150-192 MB can be served entirely from cache
   - Each chunk is cached and evicted independently based on the hybrid cache's LRU policy

4. **Example**:
   ```rust
   // Enable chunked cache with 64 MB chunks
   let layer = FoyerLayer::new()
       .with_chunk_size_bytes(64 * 1024 * 1024);  // 64 MB chunks

   let op = Operator::new(S3::default())?
       .layer(layer)
       .finish();

   // Read a range from a 1 GB file
   let data = op.read_with("large_file.bin")
       .range(100_000_000..150_000_000)  // Read 50 MB starting at 100 MB
       .await?;

   // Only the chunks overlapping this range (chunk 1 and chunk 2) are cached
   // Next time you read bytes 120-180 MB, chunk 2 is already in cache
   ```

### Trade-offs

- **Pros**: Better cache utilization for large objects, supports efficient partial reads, reduced memory pressure
- **Cons**: Slightly more complex cache management, small overhead for tracking chunks & metadata.

## Reference-level explanation

This section describes the technical implementation details of chunked cache support in `FoyerLayer`. The design is based on SlateDB's `CachedObjectStore` implementation, which has proven effective in production workloads.

### Configuration

Extend `FoyerLayer` with a new optional configuration parameter:

```rust
pub struct FoyerLayer {
    cache: HybridCache<String, Bytes>,
    chunk_size_bytes: Option<usize>,  // None = whole-object mode (default)
}

impl FoyerLayer {
    pub fn with_chunk_size_bytes(mut self, size: usize) -> Self {
        // Validate chunk size is aligned with 1KB
        assert!(size > 0 && size.is_multiple_of(1024),
                "chunk_size_bytes must be > 0 and aligned to 1KB");
        self.chunk_size_bytes = Some(size);
        self
    }
}
```

When `chunk_size_bytes` is `None`, the layer operates in whole-object mode (current behavior). When set to a value, chunked cache mode is enabled.

### Cache Key Design

Chunked cache uses structured cache keys encoded with bincode for type safety and efficiency. The chunk size and object version must be included in the cache key to prevent data corruption when chunk size changes or object is updated.

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
enum CacheKey {
    /// Metadata cache entry for an object
    Metadata {
        path: String,
        chunk_size: usize,
        version: Option<String>,
    },
    /// Chunk data cache entry
    Chunk {
        path: String,
        chunk_size: usize,
        chunk_index: usize,
        version: Option<String>,
    },
}
```

**Usage examples**:

```rust
// Metadata key for "data/large.bin" with 64MB chunks and etag
let meta_key = CacheKey::Metadata {
    path: "data/large.bin".to_string(),
    chunk_size: 67108864,
    version: Some("\"abc123def\"".to_string()),  // etag from object metadata
}.to_bytes();

// Chunk key for chunk 0
let chunk_key = CacheKey::Chunk {
    path: "data/large.bin".to_string(),
    chunk_size: 67108864,
    chunk_index: 0,
    version: Some("\"abc123def\"".to_string()),
}.to_bytes();
```

### Metadata Structure

In addition to caching chunks, the implementation must cache object metadata separately. The object size is critical for determining the total number of chunks and calculating the boundary of the last chunk, which may be smaller than the configured chunk size.

Metadata lookup always precedes chunk operations. Before reading any chunks, the implementation must first retrieve the metadata to obtain the object size. This information is essential for correctly calculating chunk boundaries and ensuring cache consistency.

If metadata is not cached, the object is treated as uncached and the implementation falls back to the underlying storage.

Since OpenDAL's `Metadata` does not currently implement `Serialize/Deserialize`, we define a `CachedMetadata` wrapper that can be serialized to cache:

```rust
/// Wrapper for OpenDAL's Metadata to make it serializable for cache.
#[derive(Serialize, Deserialize)]
struct CachedMetadata {
    meta: Metadata,
}

impl From<&Metadata> for CachedMetadata {
    fn from(meta: &Metadata) -> Self {
        Self {
            meta: meta.clone(),
        }
    }
}
```

### Implementation

The read operation follows this flow:

1. **Check chunked mode**: If `chunk_size_bytes` is `None`, fallback to whole-object caching (current implementation).

2. **Prefetch with coerced range**:

   When an object is not yet cached, the implementation coerces the requested range to chunk boundaries and prefetches complete chunks in a single aligned request, and cache these chunks.

   Without prefetching, chunked cache would require separate requests for each chunk on cache miss, potentially increasing the total number of requests to the underlying storage, which might increase the cost of API calls on object stores.

   For example, if a user requests bytes 100MB-150MB with 64MB chunks configured, the implementation fetches the aligned range 64MB-192MB in one request and saves chunks 1 and 2. This consolidates what would otherwise be two separate chunk requests into a single request. Future reads to any part of these cached chunks will hit the cache without additional requests.

   ```rust
   async fn maybe_prefetch_range(
       &self,
       path: &str,
       version: Option<String>,
       range: Option<Range<u64>>,
   ) -> Result<CachedMetadata> {
       let chunk_size = self.chunk_size_bytes.unwrap();

       // First, try to get cached metadata
       let meta_key = CacheKey::Metadata {
           path: path.to_string(),
           chunk_size,
           version: version.clone(),
       }.to_bytes();

       if let Some(cached_meta) = self.cache.get(&meta_key).await {
           return Ok(cached_meta);
       }

       // Align the range to chunk boundaries for efficient prefetching
       let aligned_range = range.map(|r| self.align_range(&r));

       // Fetch from underlying storage with aligned range
       // This fetches MORE data than requested, but aligned to chunk boundaries
       // Example: User requests 100-150MB, we fetch 64-192MB (chunks 1,2)
       let (rp, mut reader) = self.inner.read(path, aligned_range).await?;

       // Convert OpenDAL's Metadata to CachedMetadata for cache storage
       let metadata = CachedMetadata::from(rp.metadata());
       self.cache.insert(meta_key, metadata).await;

       // Stream data and save chunks (with version)
       self.save_chunks_from_stream(path, reader, aligned_range.start, version).await?;

       Ok(metadata)
   }

   fn align_range(&self, range: &Range<u64>) -> Range<u64> {
       let chunk_size = self.chunk_size_bytes.unwrap() as u64;
       let start_aligned = range.start - (range.start % chunk_size);
       let end_aligned = range.end.div_ceil(chunk_size) * chunk_size;
       start_aligned..end_aligned
   }
   ```

3. **Split range into chunks**:

   After obtaining the object size from metadata in the prefetch phase, the implementation splits the requested range into chunks. For each chunk, it calculates the exact range needed within that chunk, accounting for potential partial ranges at the beginning and end.

   ```rust
   fn split_range_into_chunks(
       &self,
       range: Range<u64>,
       object_size: u64,
   ) -> Vec<(ChunkIndex, Range<usize>)> {
       let chunk_size = self.chunk_size_bytes.unwrap() as u64;
       let range_aligned = self.align_range(&range);

       let start_chunk = (range_aligned.start / chunk_size) as usize;
       let end_chunk = (range_aligned.end / chunk_size) as usize;

       let mut chunks: Vec<_> = (start_chunk..end_chunk)
           .map(|chunk_idx| (chunk_idx, 0..self.chunk_size_bytes.unwrap()))
           .collect();

       // Adjust first chunk's start offset
       if let Some(first) = chunks.first_mut() {
           first.1.start = (range.start % chunk_size) as usize;
       }

       // Adjust last chunk's end offset (handle unaligned end)
       if let Some(last) = chunks.last_mut() {
           if range.end % chunk_size != 0 {
               last.1.end = (range.end % chunk_size) as usize;
           }
           // Handle last chunk of object being smaller than chunk_size
           let chunk_global_end = ((last.0 + 1) as u64 * chunk_size).min(object_size);
           let actual_chunk_size = (chunk_global_end - last.0 as u64 * chunk_size) as usize;
           last.1.end = last.1.end.min(actual_chunk_size);
       }

       chunks
   }
   ```

4. **Read chunks individually**:
   ```rust
   async fn read_chunk(
       &self,
       path: &str,
       chunk_idx: usize,
       range_in_chunk: Range<usize>,
       version: Option<String>,  // Object version from metadata
   ) -> Result<Bytes> {
       let chunk_size = self.chunk_size_bytes.unwrap();
       let chunk_key = CacheKey::Chunk {
           path: path.to_string(),
           chunk_size,
           chunk_index: chunk_idx,
           version: version.clone(),
       }.to_bytes();

       // Try cache first
       if let Some(cached_chunk) = self.cache.get(&chunk_key).await {
           return Ok(cached_chunk.slice(range_in_chunk));
       }

       // Cache miss - fetch entire chunk from underlying storage
       let chunk_range = Range {
           start: chunk_idx as u64 * chunk_size as u64,
           end: (chunk_idx + 1) as u64 * chunk_size as u64,
       };

       let (_, mut reader) = self.inner.read(path, Some(chunk_range)).await?;
       let chunk_data = reader.read_all().await?;

       // Save to cache (best-effort, ignore errors)
       self.cache.insert(chunk_key, chunk_data.clone()).await.ok();

       // Return the requested range
       Ok(chunk_data.slice(range_in_chunk))
   }
   ```

5. **Assemble result as stream**:
   - Return chunks as a `Stream<Item = Result<Bytes>>` rather than buffering all data
   - Each chunk is fetched lazily when the stream is polled
   - This reduces memory pressure and allows streaming large ranges efficiently

### Key Design Considerations

1. **Range alignment strategy**

   When metadata is not yet cached, the implementation aligns the requested range to chunk boundaries before fetching from the underlying storage. For example, if a user requests bytes 100-150MB with 64MB chunks configured, the system will fetch the aligned range of 64-192MB.

   While this fetches more data initially, it significantly reduces the number of requests to the underlying storage by consolidating multiple chunk fetches into a single aligned request. This trade-off proves beneficial as it populates the cache more efficiently and reduces overall latency.

   The alignment is only applied on the first fetch (cache miss). Subsequent reads can directly use the cached chunks without additional alignment overhead.

2. **Streaming result**

   The implementation returns data as a stream where each chunk is fetched lazily when consumed.

   This approach is critical for memory efficiency when reading large ranges that span many chunks. Without streaming, reading a multi-gigabyte range would require loading all chunks into memory simultaneously, potentially exhausting available memory and causing performance degradation.

3. **Best-effort cache operations**

   All cache operations (insert, remove, get) are designed to never fail the user's read or write operation.

   If a cache operation encounters an error, the implementation logs a warning and continues by falling back to the underlying storage. This ensures that the cache layer remains truly transparent to users.

### Edge Cases and Considerations

**Last chunk handling**

The last chunk of an object may be smaller than the configured chunk size and requires special attention. The implementation calculates the actual chunk size using the formula `min((chunk_idx + 1) * chunk_size, object_size) - chunk_idx * chunk_size`.

For example, a 200 MB file with 64 MB chunks would be split into chunks 0, 1, and 2 of 64MB each, followed by chunk 3 containing only 8MB.

**Empty or invalid range requests**

Range requests are handled according to OpenDAL's existing semantics:
- Empty range: Returns empty result without performing any cache operations
- Range start beyond object size: Returns error to match OpenDAL's behavior
- Range end exceeds object size: Clamped to the actual object size, allowing partial reads near the end of objects

**Concurrent access**

Concurrent access patterns benefit from Foyer's built-in request deduplication mechanism. When multiple concurrent reads request the same chunk, Foyer ensures that only one fetch actually occurs from the underlying storage, while other readers wait and reuse the result.

This deduplication happens transparently within the Foyer cache layer, requiring no additional locking or coordination logic in FoyerLayer itself.

**Cache consistency**

**Important**: The cache is designed with the assumption that objects in the underlying storage are immutable. This means the cache expects that once an object is written, its content at a given path and version will not change.

When an update or delete operation occurs, the implementation attempts to invalidate the affected metadata and chunks from the cache. However, there are scenarios where stale cache entries may temporarily persist:

- Concurrent writes from different processes or clients cannot be detected
- Cache invalidation failures are silently ignored to maintain transparency

This design is suitable for typical object storage workloads where objects are write-once-read-many. **Applications that frequently modify objects at the same path should carefully evaluate whether using this feature is appropriate.** Those requiring strong consistency guarantees should either disable caching or implement application-level cache invalidation mechanisms.

### Testing Strategy

1. **Unit tests**

   Focus on the core algorithms with various test cases:
   - `split_range_into_chunks` with different combinations of ranges and object sizes to verify correct chunk boundary calculations
   - `align_range` with aligned ranges, unaligned ranges, and boundary conditions to ensure all edge cases are handled correctly
   - Last chunk handling when it's smaller than chunk_size
   - Empty and invalid range scenarios

2. **Integration tests**

   Validate end-to-end behavior of the chunked cache system:
   - Cache hit and miss scenarios to ensure prefetching and caching logic works correctly
   - Concurrent reads to the same chunk to verify Foyer's request deduplication
   - Write invalidation behavior to confirm cached data is properly invalidated when objects are modified
   - Mixed workloads using both whole-object mode and chunked mode

3. **Behavior tests**

   Leverage OpenDAL's existing behavior test suite, which provides comprehensive coverage across different backends.

   Add chunked cache specific scenarios:
   - Large files with range reads to validate performance characteristics
   - Sequential read patterns to verify prefetching efficiency
   - Random access patterns to ensure proper handling of non-sequential workloads

### Compatibility and Migration

**Backward compatibility**

The chunked cache feature is fully backward compatible with existing FoyerLayer usage. The implementation defaults to `chunk_size_bytes = None`, which activates whole-object mode matching the current behavior. This means existing users are completely unaffected by the introduction of chunked caching.

**Opt-in design**

Chunked cache is an opt-in feature that users must explicitly enable through configuration by setting the chunk size. This conservative approach ensures that users who haven't evaluated whether chunked caching benefits their workload will continue to use the proven whole-object caching strategy.

**Cache key migration**

The cache key format changes between whole-object and chunked modes, but this requires no special migration handling. Since whole-object cache uses different keys than chunked cache, and different chunk sizes use different keys from each other, old cache entries simply coexist harmlessly with new ones.

As the LRU eviction policy runs, old entries naturally expire and are replaced with new entries in the current format. This natural invalidation is acceptable because the cache is ephemeral by design, storing temporary performance-optimization data rather than durable state.