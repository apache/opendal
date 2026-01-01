- Proposal Name: `foyer_chunked`
- Start Date: 2026-01-01
- RFC PR: [apache/opendal#6370](https://github.com/apache/opendal/pull/7127)
- Tracking Issue: [apache/opendal#6372](https://github.com/apache/opendal/issues/6372)

## Summary

Introduce chunked-cache support for `FoyerLayer`.

## Motivation

In https://github.com/apache/opendal/pull/6366, we introduced the first version of `FoyerLayer`, which allows users to add hybrid caching capability (memory + disk) out of the box.

The current implementation caches the entire object as a single unit in the hybrid cache. While this approach works well for small objects and is straightforward to implement correctly, it faces limitations when dealing with large objects:

- **Memory pressure**: Caching large objects (e.g., multi-GB files) as a whole can quickly exhaust the in-memory cache, causing frequent evictions and poor cache hit rates.
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

Beside the cache key, we also need to cache the metadata of the object. Since tbd

```rust
#[derive(Serialize, Deserialize)]
struct ObjectMetadata {
    size: u64,
    etag: Option<String>,
    last_modified: Option<SystemTime>,
}
```

### Read Operation Implementation

The read operation follows this flow (inspired by SlateDB's design):

1. **Check chunked mode**: If `chunk_size_bytes` is `None`, fallback to whole-object caching (current implementation).

2. **Prefetch with aligned range** (key optimization):
   ```rust
   async fn maybe_prefetch_range(
       &self,
       path: &str,
       range: Option<Range<u64>>,
   ) -> Result<ObjectMetadata> {
       let chunk_size = self.chunk_size_bytes.unwrap();

       // First, try to get cached metadata to obtain version
       // We try with version=None first since we don't know the version yet
       let meta_key_without_version = CacheKey::Metadata {
           path: path.to_string(),
           chunk_size,
           version: None,
       }.to_bytes();

       if let Some(cached_meta) = self.cache.get(&meta_key_without_version).await {
           return deserialize_metadata(cached_meta);
       }

       // Align the range to chunk boundaries for efficient prefetching
       let aligned_range = range.map(|r| self.align_range(&r));

       // Fetch from underlying storage with aligned range
       // This fetches MORE data than requested, but aligned to chunk boundaries
       // Example: User requests 100-150MB, we fetch 64-192MB (chunks 1,2)
       let (rp, mut reader) = self.inner.read(path, aligned_range).await?;

       // Extract version (etag) from response
       let version = rp.metadata().etag().map(String::from);

       // Save metadata with version
       let metadata = ObjectMetadata {
           size: rp.metadata().content_length(),
           etag: version.clone(),
           last_modified: rp.metadata().last_modified(),
       };

       let meta_key = CacheKey::Metadata {
           path: path.to_string(),
           chunk_size,
           version: version.clone(),
       }.to_bytes();

       self.cache.insert(meta_key, serialize_metadata(&metadata)?).await;

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

   **Why alignment matters**: When object is not yet cached, aligning the range allows us to fetch complete chunks in a single request. For example, if user requests bytes 100MB-150MB with 64MB chunks, we fetch 64MB-192MB in one request and save chunks 1 and 2. Future reads to any part of chunks 1 or 2 will hit cache.

   **Version handling**: The version (etag) is obtained from the read response and included in all cache keys. This ensures that when an object is updated (etag changes), old cached chunks won't be used.

3. **Split range into chunks**:
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

       Ok(chunk_data.slice(range_in_chunk))
   }
   ```

5. **Assemble result as stream**:
   - Return chunks as a `Stream<Item = Result<Bytes>>` rather than buffering all data
   - Each chunk is fetched lazily when the stream is polled
   - This reduces memory pressure and allows streaming large ranges efficiently

### Write Operation Implementation

Following SlateDB's pattern, write operations can optionally cache the written data:

```rust
async fn write(&self, path: &str, args: OpWrite) -> Result<RpWrite> {
    // Write to underlying storage first
    let result = self.inner.write(path, args.clone()).await?;

    // Optionally cache the written data (can be controlled by a flag)
    if self.cache_writes {
        // Fetch metadata via stat
        if let Ok(meta) = self.inner.stat(path).await {
            let metadata = ObjectMetadata::from(meta);
            let meta_key = format!("{}#meta", path);
            self.cache.insert(meta_key, serialize_metadata(&metadata)?).await;

            // Stream the written data into chunks
            // Note: This requires buffering the write payload, which may not be desirable
            // For now, we can skip caching write data and only cache on subsequent reads
        }
    }

    Ok(result)
}
```

**Write caching strategy**:
- **Simple approach**: Only invalidate metadata, don't cache write data
  - Remove `{path}#meta` from cache
  - Let chunks naturally expire via LRU
  - Subsequent reads will populate cache
- **Aggressive approach**: Cache written data if enabled
  - Useful for write-then-read patterns
  - Requires access to write payload (may need buffering)
  - Can be controlled via `cache_writes` flag (similar to SlateDB)

### Delete Operation Implementation

When a delete completes:

```rust
async fn delete(&self, path: &str) -> Result<RpDelete> {
    let result = self.inner.delete(path).await?;

    // Best-effort cache invalidation
    // Remove metadata (chunks will be evicted naturally)
    let meta_key = format!("{}#meta", path);
    self.cache.remove(&meta_key).await.ok();

    // Optionally: If metadata is in cache, calculate and remove all chunks
    // This is more thorough but requires additional cache lookup

    Ok(result)
}
```

**Rationale**: Lazy chunk removal is acceptable because:
- Cached chunks for deleted objects are harmless (worst case: wasted cache space)
- They'll be evicted naturally by LRU when cache pressure increases
- Scanning cache for all chunks is expensive and not worth the cost

### Key Design Decisions

**Range alignment strategy**:
- When metadata is not cached, align the requested range to chunk boundaries before fetching
- Example: Request 100-150MB with 64MB chunks → fetch aligned 64-192MB
- **Trade-off**: Fetches more data initially, but populates cache more efficiently
- **Benefit**: Reduces number of requests to underlying storage (one aligned request vs. multiple chunk requests)
- Only apply alignment on first fetch (cache miss); subsequent reads use cached chunks

**Streaming instead of buffering**:
- Return data as a stream rather than loading all chunks into memory
- Each chunk is fetched lazily when consumed
- Matches OpenDAL's streaming API design
- Critical for memory efficiency when reading large ranges

**Chunk size validation**:
- Require chunk size to be aligned to 1KB (similar to SlateDB)
- Prevents edge cases with very small or misaligned chunks
- Recommended range: 16MB - 128MB

**Cache operation error handling**:
- All cache operations (insert, remove, get) should be best-effort
- Cache failures should NOT fail the user's read/write operation
- Log warnings for cache errors but continue with fallback to underlying storage
- This ensures cache is truly transparent to users

### Edge Cases and Considerations

**Last chunk handling**:
- The last chunk may be smaller than `chunk_size_bytes`
- Calculate actual chunk size: `min((chunk_idx + 1) * chunk_size, object_size) - chunk_idx * chunk_size`
- Example: 200 MB file with 64 MB chunks → chunks 0, 1, 2 (64MB each), chunk 3 (8MB)
- Already handled in `split_range_into_chunks` logic above

**Empty or invalid range requests**:
- Empty range: Return empty result without cache operations
- Start beyond object size: Return error (per OpenDAL semantics)
- End beyond object size: Clamp end to object size

**Concurrent access**:
- Foyer's built-in request deduplication handles concurrent reads to the same chunk
- Multiple concurrent reads to chunk N will result in only one fetch from underlying storage
- Other readers wait and reuse the result
- No additional locking needed in FoyerLayer

**Cache consistency**:
- Cache follows eventual consistency model (same as OpenDAL)
- No distributed coordination for concurrent writes from different processes
- Cache invalidation on write/delete is best-effort
- Acceptable for object storage workloads (most are read-heavy, immutable objects)

### Performance Characteristics

**Benefits of aligned prefetching**:
- **Fewer requests**: One aligned request instead of N chunk requests on cache miss
  - Example: Request 100-150MB → 1 aligned fetch (64-192MB) vs. 2 separate chunk fetches
- **Better locality**: Neighboring chunks are likely to be accessed together
- **Reduced latency**: Fewer round-trips to underlying storage

**Memory efficiency**:
- Metadata overhead: ~100-200 bytes per object
- Chunk data follows normal LRU eviction
- Streaming API avoids buffering large ranges in memory
- Each chunk is independently evictable

**Cache hit rate analysis**:
- **Partial reads**: Significantly improved hit rate
  - Chunks are smaller units, higher reuse probability
  - Example: Reading different columns of a Parquet file reuses row group chunks
- **Whole-object reads**: Slightly lower hit rate due to fragmentation
  - Requires all chunks to be cached vs. one whole-object entry
  - Trade-off is acceptable given target workload (partial reads)

### Testing Strategy

**Unit tests**:
- `split_range_into_chunks` with various ranges and object sizes
- `align_range` edge cases (aligned, unaligned, boundary conditions)
- Last chunk handling (smaller than chunk_size)
- Empty and invalid ranges

**Integration tests**:
- End-to-end read with cache hit and miss
- Concurrent reads to same chunk (verify deduplication)
- Write invalidation behavior
- Mixed whole-object and chunked reads

**Behavior tests**:
- Use existing OpenDAL behavior test suite
- Add chunked cache specific scenarios:
  - Large file with range reads
  - Sequential read patterns
  - Random access patterns

### Compatibility and Migration

- **Backward compatible**: Defaults to `chunk_size_bytes = None` (whole-object mode)
- **No breaking changes**: Existing users unaffected
- **Opt-in**: Users explicitly enable chunked mode via configuration
- **Cache format change**: Whole-object cache and chunked cache use different key formats
  - No automatic migration needed (cache rebuilds naturally)
  - Changing chunk size also invalidates cache (keys change)
  - This is acceptable since cache is ephemeral