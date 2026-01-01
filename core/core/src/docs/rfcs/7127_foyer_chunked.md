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