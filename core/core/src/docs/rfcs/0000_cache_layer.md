- Proposal Name: `cache_layer`
- Start Date: 2025-06-16
- RFC PR: [apache/opendal#0000](https://github.com/apache/opendal/pull/0000)
- Tracking Issue: [apache/opendal#0000](https://github.com/apache/opendal/issues/0000)

# Summary

This RFC proposes the addition of a Cache Layer to OpenDAL, providing transparent read-through and write-through caching capabilities. The Cache Layer allows users to improve performance by caching data from a slower storage service (e.g., S3, HDFS) to a faster one (e.g., Memory, Redis, Moka).

# Motivation

Storage access performance varies greatly across different storage services.
Remote object stores like S3 or GCS have much higher latency than local storage or in-memory caches.
In many applications, particularly those with read-heavy workloads or repeated access to the same data, caching can significantly improve performance.

Currently, users who want to implement caching with OpenDAL must manually:

1. Check if data exists in cache storage
2. If cache miss, fetch from original storage and manually populate cache
3. Handle cache invalidation and consistency manually

By introducing a dedicated Cache Layer, we can:

- Provide a unified, transparent caching solution within OpenDAL
- Eliminate boilerplate code for common caching patterns
- Allow flexible configuration of caching policies
- Enable performance optimization with minimal code changes
- Leverage existing OpenDAL services as cache storage

# Guide-level explanation

The Cache Layer allows you to wrap any existing storage with a caching mechanism.
When data is accessed through this layer, it will automatically be cached in your specified cache storage.
The cache layer is designed to be simple and delegates cache management policies (like TTL, eviction) to the underlying cache storage service.

## Basic Usage

```rust
use opendal::{layers::CacheLayer, services::Memory, services::S3, Operator};

#[tokio::main]
async fn main() -> opendal::Result<()> {
    // Create a memory operator to use as cache
    let memory = Operator::new(Memory::default()))?;

    // Create a primary storage operator (e.g., S3)
    let s3 = Operator::new(
        S3::default()
            .bucket("my-bucket")
            .region("us-east-1")
            .build()?
        )?
        .finish();
    
    // Wrap the primary storage with the cache layer
    let op = s3.layer(CacheLayer::new(memory)).finish();
    
    // Use the operator as normal - caching is transparent
    let data = op.read("path/to/file").await?;
    
    // Subsequent reads will be served from cache if available
    let cached_data = op.read("path/to/file").await?;
    
    Ok(())
}
```

## Using Different Cache Storage Services

The Cache Layer can use any OpenDAL service as cache storage:

```rust
// Using Redis as cache
use opendal::services::Redis;

let redis_cache = Operator::new(
    Redis::default()
        .endpoint("redis://localhost:6379")
    )?
    .finish();

let op = s3.layer(CacheLayer::new(redis_cache)).finish();
```

```rust
// Using Moka (in-memory cache with advanced features)
use opendal::services::Moka;

let moka_cache = Operator::new(
        Moka::default()
            .max_capacity(1000)
            .time_to_live(Duration::from_secs(3600)) // TTL managed by Moka
    )?
    .finish();

let op = s3.layer(CacheLayer::new(moka_cache)).finish();
```

## Multiple Cache Layers

You can stack multiple cache layers for a multi-tier caching strategy:

```rust
// L1 cache: Fast in-memory cache
let l1_cache = Operator::new(Memory::default())?.finish();

// L2 cache: Larger but slightly slower cache (e.g., Redis)
let l2_cache = Operator::new(
        Redis::default().endpoint("redis://localhost:6379")
    )?
    .finish();

// Stack the caches: L1 -> L2 -> S3
let op = s3
    .layer(CacheLayer::new(l2_cache))  // L2 cache
    .layer(CacheLayer::new(l1_cache))  // L1 cache
    .finish();
```

## Configuration Options

The Cache Layer provides minimal configuration to keep it simple:

```rust
let op = s3.layer(
        CacheLayer::new(memory)
            // Enable read caching (default: true)
            .with_cache_read(true) 
            // Enable write-through caching (default: true)
            .with_cache_write(true)         
            // Enable cache promotion on read (default: true)
            .with_cache_read_promotion(true)    
    )
    .finish();
```

- `with_cache_read(bool)`: Controls whether read operations check the cache first
- `with_cache_write(bool)`: Controls whether write operations also write to cache
- `with_cache_read_promotion(bool)`: Controls whether data read from the underlying storage is promoted to the cache

# Reference-level explanation

## Architecture

The Cache Layer implements the `Layer` trait and wraps an underlying `Access` implementation with caching capabilities. It introduces a `CacheStorage` trait that defines the interface for cache operations.

### Customizable Cache Storage

```rust
pub trait CacheStorage {
    fn cache_type(&self) -> &'static str;

    // should we return `impl Future<Output = Result<Buffer>> + MaybeSend` here???
    fn get(&self, key: &str) -> impl Future<Output = Result<Option<Buffer>>> + MaybeSend;

    fn set(&self, key: &str, value: Vec<u8>) -> impl Future<Output = Result<()>> + MaybeSend;

    fn exists(&self, key: &str) -> impl Future<Output = Result<bool>> + MaybeSend;
}
```

OpenDAL `Operator` implements `CacheStorage` trait, making any service usable as cache storage.

```rust
impl CacheStorage for Operator {
    fn cache_type(&self) -> &'static str {
        self.info().scheme().into_static()
    }

    async fn get(&self, key: &str) -> Result<Option<Buffer>> {
        let r = self.read(key).await;
        match r {
            Ok(r) => Ok(Some(r)),
            Err(err) => match err.kind() {
                ErrorKind::NotFound => Ok(None),
                _ => Err(err),
            },
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.write(key, value).await.map(|_| ())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.exists(key).await
    }
}
```

### CacheLayer && CacheAccessor

The layer wraps the underlying access with `CacheAccessor`, which implements caching logic for each operation.

```rust
pub struct CacheAccessor<A, C> {
    pub(crate) inner: A,
    pub(crate) cache: Arc<C>,
    pub(crate) cache_read: bool,
    pub(crate) cache_read_promotion: bool,
    pub(crate) cache_write: bool,
}

impl<A, C> LayeredAccess for CacheAccessor<A, C>
where
    A: Access,
    C: CacheStorage + Send + Sync + 'static,
{
    type Inner = A;
    type Reader = CacheReader<A::Reader, C>;
    type Writer = CacheWriter<A::Writer, C>;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let cache_key = path.to_string();

        // If `cache_read` is enabled, check cache first using `cache.get(key)`
        if self.cache_read {
            match self.cache.get(&cache_key).await {
                Ok(Some(cached_data)) => {
                    // On cache hit, return cached data immediately
                    return Ok((RpRead::new(), CacheReader::Cached { data: cached_data, pos: 0 }));
                }
                Ok(None) => { /* On cache miss, continue to underlying storage */ }
                Err(_) => { /* On cache error, continue to underlying storage */ }
            }
        }

        // Read from underlying storage
        let (rp, reader) = self.inner.read(path, args).await?;

        Ok((
            rp,
            CacheReader::Uncached {
                inner: reader,
                cache: self.cache.clone(),
                cache_key,
                cache_read_promotion: self.cache_read_promotion,
                buffer: Vec::new(),
            },
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let cache_key = path.to_string();

        // Always write to underlying storage first to ensure data persistence
        let (rp, writer) = self.inner.write(path, args).await?;

        // If `cache_write` is enabled, also cache the written data
        Ok((
            rp,
            CacheWriter::new(writer, self.cache.clone(), cache_key, self.cache_write),
        ))
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let cache_key = path.to_string();

        // If `cache_read` is enabled, check if the file exists in cache using `cache.exists(key)`
        if self.cache_read {
            match self.cache.exists(&cache_key).await {
                Ok(true) => {
                    // TODO: maybe we need to cache metadata as well ???
                    // On cache hit - file exists in cache, return a basic metadata ???
                    let mut metadata = Metadata::new(EntryMode::FILE);
                    metadata.set_content_length(0);
                    return Ok(RpStat::new(metadata));
                }
                Ok(false) => { /* On cache miss, continue to underlying storage */ }
                Err(_) => { /* On cache error, continue to underlying storage */ }
            }
        }

        // Fall back to underlying storage
        self.inner.stat(path, args).await
    }

    // Do we need to cache copy/rename operations?

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        // For deletion operations, we should avoid performing any operations on cache storage 
        // at the layer level.
        // Instead, we should allow the cache storage to manage these operations itself, 
        // such as expiring cache the through its own TTL (Time-To-Live) policy.
        self.inner.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        // For list operations, we typically don't cache results
        // as they can be large and change frequently
        self.inner.list(path, args).await
    }
}
```

### CacheReader

The `CacheReader` handles the complexity of reading data either from cache or underlying storage, with optional cache promotion:

```rust
/// Reader that caches data as it reads from the underlying storage
pub enum CacheReader<R, C> {
    /// Reader backed by cached data
    Cached { data: Buffer, pos: usize },
    /// Reader that reads from underlying storage and caches the data
    Uncached {
        inner: R,
        cache: Arc<C>,
        cache_key: String,
        cache_read_promotion: bool,
        buffer: BytesMut,
    },
}
```

#### Cache Hit Path

- The reader is created with the cached `Buffer` and starts at position 0
- Subsequent `read()` calls return chunks of the cached data
- No network I/O or underlying storage access is needed
- Reading is completed entirely from cache

#### Cache Miss Path

- The reader wraps the underlying storage reader
- Each `read()` call forwards to the underlying reader
- If `cache_read_promotion` is enabled, data is accumulated in an internal `buffer`
- When reading is complete (EOF reached), the accumulated data is stored in cache for future reads
- This ensures that subsequent reads of the same file will be cache hits

### CacheWriter

The `CacheWriter` handles write-through caching by writing to both the underlying storage and cache:

```rust
/// Writer that caches data as it writes to the underlying storage
pub struct CacheWriter<W, C> {
    inner: W,
    cache: Arc<C>,
    cache_key: String,
    cache_write: bool,
    buffer: BytesMut,
}
```

1. **Buffer Accumulation**: All written data is accumulated in an internal `buffer`
2. **Primary Write**: Data is always written to the underlying storage first via `inner.write()`
3. **Cache Write**: If `cache_write` is enabled and the write to underlying storage succeeds, the complete data is written to cache
4. **Atomic Caching**: Cache operations happen only after successful completion to ensure cache consistency

### Error Handling

Cache operations are designed to be transparent and non-blocking:

- Cache errors don't fail the underlying operation
- Cache misses fall back to underlying storage
- Write operations succeed even if caching fails

### Key Generation

Cache keys are generated from the file path.
The current implementation uses the path directly as the cache key, which works well for most use cases.
Future improvements could include:

- Key prefixing for namespace isolation
- Hashing for very long paths
- Custom key generation strategies

# Drawbacks

1. **Cache Consistency**: The layer doesn't provide strong consistency guarantees between cache and underlying storage. External modifications to the underlying storage won't automatically invalidate the cache.

2. **Limited Metadata Caching**: The layer doesn't cache full metadata, which means `stat` operations may not benefit as much from caching.

3. **No Cache Invalidation API**: There's no built-in way to explicitly invalidate cache entries, relying entirely on the underlying cache storage's policies.

# Rationale and alternatives

## Why This Design?

1. **Simplicity**: By delegating cache policies to the underlying storage service, the Cache Layer remains simple and focused.

2. **Flexibility**: Users can choose any OpenDAL service as cache storage, allowing them to pick the right tool for their use case.

3. **Composability**: The layer design allows stacking multiple cache layers for complex caching strategies.

4. **Transparency**: The caching is completely transparent to the application code.

## Alternative Designs Considered

### Built-in Cache Policies

This was rejected because:

- It would duplicate functionality already available in specialized cache services
- It would make the layer more complex and harder to maintain
- It would limit flexibility for users who need specific cache behaviors

### Metadata Caching

Not sure if caching metadata is necessary for the initial version, as it would complicate the design and add overhead. We can consider it in a future RFC if needed.

# Prior art

None

# Unresolved questions

1. **Metadata Caching**: Should we cache more comprehensive metadata, and if so, how do we handle metadata format differences across services?

2. **Cache Statistics**: Should the layer provide built-in metrics for cache hit/miss rates and performance monitoring?

3. **Cache Key Strategy**: Should we provide options for custom cache key generation (e.g., hashing, prefixing)?

4. **Invalidation API**: Should we provide explicit cache invalidation methods, or rely entirely on the underlying cache storage?

# Future possibilities

- Customizable Cache Key Generation:
  - Options for hashing, prefixing, or other strategies
- Cache Statistics and Monitoring:
  - Built-in metrics for cache performance (hit/miss rates, latency)
