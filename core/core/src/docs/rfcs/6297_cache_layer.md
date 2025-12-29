- Proposal Name: `cache_layer`
- Start Date: 2025-06-16
- RFC PR: [apache/opendal#6297](https://github.com/apache/opendal/pull/6297)
- Tracking Issue: [apache/opendal#7107](https://github.com/apache/opendal/issues/7107)

# Summary

This RFC proposes the addition of a Cache Layer to OpenDAL, providing transparent read-through and write-through caching capabilities. The Cache Layer allows users to improve performance by caching data from a slower storage service (e.g., S3, HDFS) to a faster one (e.g., Memory, Moka, Redis).

# Motivation

Storage access performance varies greatly across different storage services.
Remote object stores like S3 or GCS have much higher latency than local storage or in-memory caches.
In many applications, particularly those with read-heavy workloads or repeated access to the same data, caching can significantly improve performance.

Currently, users who want to implement caching with OpenDAL must manually:

1. Check if data exists in cache service
2. If cache misses, fetch from original storage and manually populate cache
3. Handle cache invalidation and consistency manually

By introducing a dedicated Cache Layer, we can:

- Provide a unified, transparent caching solution within OpenDAL
- Eliminate boilerplate code for common caching patterns
- Allow flexible configuration of caching policies
- Enable performance optimization with minimal code changes
- Leverage existing OpenDAL services as cache storage

# Guide-level explanation

The Cache Layer allows you to wrap any existing service with a caching mechanism.
When data is accessed through this layer, it will automatically be cached in your specified cache service.
The cache layer is designed to be straightforward, and delegates cache management policies (like TTL, eviction policy) to the underlying cache service.

## Basic Usage

```rust
use opendal::{layers::CacheLayer, services::Memory, services::S3, Operator};

#[tokio::main]
async fn main() -> opendal::Result<()> {
    // Create a memory operator to use as cache
    let memory = Operator::new(Memory::default())?;

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

    // Later reads will be served from cache if available
    let cached_data = op.read("path/to/file").await?;

    Ok(())
}
```

## Using Different Cache Services

The Cache Layer can use any OpenDAL service as cache service:

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
        .with_options(CacheOptions {
            // Enable read-through caching (default: true)
            read: true,
            // Enable cache promotion during read operations (default: true)
            read_promotion: true,
            // Enable write-through caching (default: true)
            write: true,
        })
    )
    .finish();
```

- `read` option: When disabled, reads bypass the cache entirely.
- `read_promotion` option: When disabled, data fetched from the inner service on a miss will not be stored back into the cache.
- `write` option: When enabled, bytes written to the inner service are also stored into the cache.

# Reference-level explanation

## Architecture

The Cache Layer implements the `Layer` trait and wraps an underlying `Access` implementation with caching capabilities.
It introduces a `CacheService` trait that defines the interface for cache operations.

### Customizable Cache Storage

```rust
/// `CacheService` defines the backing storage interface for [`CacheLayer`].
/// It should behave like a simple object store: get/set bytes by key and
/// expose lightweight metadata for existence checks.
pub trait CacheService: Clone + Send + Sync + 'static {
    /// Identifier of the cache backend, used mainly for logging and debugging.
    fn scheme(&self) -> &'static str;

    /// Read cached content by `key`. Returns `Ok(None)` on cache miss instead of `NotFound`.
    fn read(&self, key: &str) -> impl Future<Output = Result<Option<Buffer>>> + MaybeSend;

    /// Write full bytes for `key`, replacing any existing value.
    fn write(&self, key: &str, value: Vec<u8>) -> impl Future<Output = Result<()>> + MaybeSend;

    /// Fetch metadata for `key`. Should return [`ErrorKind::NotFound`] on miss.
    fn stat(&self, key: &str) -> impl Future<Output = Result<Metadata>> + MaybeSend;

    /// Check whether `key` exists in the cache.
    fn exists(&self, key: &str) -> impl Future<Output = Result<bool>> + MaybeSend;
}
```

OpenDAL `Operator` implements `CacheService` trait, making any OpenDAL service usable as a cache service.

```rust
impl CacheService for Operator {
    fn scheme(&self) -> &'static str {
        self.info().scheme()
    }

    async fn read(&self, key: &str) -> Result<Option<Buffer>> {
        let r = Operator::read(self, key).await;
        match r {
            Ok(r) => Ok(Some(r)),
            Err(err) => match err.kind() {
                ErrorKind::NotFound => Ok(None),
                _ => Err(err),
            },
        }
    }

    async fn write(&self, key: &str, value: Vec<u8>) -> Result<()> {
        Operator::write(self, key, value).await.map(|_| ())
    }

    async fn stat(&self, key: &str) -> Result<Metadata> {
        Operator::stat(self, key).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Operator::exists(self, key).await
    }
}
```

### CacheLayer && CacheAccessor

The layer wraps the underlying access with `CacheAccessor`, which implements caching logic for each operation.

```rust
#[derive(Clone, Copy, Debug)]
pub struct CacheOptions {
    /// Enable cache lookups before hitting the inner service.
    pub read: bool,
    /// Promote data read from the inner service into the cache (read-through fill).
    ///
    /// Note: This option only takes effect when [`CacheOptions::read`] is enabled.
    pub read_promotion: bool,
    /// Write-through caching for data written to the inner service.
    pub write: bool,
}

pub struct CacheAccessor<A, S> {
    inner: A,
    cache_service: Arc<S>,
    cache_options: CacheOptions,
}

impl<A: Access, S: CacheService> LayeredAccess for CacheAccessor<A, S> {
    type Inner = A;
    type Reader = CacheReader<A::Reader, S>;
    type Writer = CacheWriter<A::Writer, S>;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let cache_key = path.to_owned();

        // Try cache first if read caching is enabled
        if self.cache_options.read {
            match self.cache_service.read(&cache_key).await {
                Ok(Some(cached_data)) => {
                    // Cache hit
                    return Ok((RpRead::new(), CacheReader::from_buffer(cached_data)));
                }
                Ok(None) => { /* Cache miss, continue to underlying service */ }
                Err(_) => { /* Cache error, continue to underlying service */ }
            }
        }

        // Query underlying service
        let (rp, reader) = self.inner.read(path, args).await?;

        // Create a reader that will cache data as it's read
        Ok((
            rp,
            CacheReader::new(
                reader,
                self.cache_service.clone(),
                cache_key,
                self.cache_options.read_promotion,
            ),
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let cache_key = path.to_owned();

        // Always try to write to underlying storage first
        let (rp, writer) = self.inner.write(path, args).await?;

        // Create a writer that will cache data as it's written
        Ok((
            rp,
            CacheWriter::new(
                writer,
                self.cache_service.clone(),
                cache_key,
                self.cache_options.write,
            ),
        ))
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let cache_key = &path;

        // Check cache first if read caching is enabled
        if self.cache_options.read {
            match self.cache_service.stat(cache_key).await {
                Ok(metadata) => {
                    // Cache hit - key exists in cache service
                    return Ok(RpStat::new(metadata));
                }
                Err(_) => { /* Cache miss, continue to underlying service */ }
            }
        }

        // Fallback to underlying service
        self.inner.stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
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
/// Reader that caches data as it reads from the underlying service
pub enum CacheReader<R, S> {
    /// Reader backed by cached data
    Cached { data: Buffer, pos: usize },
    /// Reader that reads from underlying service and caches the data
    Uncached {
        inner: R,
        cache_service: Arc<S>,
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
- This ensures that later reads of the same file will be cache hits

### CacheWriter

The `CacheWriter` handles write-through caching by writing to both the underlying storage and cache:

```rust
/// Writer that caches data as it writes to the underlying service
pub struct CacheWriter<W, S> {
    inner: W,
    cache_service: Arc<S>,
    cache_key: String,
    cache_write: bool,
    buffer: BytesMut,
}
```

1. **Buffer Accumulation**: All written data is accumulated in an internal `buffer`
2. **Primary Write**: Data is always written to the underlying service first via `inner.write()`
3. **Cache Write**: If `cache_write` is enabled and the writing to underlying service succeeds, the complete data is written to cache
4. **Atomic Caching**: Cache operations happen only after successful completion to ensure cache consistency

### Error Handling

Cache operations are designed to be transparent and non-blocking:

- Cache errors don't fail the underlying operation
- Cache misses fall back to underlying service
- Write operations succeed even if caching fails

### Key Generation

Cache keys are generated from the file path.
The current implementation uses the path directly as the cache key, which works well for most use cases.
Future improvements could include:

- Key prefixing for namespace isolation
- Hashing for very long paths
- Custom key generation strategies

# Drawbacks

1. **Cache Consistency**: The layer doesn't provide strong consistency guarantees between cache and underlying service.
   External modifications to the underlying storage won't automatically invalidate the cache.

2. **No Cache Invalidation API**: There's no built-in way to explicitly invalidate cache entries, relying entirely on the underlying cache service's policies.

# Rationale and alternatives

## Why This Design?

1. **Simplicity**: By delegating cache policies to the underlying storage service, the Cache Layer remains simple and focused.

2. **Flexibility**: Users can choose any OpenDAL service as a cache service, allowing them to pick the right tool for their use case.

3. **Composability**: The layer design allows stacking multiple cache layers for complex caching strategies.

4. **Transparency**: The caching is completely transparent to the application code.

## Alternative Designs Considered

### Built-in Cache Policies

This was rejected because:

- It would duplicate functionality already available in specialized cache services
- It would make the layer more complex and harder to maintain

# Prior art

None

# Unresolved questions

1. **Cache Key Strategy**: Should we provide options for custom cache key generation (e.g., hashing, prefixing)?

2. **Invalidation API**: Should we provide explicit cache invalidation methods, or rely entirely on the underlying cache storage?

# Future possibilities

- Customizable Cache Key Generation:
  - Options for hashing, prefixing, or other strategies
- Cache Statistics and Monitoring:
  - Built-in metrics for cache performance (hit/miss rates, latency)
