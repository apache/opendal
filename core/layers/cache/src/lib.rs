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

//! Cache layer implementation for Apache OpenDAL.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

use std::fmt::Debug;
use std::sync::Arc;

use bytes::BytesMut;
use opendal_core::raw::*;
use opendal_core::*;

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

/// Configuration options for [`CacheLayer`].
///
/// See [`CacheLayer`] for an overview of the cache behavior controlled by these options.
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

impl CacheOptions {
    /// Normalize options to ensure they are internally consistent.
    ///
    /// In particular, `read_promotion` has no effect when `read` is disabled.
    pub fn normalize(mut self) -> Self {
        if !self.read {
            self.read_promotion = false;
        }
        self
    }
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            read: true,
            read_promotion: true,
            write: true,
        }
    }
}

/// Cache layer lets you wrap a slower backend (for example, FS or S3) with a faster cache
/// (for example, Memory or Moka) without changing application code.
///
/// The cache service can be any OpenDAL [`Operator`], allowing reuse of existing services as a
/// cache backend.
///
/// # Options
///
/// - **Read-through**: Look up in cache first, fallback to inner service.
/// - **Read promotion**: On a miss, read from the inner service and store the bytes into cache for later hits.
/// - **Write-through**: Persist writes to both the inner service and cache once the inner write succeeds.
///
/// # Examples
///
/// ## Basic usage (single cache layer)
///
/// ```no_run
/// # use opendal_core::services::Memory;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_cache::{CacheLayer, CacheOptions};
/// # use opendal_service_fs::Fs;
/// #
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// // Inner: a slower backend (for example, Fs).
/// let inner = Operator::new(Fs::default().root("/tmp"))?.finish();
///
/// // Cache: a faster backend (for example, Memory).
/// let cache = Operator::new(Memory::default())?.finish();
///
/// let op = inner.layer(
///     CacheLayer::new(cache).with_options(CacheOptions {
///         read: true,             // default: true
///         read_promotion: true,   // default: true
///         write: true,            // default: true
///     }),
/// );
///
/// // Reads and writes go through `op` as usual.
/// op.write("hello.txt", "hello").await?;
/// let data = op.read("hello.txt").await?;
/// assert_eq!(data.to_bytes(), b"hello"[..]);
/// # Ok(())
/// # }
/// ```
///
/// ## Multi-layer usage (L1 + L2 cache)
///
/// Stack cache layers to build a tiered cache. With typical settings, reads will try L1 first,
/// then L2, then the source. Data read from lower tiers can be promoted back into upper tiers.
///
/// ```no_run
/// # use opendal_core::services::Memory;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_cache::{CacheLayer, CacheOptions};
/// # use opendal_service_fs::Fs;
/// #
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let source = Operator::new(Fs::default().root("/tmp"))?.finish();
///
/// // L2 cache (larger, potentially slower than L1).
/// let l2 = Operator::new(Memory::default())?.finish();
/// // L1 cache (smaller, fastest).
/// let l1 = Operator::new(Memory::default())?.finish();
///
/// let options = CacheOptions {
///     read: true,             // default: true
///     read_promotion: true,   // default: true
///     write: true,            // default: true
/// };
///
/// let op = source
///     .layer(CacheLayer::new(l2).with_options(options))
///     .layer(CacheLayer::new(l1).with_options(options));
///
/// op.write("multi.txt", "tiered").await?;
/// let data = op.read("multi.txt").await?;
/// assert_eq!(data.to_bytes(), b"tiered"[..]);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct CacheLayer<S> {
    service: Arc<S>,
    options: CacheOptions,
}

impl<S> CacheLayer<S> {
    /// Create a new [`CacheLayer`] using the given cache service with default options.
    pub fn new(inner: S) -> Self {
        Self {
            service: Arc::new(inner),
            options: CacheOptions::default(),
        }
    }

    /// Configure this layer with [`CacheOptions`].
    pub fn with_options(mut self, options: CacheOptions) -> Self {
        self.options = options.normalize();
        self
    }
}

impl<A: Access, S: CacheService> Layer<A> for CacheLayer<S> {
    type LayeredAccess = CacheAccessor<A, S>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        CacheAccessor {
            inner,
            cache_service: self.service.clone(),
            cache_options: self.options.normalize(),
        }
    }
}

#[doc(hidden)]
pub struct CacheAccessor<A, S> {
    inner: A,
    cache_service: Arc<S>,
    cache_options: CacheOptions,
}

impl<A: Debug, S: CacheService> Debug for CacheAccessor<A, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheAccessor")
            .field("inner", &self.inner)
            .field("cache_scheme", &self.cache_service.scheme())
            .field("cache_options", &self.cache_options)
            .finish()
    }
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

/// Reader that caches data as it reads from the underlying service.
#[doc(hidden)]
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

impl<R, S> CacheReader<R, S> {
    /// Create a new cache reader from cached data.
    fn from_buffer(data: Buffer) -> Self {
        Self::Cached { data, pos: 0 }
    }

    /// Create a new cache reader that will read from underlying service.
    fn new(inner: R, cache_service: Arc<S>, cache_key: String, cache_read_promotion: bool) -> Self {
        Self::Uncached {
            inner,
            cache_service,
            cache_key,
            cache_read_promotion,
            buffer: BytesMut::new(),
        }
    }
}

impl<R: oio::Read, S: CacheService> oio::Read for CacheReader<R, S> {
    async fn read(&mut self) -> Result<Buffer> {
        match self {
            Self::Cached { data, pos } => {
                if *pos >= data.len() {
                    return Ok(Buffer::new());
                }

                let remaining = data.slice(*pos..);
                *pos = data.len();
                Ok(remaining)
            }
            Self::Uncached {
                inner,
                cache_service,
                cache_key,
                cache_read_promotion,
                buffer,
            } => {
                let chunk = inner.read().await?;

                if chunk.is_empty() {
                    // EOF reached, cache the complete data if read promotion is enabled
                    if *cache_read_promotion && !buffer.is_empty() {
                        let cached_data = buffer.to_vec();
                        let _ = cache_service.write(cache_key, cached_data).await;
                    }
                    return Ok(Buffer::new());
                }

                // Accumulate data for caching
                if *cache_read_promotion {
                    buffer.extend_from_slice(&chunk.to_bytes());
                }

                Ok(chunk)
            }
        }
    }
}

/// Writer that caches data as it writes to the underlying service
#[doc(hidden)]
pub struct CacheWriter<W, S> {
    inner: W,
    cache_service: Arc<S>,
    cache_key: String,
    cache_write: bool,
    buffer: BytesMut,
}

impl<W, S> CacheWriter<W, S> {
    fn new(inner: W, cache_service: Arc<S>, cache_key: String, cache_write: bool) -> Self {
        Self {
            inner,
            cache_service,
            cache_key,
            cache_write,
            buffer: BytesMut::new(),
        }
    }
}

impl<W: oio::Write, S: CacheService> oio::Write for CacheWriter<W, S> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        // Always write to underlying service first
        self.inner.write(bs.clone()).await?;

        // Accumulate data for potential caching if `cache_write` is enabled
        if self.cache_write {
            self.buffer.extend_from_slice(&bs.to_bytes());
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        // Finalize the underlying writer
        match self.inner.close().await {
            Ok(metadata) => {
                // Cache the complete data if `cache_write` is enabled
                if self.cache_write && !self.buffer.is_empty() {
                    // Cache errors don't fail the write operation
                    let _ = self
                        .cache_service
                        .write(&self.cache_key, self.buffer.to_vec())
                        .await;
                }

                self.buffer.clear();
                Ok(metadata)
            }
            Err(err) => {
                self.buffer.clear();
                Err(err)
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        // No need to cache anything since the write operation was aborted
        self.buffer.clear();

        // Abort underlying writer
        self.inner.abort().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::services::Memory;
    use opendal_service_fs::Fs;

    fn fs_backend() -> Result<Operator> {
        // let tmp = tempfile::tempdir().expect("create tempdir");
        let op = Operator::new(Fs::default().root("/tmp"))?.finish();
        Ok(op)
    }

    fn memory_backend() -> Result<Operator> {
        let op = Operator::new(Memory::default())?.finish();
        Ok(op)
    }

    #[tokio::test]
    async fn basic_cache_layer_with_default_options() -> Result<()> {
        let inner = fs_backend()?;
        let cache = memory_backend()?;
        let op = inner.clone().layer(CacheLayer::new(cache.clone()));

        let path = "foo.txt";

        // write into the inner service directly
        {
            inner.write(path, "hello").await?;

            // there's no data in the cache
            let err = cache.read(path).await.expect_err("not in the memory cache");
            assert_eq!(err.kind(), ErrorKind::NotFound);
            assert!(!cache.exists(path).await?);

            inner.delete(path).await?;
        }

        // write into the operator with cache layer
        {
            op.write(path, "world").await?;

            let data = inner.read(path).await?;
            assert_eq!(data.to_bytes(), b"world"[..]);

            let data = cache.read(path).await?;
            assert_eq!(data.to_bytes(), b"world"[..]);

            let data = op.read(path).await?;
            assert_eq!(data.to_bytes(), b"world"[..]);
        }

        Ok(())
    }

    #[tokio::test]
    async fn multi_layer_cache_hits_l1_then_l2_then_source() -> Result<()> {
        let inner = fs_backend()?;
        let l2 = memory_backend()?;
        let l1 = memory_backend()?;

        let op = inner
            .clone()
            .layer(CacheLayer::new(l2.clone()))
            .layer(CacheLayer::new(l1.clone()));

        let path = "multi.txt";
        inner.write(path, "tiered").await?;

        // First read: miss L1/L2, fill both
        let first = op.read(path).await?;
        assert_eq!(first.to_bytes(), b"tiered"[..]);

        // Remove source; should hit L1 directly
        inner.delete(path).await?;
        let second = op.read(path).await?;
        assert_eq!(second.to_bytes(), b"tiered"[..]);

        // Drop L1 entry to force fallback into L2 on next read
        l1.delete(path).await?;

        // Third read: expect fetch from L2 and re-promote to L1
        let third = op.read(path).await?;
        assert_eq!(third.to_bytes(), b"tiered"[..]);
        assert!(l1.exists(path).await?);

        // L2 should still have the data
        let l2_hit = l2.read(path).await?;
        assert_eq!(l2_hit.to_bytes(), b"tiered"[..]);

        Ok(())
    }

    #[derive(Clone, Copy, Debug)]
    struct Case {
        name: &'static str,
        options: CacheOptions,
        // Expectations for the common scenarios below:
        expect_cache_after_read_miss: bool,
        expect_read_hit_after_source_deleted: bool,
        expect_cache_after_write: bool,
    }

    async fn run_case(case: Case) -> Result<()> {
        // Use FS inner so we can delete source to validate behavior; keep cache as Memory for determinism.
        let inner = fs_backend()?;
        let cache = memory_backend()?;

        let op = inner
            .clone()
            .layer(CacheLayer::new(cache.clone()).with_options(case.options));

        // Use flat keys to avoid any path semantics differences between services.
        // Also include options to guarantee uniqueness even if `name` collides.
        let read_path = format!(
            "case-{}-r{}-p{}-w{}-read.txt",
            case.name, case.options.read, case.options.read_promotion, case.options.write
        );
        let write_path = format!(
            "case-{}-r{}-p{}-w{}-write.txt",
            case.name, case.options.read, case.options.read_promotion, case.options.write
        );

        // ---- Read-miss flow ----
        //
        // Don't assert initial cache emptiness here: other tests run in parallel and some cache
        // backends may behave differently with key normalization. Instead, verify cache behavior
        // via observable outcomes (whether we can still read after deleting the source, and whether
        // the cache backend reports the key afterwards).
        inner.write(&read_path, "hello").await?;

        let data = op.read(&read_path).await?;
        assert_eq!(data.to_bytes(), b"hello"[..]);

        // Cache may be (or may become) populated depending on read promotion.
        if case.expect_cache_after_read_miss {
            assert!(cache.exists(&read_path).await?);
        }

        // Delete source and try reading again to see whether cache can serve the hit.
        inner.delete(&read_path).await?;
        match op.read(&read_path).await {
            Ok(v) => {
                assert!(
                    case.expect_read_hit_after_source_deleted,
                    "case {}: expected miss after source deleted but got hit",
                    case.name
                );
                assert_eq!(v.to_bytes(), b"hello"[..]);
            }
            Err(err) => {
                assert!(
                    !case.expect_read_hit_after_source_deleted,
                    "case {}: expected hit after source deleted but got error {err:?}",
                    case.name
                );
                assert_eq!(err.kind(), ErrorKind::NotFound);
            }
        }

        // When we expect the cache to be able to serve the hit, it should also report existence.
        if case.expect_read_hit_after_source_deleted {
            assert!(cache.exists(&read_path).await?);
        }

        // ---- Write flow (write-through) ----
        //
        // Same: avoid asserting initial emptiness and only assert positive behavior when enabled.
        op.write(&write_path, "world").await?;
        assert!(inner.exists(&write_path).await?);

        if case.expect_cache_after_write {
            assert!(cache.exists(&write_path).await?);
            let cached = cache.read(&write_path).await?;
            assert_eq!(cached.to_bytes(), b"world"[..]);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_options_cases() -> Result<()> {
        // There are 3 booleans: read/read_promotion/write, but read_promotion only matters when read=true.
        // So there are 6 effective configurations; verify them all.
        //
        // Legend:
        // - expect_cache_after_read_miss: cache is filled after a miss + full read
        // - expect_read_hit_after_source_deleted: cache can serve read after deleting source
        // - expect_cache_after_write: write-through fills cache
        let cases = [
            // read=false => promotion is irrelevant; no read caching / no promotion.
            Case {
                name: "read_off_write_off",
                options: CacheOptions {
                    read: false,
                    read_promotion: false,
                    write: false,
                },
                expect_cache_after_read_miss: false,
                expect_read_hit_after_source_deleted: false,
                expect_cache_after_write: false,
            },
            Case {
                name: "read_off_write_on_promo_irrelevant",
                options: CacheOptions {
                    read: false,
                    read_promotion: true,
                    write: true,
                },
                expect_cache_after_read_miss: false,
                expect_read_hit_after_source_deleted: false,
                expect_cache_after_write: true,
            },
            // read=true, promotion=false: reads consult cache but won't fill on miss.
            Case {
                name: "read_on_promo_off_write_off",
                options: CacheOptions {
                    read: true,
                    read_promotion: false,
                    write: false,
                },
                expect_cache_after_read_miss: false,
                expect_read_hit_after_source_deleted: false,
                expect_cache_after_write: false,
            },
            Case {
                name: "read_on_promo_off_write_on",
                options: CacheOptions {
                    read: true,
                    read_promotion: false,
                    write: true,
                },
                expect_cache_after_read_miss: false,
                expect_read_hit_after_source_deleted: false,
                expect_cache_after_write: true,
            },
            // read=true, promotion=true: reads fill on miss; cache can serve after source deletion.
            Case {
                name: "read_on_promo_on_write_off",
                options: CacheOptions {
                    read: true,
                    read_promotion: true,
                    write: false,
                },
                expect_cache_after_read_miss: true,
                expect_read_hit_after_source_deleted: true,
                expect_cache_after_write: false,
            },
            Case {
                name: "read_on_promo_on_write_on",
                options: CacheOptions {
                    read: true,
                    read_promotion: true,
                    write: true,
                },
                expect_cache_after_read_miss: true,
                expect_read_hit_after_source_deleted: true,
                expect_cache_after_write: true,
            },
        ];

        for case in cases {
            run_case(case).await?;
        }

        Ok(())
    }
}
