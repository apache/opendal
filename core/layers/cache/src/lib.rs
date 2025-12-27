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

#[derive(Clone, Debug)]
struct CacheOptions {
    /// Enable cache lookups before hitting the inner service.
    read: bool,
    /// Promote data read from the inner service into the cache (read-through fill).
    read_promotion: bool,
    /// Write-through caching for data written to the inner service.
    write: bool,
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

/// Cache layer that wraps an `Access` with a [`CacheService`].
///
/// The cache service can be any OpenDAL [`Operator`], allowing reuse of existing services as a
/// cache backend. Provides read-through (cache lookup), read-promotion (populate cache after
/// misses), and write-through caching behaviors.
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

    /// Enable/disable read-through caching.
    /// When disabled, reads bypass the cache entirely.
    pub fn with_cache_read(mut self, enabled: bool) -> Self {
        self.options.read = enabled;
        self
    }

    /// Enable/disable cache promotion during read operations.
    /// When disabled, data fetched from the inner service on a miss will not be stored back into the cache.
    pub fn with_cache_read_promotion(mut self, enabled: bool) -> Self {
        self.options.read_promotion = enabled;
        self
    }

    /// Enable/disable write-through caching.
    /// When enabled, bytes written to the inner service are also stored into the cache.
    pub fn with_cache_write(mut self, enabled: bool) -> Self {
        self.options.write = enabled;
        self
    }
}

impl<A: Access, S: CacheService> Layer<A> for CacheLayer<S> {
    type LayeredAccess = CacheAccessor<A, S>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        CacheAccessor {
            inner,
            cache_service: self.service.clone(),
            cache_options: self.options.clone(),
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
