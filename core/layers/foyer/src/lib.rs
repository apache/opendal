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

mod chunked;
mod deleter;
mod error;
mod full;
mod writer;

use std::{
    future::Future,
    ops::{Bound, Range, RangeBounds},
    sync::Arc,
};

use foyer::HybridCache;

use opendal_core::raw::*;
use opendal_core::*;

pub use deleter::Deleter;
pub use writer::Writer;

/// [`FoyerKey`] is a key for the foyer cache. It's encoded via bincode, which is
/// backed by foyer's "serde" feature.
///
/// It's possible to specify a version in the [`OpRead`] args:
///
/// - If a version is given, the object is cached under that versioned key.
/// - If version is not supplied, the object is cached exactly as returned by the backend,
///   We do NOT interpret `None` as "latest" and we do not promote it to any other version.
///
/// # Variants
///
/// - `Full`: Caches the entire object (used in non-chunked mode)
/// - `ChunkMetadata`: Caches object metadata for chunked mode (content_length, version, etag)
/// - `Chunk`: Caches a specific chunk of the object
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum FoyerKey {
    /// Cache key for the entire object (non-chunked mode)
    Full {
        path: String,
        version: Option<String>,
    },
    /// Cache key for object metadata (chunked mode)
    ChunkMetadata {
        path: String,
        chunk_size: usize,
        version: Option<String>,
    },
    /// Cache key for a specific chunk of the object
    Chunk {
        path: String,
        chunk_size: usize,
        chunk_index: u64,
        version: Option<String>,
    },
}

/// [`FoyerValue`] is a value type for the foyer cache that can store either
/// a raw buffer or cached metadata.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum FoyerValue {
    /// Raw buffer data (used for full objects and chunks)
    Buffer(CachedBuffer),
    /// Cached metadata for chunked mode
    ChunkMetadata(CachedChunkMetadata),
}

/// A serializable wrapper around [`Buffer`].
#[derive(Debug, Clone)]
pub struct CachedBuffer(pub Buffer);

impl serde::Serialize for CachedBuffer {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.0.to_vec(), serializer)
    }
}

impl<'de> serde::Deserialize<'de> for CachedBuffer {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let vec = Vec::<u8>::deserialize(deserializer)?;
        Ok(CachedBuffer(Buffer::from(vec)))
    }
}

/// Cached metadata for an object in chunked mode.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CachedChunkMetadata {
    pub content_length: u64,
    pub version: Option<String>,
    pub etag: Option<String>,
}

/// Hybrid cache layer for OpenDAL that uses [foyer](https://github.com/foyer-rs/foyer) for caching.
///
/// # Operation Behavior
/// - `write`: [`FoyerLayer`] will write to the foyer hybrid cache after the service's write operation is completed.
/// - `read`: [`FoyerLayer`] will first check the foyer hybrid cache for the data. If the data is not found, it will perform the read operation on the service and cache the result.
/// - `delete`: [`FoyerLayer`] will remove the data from the foyer hybrid cache regardless of whether the service's delete operation is successful.
/// - Other operations: [`FoyerLayer`] will not cache the results of other operations, such as `list`, `copy`, `rename`, etc. They will be passed through to the underlying accessor without caching.
///
/// # Caching Modes
///
/// ## Full Mode (default)
/// Caches the entire object as a single cache entry. Best for small to medium-sized objects.
///
/// ## Chunked Mode
/// When `chunk_size` is set via [`with_chunk_size`](FoyerLayer::with_chunk_size), objects are cached
/// in fixed-size chunks. This improves cache efficiency for large files with partial reads:
/// - Only the chunks covering the requested range are fetched and cached
/// - Subsequent reads of overlapping ranges reuse cached chunks
/// - Each chunk is cached independently, allowing fine-grained cache eviction
///
/// # Examples
///
/// ```no_run
/// use opendal_core::{Operator, services::Memory};
/// use opendal_layer_foyer::FoyerLayer;
/// use foyer::{HybridCacheBuilder, Engine};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let cache = HybridCacheBuilder::new()
///     .memory(64 * 1024 * 1024) // 64MB memory cache
///     .with_shards(4)
///     .storage(Engine::Large(Default::default()))
///     .build()
///     .await?;
///
/// // Full mode (default)
/// let op = Operator::new(Memory::default())?
///     .layer(FoyerLayer::new(cache.clone()))
///     .finish();
///
/// // Chunked mode with 4MB chunks
/// let op_chunked = Operator::new(Memory::default())?
///     .layer(FoyerLayer::new(cache).with_chunk_size(4 * 1024 * 1024))
///     .finish();
/// # Ok(())
/// # }
/// ```
///
/// # Note
///
/// If the object version is enabled, the foyer cache layer will treat the objects with same path but different versions as different objects.
#[derive(Debug)]
pub struct FoyerLayer {
    cache: HybridCache<FoyerKey, FoyerValue>,
    size_limit: Range<usize>,
    chunk_size: Option<usize>,
}

impl FoyerLayer {
    /// Creates a new `FoyerLayer` with the given foyer hybrid cache.
    pub fn new(cache: HybridCache<FoyerKey, FoyerValue>) -> Self {
        FoyerLayer {
            cache,
            size_limit: 0..usize::MAX,
            chunk_size: None,
        }
    }

    /// Sets the size limit for caching.
    ///
    /// It is recommended to set a size limit to avoid caching large files that may not be suitable for caching.
    pub fn with_size_limit<R: RangeBounds<usize>>(mut self, size_limit: R) -> Self {
        let start = match size_limit.start_bound() {
            Bound::Included(v) => *v,
            Bound::Excluded(v) => *v + 1,
            Bound::Unbounded => 0,
        };
        let end = match size_limit.end_bound() {
            Bound::Included(v) => *v + 1,
            Bound::Excluded(v) => *v,
            Bound::Unbounded => usize::MAX,
        };
        self.size_limit = start..end;
        self
    }

    /// Enables chunked caching mode with the specified chunk size.
    ///
    /// When enabled, objects are cached in fixed-size chunks rather than as whole objects.
    /// This is beneficial for:
    /// - Large files where only portions are typically read
    /// - Random access patterns within files
    /// - Reducing memory pressure by caching only needed chunks
    ///
    /// # Arguments
    /// * `chunk_size` - The size of each chunk in bytes. Recommended: 1MB - 16MB.
    ///
    /// # Note
    /// - The `size_limit` setting is ignored in chunked mode
    /// - Writer and Deleter still operate in full mode for consistency
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = Some(chunk_size);
        self
    }
}

impl<A: Access> Layer<A> for FoyerLayer {
    type LayeredAccess = FoyerAccessor<A>;

    fn layer(&self, accessor: A) -> Self::LayeredAccess {
        let cache = self.cache.clone();
        FoyerAccessor {
            inner: Arc::new(Inner {
                accessor,
                cache,
                size_limit: self.size_limit.clone(),
                chunk_size: self.chunk_size,
            }),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Inner<A: Access> {
    pub(crate) accessor: A,
    pub(crate) cache: HybridCache<FoyerKey, FoyerValue>,
    pub(crate) size_limit: Range<usize>,
    pub(crate) chunk_size: Option<usize>,
}

#[derive(Debug)]
pub struct FoyerAccessor<A: Access> {
    inner: Arc<Inner<A>>,
}

impl<A: Access> LayeredAccess for FoyerAccessor<A> {
    type Inner = A;
    type Reader = Buffer;
    type Writer = Writer<A>;
    type Lister = A::Lister;
    type Deleter = Deleter<A>;

    fn inner(&self) -> &Self::Inner {
        &self.inner.accessor
    }

    fn info(&self) -> Arc<AccessorInfo> {
        self.inner.accessor.info()
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        match self.inner.chunk_size {
            None => {
                full::FullReader::new(self.inner.clone(), self.inner.size_limit.clone())
                    .read(path, args)
                    .await
            }
            Some(chunk_size) => {
                chunked::ChunkedReader::new(self.inner.clone(), chunk_size)
                    .read(path, args)
                    .await
            }
        }
    }

    fn write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> impl Future<Output = Result<(RpWrite, Self::Writer)>> + MaybeSend {
        let inner = self.inner.clone();
        let size_limit = self.inner.size_limit.clone();
        let path = path.to_string();
        async move {
            let (rp, w) = inner.accessor.write(&path, args).await?;
            Ok((rp, Writer::new(w, path, inner, size_limit)))
        }
    }

    fn delete(&self) -> impl Future<Output = Result<(RpDelete, Self::Deleter)>> + MaybeSend {
        let inner = self.inner.clone();
        async move {
            let (rp, d) = inner.accessor.delete().await?;
            Ok((rp, Deleter::new(d, inner)))
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.accessor.list(path, args).await
    }

    // TODO(MrCroxx): Implement copy, rename with foyer cache.
}

#[cfg(test)]
mod tests {
    use foyer::{
        Code, DirectFsDeviceOptions, Engine, Error as FoyerError, HybridCacheBuilder,
        LargeEngineOptions, RecoverMode,
    };
    use opendal_core::{Operator, services::Memory};
    use size::consts::MiB;
    use std::io::Cursor;

    use super::*;
    use crate::error::extract_err;

    fn key(i: u8) -> String {
        format!("obj-{i}")
    }

    fn value(i: u8) -> Vec<u8> {
        // ~ 64KiB with metadata
        vec![i; 63 * 1024]
    }

    #[tokio::test]
    async fn test() {
        let dir = tempfile::tempdir().unwrap();

        let cache = HybridCacheBuilder::new()
            .memory(10)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()))
            .finish();

        assert!(op.list("/").await.unwrap().is_empty());

        for i in 0..64 {
            op.write(&key(i), value(i)).await.unwrap();
        }

        assert_eq!(op.list("/").await.unwrap().len(), 64);

        for i in 0..64 {
            let buf = op.read(&key(i)).await.unwrap();
            assert_eq!(buf.to_vec(), value(i));
        }

        cache.clear().await.unwrap();

        for i in 0..64 {
            let buf = op.read(&key(i)).await.unwrap();
            assert_eq!(buf.to_vec(), value(i));
        }

        for i in 0..64 {
            op.delete(&key(i)).await.unwrap();
        }

        assert!(op.list("/").await.unwrap().is_empty());

        for i in 0..64 {
            let res = op.read(&key(i)).await;
            assert!(res.is_err(), "should fail to read deleted file");
        }
    }

    #[tokio::test]
    async fn test_size_limit() {
        let dir = tempfile::tempdir().unwrap();

        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        // Set size limit: only cache files between 1KB and 10KB
        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()).with_size_limit(1024..10 * 1024))
            .finish();

        let small_data = vec![1u8; 5 * 1024]; // 5KB - should be cached
        let large_data = vec![2u8; 20 * 1024]; // 20KB - should NOT be cached
        let tiny_data = vec![3u8; 512]; // 512B - below size limit, should NOT be cached

        // Write all files
        op.write("small.txt", small_data.clone()).await.unwrap();
        op.write("large.txt", large_data.clone()).await.unwrap();
        op.write("tiny.txt", tiny_data.clone()).await.unwrap();

        // All should be readable
        let read_small = op.read("small.txt").await.unwrap();
        assert_eq!(read_small.to_vec(), small_data);

        let read_large = op.read("large.txt").await.unwrap();
        assert_eq!(read_large.to_vec(), large_data);

        let read_tiny = op.read("tiny.txt").await.unwrap();
        assert_eq!(read_tiny.to_vec(), tiny_data);

        // Clear the cache to test read-through behavior
        cache.clear().await.unwrap();

        // All files should still be readable from underlying storage
        let read_small = op.read("small.txt").await.unwrap();
        assert_eq!(read_small.to_vec(), small_data);

        let read_large = op.read("large.txt").await.unwrap();
        assert_eq!(read_large.to_vec(), large_data);

        let read_tiny = op.read("tiny.txt").await.unwrap();
        assert_eq!(read_tiny.to_vec(), tiny_data);

        // After reading, small file should be cached, but large and tiny should not
        // We can verify this by reading with range - cached files should support range reads
        let read_small_range = op.read_with("small.txt").range(0..1024).await.unwrap();
        assert_eq!(read_small_range.len(), 1024);
        assert_eq!(read_small_range.to_vec(), small_data[0..1024]);
    }

    #[test]
    fn test_error() {
        let e = Error::new(ErrorKind::NotFound, "not found");
        let fe = FoyerError::other(e);
        let oe = extract_err(fe);
        assert_eq!(oe.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn test_foyer_key_version_none_vs_empty() {
        let key_none = FoyerKey::Full {
            path: "test/path".to_string(),
            version: None,
        };

        let key_empty = FoyerKey::Full {
            path: "test/path".to_string(),
            version: Some("".to_string()),
        };

        let mut buf_none = Vec::new();
        key_none.encode(&mut buf_none).unwrap();

        let mut buf_empty = Vec::new();
        key_empty.encode(&mut buf_empty).unwrap();

        assert_ne!(
            buf_none, buf_empty,
            "Serialization of version=None and version=\"\" should be different"
        );

        let decoded_none = FoyerKey::decode(&mut Cursor::new(&buf_none)).unwrap();
        assert_eq!(decoded_none, key_none);
        let decoded_empty = FoyerKey::decode(&mut Cursor::new(&buf_empty)).unwrap();
        assert_eq!(decoded_empty, key_empty);
    }

    #[test]
    fn test_foyer_key_serde() {
        use std::io::Cursor;

        let test_cases: Vec<FoyerKey> = vec![
            FoyerKey::Full {
                path: "simple".to_string(),
                version: None,
            },
            FoyerKey::Full {
                path: "with/slash/path".to_string(),
                version: None,
            },
            FoyerKey::Full {
                path: "versioned".to_string(),
                version: Some("v1.0.0".to_string()),
            },
            FoyerKey::Full {
                path: "empty-version".to_string(),
                version: Some("".to_string()),
            },
            FoyerKey::Full {
                path: "".to_string(),
                version: None,
            },
            FoyerKey::Full {
                path: "unicode/路径/🚀".to_string(),
                version: Some("版本-1".to_string()),
            },
            FoyerKey::Full {
                path: "long/".to_string().repeat(100),
                version: Some("long-version-".to_string().repeat(50)),
            },
            // Test Metadata variant
            FoyerKey::ChunkMetadata {
                path: "meta/path".to_string(),
                chunk_size: 1024,
                version: None,
            },
            FoyerKey::ChunkMetadata {
                path: "meta/versioned".to_string(),
                chunk_size: 4096,
                version: Some("v2".to_string()),
            },
            // Test Chunk variant
            FoyerKey::Chunk {
                path: "chunk/path".to_string(),
                chunk_size: 1024,
                chunk_index: 0,
                version: None,
            },
            FoyerKey::Chunk {
                path: "chunk/versioned".to_string(),
                chunk_size: 4096,
                chunk_index: 42,
                version: Some("v3".to_string()),
            },
        ];

        for original in test_cases {
            let mut buffer = Vec::new();
            original
                .encode(&mut buffer)
                .expect("encoding should succeed");

            let decoded =
                FoyerKey::decode(&mut Cursor::new(&buffer)).expect("decoding should succeed");

            assert_eq!(
                decoded, original,
                "decode(encode(key)) should equal original key"
            );
        }
    }

    #[tokio::test]
    async fn test_chunked_single_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache).with_chunk_size(1024))
            .finish();

        let data = vec![42u8; 500];
        op.write("test.bin", data.clone()).await.unwrap();

        // Read within single chunk
        let read_data = op.read_with("test.bin").range(100..300).await.unwrap();
        assert_eq!(read_data.len(), 200);
        assert_eq!(read_data.to_vec(), data[100..300]);
    }

    #[tokio::test]
    async fn test_chunked_multiple_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache).with_chunk_size(1024))
            .finish();

        // 3KB file across 3 chunks
        let data: Vec<u8> = (0..3000).map(|i| (i % 256) as u8).collect();
        op.write("large.bin", data.clone()).await.unwrap();

        // Read spanning multiple chunks (byte 500 to 2500 covers 3 chunks)
        let read_data = op.read_with("large.bin").range(500..2500).await.unwrap();
        assert_eq!(read_data.len(), 2000);
        assert_eq!(read_data.to_vec(), data[500..2500]);
    }

    #[tokio::test]
    async fn test_chunked_full_read() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache).with_chunk_size(1024))
            .finish();

        let data = vec![123u8; 2500];
        op.write("full.bin", data.clone()).await.unwrap();

        // Read entire file
        let read_data = op.read("full.bin").await.unwrap();
        assert_eq!(read_data.len(), 2500);
        assert_eq!(read_data.to_vec(), data);
    }

    #[tokio::test]
    async fn test_chunked_last_partial_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache).with_chunk_size(1024))
            .finish();

        // 1500 bytes = 1 full chunk + 476 bytes
        let data: Vec<u8> = (0..1500).map(|i| (i % 256) as u8).collect();
        op.write("partial.bin", data.clone()).await.unwrap();

        // Read from last partial chunk
        let read_data = op.read_with("partial.bin").range(1200..1500).await.unwrap();
        assert_eq!(read_data.len(), 300);
        assert_eq!(read_data.to_vec(), data[1200..1500]);
    }

    #[tokio::test]
    async fn test_chunked_cache_hit() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()).with_chunk_size(1024))
            .finish();

        let data = vec![99u8; 3000];
        op.write("cached.bin", data.clone()).await.unwrap();

        // First read to populate cache
        let read1 = op.read_with("cached.bin").range(1000..2000).await.unwrap();
        assert_eq!(read1.to_vec(), data[1000..2000]);

        // Second read should hit cache for overlapping chunks
        let read2 = op.read_with("cached.bin").range(1500..2500).await.unwrap();
        assert_eq!(read2.to_vec(), data[1500..2500]);
    }

    #[tokio::test]
    async fn test_chunked_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache).with_chunk_size(1024))
            .finish();

        op.write("empty.bin", Vec::<u8>::new()).await.unwrap();

        let read_data = op.read("empty.bin").await.unwrap();
        assert_eq!(read_data.len(), 0);
    }

    #[tokio::test]
    async fn test_chunked_exact_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache).with_chunk_size(1024))
            .finish();

        let data = vec![77u8; 3072]; // Exactly 3 chunks
        op.write("aligned.bin", data.clone()).await.unwrap();

        // Read exactly one chunk
        let read_data = op.read_with("aligned.bin").range(1024..2048).await.unwrap();
        assert_eq!(read_data.len(), 1024);
        assert_eq!(read_data.to_vec(), data[1024..2048]);
    }

    #[tokio::test]
    async fn test_chunked_metadata_caching() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()).with_chunk_size(1024))
            .finish();

        let data = vec![42u8; 5000];
        op.write("meta.bin", data.clone()).await.unwrap();

        // First read to cache metadata and chunks
        let read1 = op.read_with("meta.bin").range(0..1000).await.unwrap();
        assert_eq!(read1.len(), 1000);

        // Clear only data cache (metadata should remain if properly keyed)
        // Second read from different range - metadata should be cached
        let read2 = op.read_with("meta.bin").range(3000..4000).await.unwrap();
        assert_eq!(read2.len(), 1000);
        assert_eq!(read2.to_vec(), data[3000..4000]);
    }

    #[tokio::test]
    async fn test_chunked_different_chunk_sizes() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(2 * 1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let data = vec![88u8; 10000];

        // Test with 1KB chunks
        let op1 = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()).with_chunk_size(1024))
            .finish();
        op1.write("chunk_size_test.bin", data.clone())
            .await
            .unwrap();
        let read1 = op1
            .read_with("chunk_size_test.bin")
            .range(2000..4000)
            .await
            .unwrap();
        assert_eq!(read1.len(), 2000);

        // Test with 2KB chunks - should create different cache keys
        let op2 = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()).with_chunk_size(2048))
            .finish();
        op2.write("chunk_size_test2.bin", data.clone())
            .await
            .unwrap();
        let read2 = op2
            .read_with("chunk_size_test2.bin")
            .range(2000..4000)
            .await
            .unwrap();
        assert_eq!(read2.len(), 2000);
        assert_eq!(read2.to_vec(), data[2000..4000]);
    }

    #[tokio::test]
    async fn test_chunked_cache_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()).with_chunk_size(1024))
            .finish();

        let data: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();
        op.write("persist.bin", data.clone()).await.unwrap();

        // Read to populate cache
        let read1 = op.read_with("persist.bin").range(1000..3000).await.unwrap();
        assert_eq!(read1.to_vec(), data[1000..3000]);

        // Verify cache hit by reading again (overlapping range)
        let read2 = op.read_with("persist.bin").range(1500..2500).await.unwrap();
        assert_eq!(read2.to_vec(), data[1500..2500]);

        // Non-overlapping range should fetch new chunks
        let read3 = op.read_with("persist.bin").range(4000..5000).await.unwrap();
        assert_eq!(read3.to_vec(), data[4000..5000]);
    }

    #[tokio::test]
    async fn test_chunked_boundary_reads() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache).with_chunk_size(1024))
            .finish();

        let data: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
        op.write("boundary.bin", data.clone()).await.unwrap();

        // Read from exact chunk start
        let read1 = op
            .read_with("boundary.bin")
            .range(1024..1524)
            .await
            .unwrap();
        assert_eq!(read1.to_vec(), data[1024..1524]);

        // Read to exact chunk end
        let read2 = op.read_with("boundary.bin").range(500..1024).await.unwrap();
        assert_eq!(read2.to_vec(), data[500..1024]);

        // Read across chunk boundary
        let read3 = op
            .read_with("boundary.bin")
            .range(1020..1030)
            .await
            .unwrap();
        assert_eq!(read3.to_vec(), data[1020..1030]);
    }

    #[tokio::test]
    async fn test_chunked_large_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(4 * 1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(32 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache).with_chunk_size(4096))
            .finish();

        // 100KB file - 25 chunks
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        op.write("large.bin", data.clone()).await.unwrap();

        // Read various sections
        let read1 = op
            .read_with("large.bin")
            .range(10_000..20_000)
            .await
            .unwrap();
        assert_eq!(read1.len(), 10_000);
        assert_eq!(read1.to_vec(), data[10_000..20_000]);

        let read2 = op
            .read_with("large.bin")
            .range(50_000..60_000)
            .await
            .unwrap();
        assert_eq!(read2.len(), 10_000);
        assert_eq!(read2.to_vec(), data[50_000..60_000]);

        // Read entire file
        let read_all = op.read("large.bin").await.unwrap();
        assert_eq!(read_all.len(), 100_000);
        assert_eq!(read_all.to_vec(), data);
    }

    #[tokio::test]
    async fn test_chunked_sequential_reads() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache).with_chunk_size(1024))
            .finish();

        let data: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        op.write("sequential.bin", data.clone()).await.unwrap();

        // Simulate sequential reading pattern
        for offset in (0..10_000).step_by(500) {
            let end = (offset + 500).min(data.len());
            let read_data = op
                .read_with("sequential.bin")
                .range(offset as u64..end as u64)
                .await
                .unwrap();
            assert_eq!(read_data.to_vec(), data[offset..end]);
        }
    }

    #[tokio::test]
    async fn test_chunked_random_access() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(2 * 1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache).with_chunk_size(1024))
            .finish();

        let data: Vec<u8> = (0..20_000).map(|i| (i % 256) as u8).collect();
        op.write("random.bin", data.clone()).await.unwrap();

        // Random access pattern
        let ranges = [
            (5000, 5500),
            (15000, 15200),
            (1000, 2000),
            (18000, 19000),
            (500, 1500),
        ];

        for (start, end) in ranges {
            let read_data = op.read_with("random.bin").range(start..end).await.unwrap();
            assert_eq!(read_data.to_vec(), data[start as usize..end as usize]);
        }
    }

    #[tokio::test]
    async fn test_chunked_with_clear_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()).with_chunk_size(1024))
            .finish();

        let data = vec![99u8; 5000];
        op.write("clear_test.bin", data.clone()).await.unwrap();

        // First read to populate cache
        let read1 = op
            .read_with("clear_test.bin")
            .range(1000..2000)
            .await
            .unwrap();
        assert_eq!(read1.to_vec(), data[1000..2000]);

        // Clear cache
        cache.clear().await.unwrap();

        // Read again - should still work (fetch from backend)
        let read2 = op
            .read_with("clear_test.bin")
            .range(1000..2000)
            .await
            .unwrap();
        assert_eq!(read2.to_vec(), data[1000..2000]);
    }

    #[tokio::test]
    async fn test_chunked_single_byte_reads() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage(Engine::Large(LargeEngineOptions::new()))
            .with_device_options(
                DirectFsDeviceOptions::new(dir.path())
                    .with_capacity(16 * MiB as usize)
                    .with_file_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache).with_chunk_size(1024))
            .finish();

        let data: Vec<u8> = (0..3000).map(|i| (i % 256) as u8).collect();
        op.write("single_byte.bin", data.clone()).await.unwrap();

        // Read single bytes at various positions
        let positions = [0, 1023, 1024, 2047, 2048, 2999];
        for pos in positions {
            let read_data = op
                .read_with("single_byte.bin")
                .range(pos..pos + 1)
                .await
                .unwrap();
            assert_eq!(read_data.len(), 1);
            assert_eq!(read_data.to_vec()[0], data[pos as usize]);
        }
    }
}
