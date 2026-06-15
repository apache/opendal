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

mod deleter;
mod error;
mod full;
mod writer;

use std::{
    collections::HashSet,
    ops::{Bound, Deref, Range, RangeBounds},
    sync::{Arc, Mutex},
};

use foyer::{Code, HybridCache, Result as FoyerResult};

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
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FoyerKey {
    pub path: String,
    pub version: Option<String>,
}

/// [`FoyerValue`] is a wrapper around `Buffer` that implements the `Code` trait.
#[derive(Debug)]
pub struct FoyerValue(pub Buffer);

impl Deref for FoyerValue {
    type Target = Buffer;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Code for FoyerValue {
    fn encode(&self, writer: &mut impl std::io::Write) -> FoyerResult<()> {
        let len = self.0.len() as u64;
        writer.write_all(&len.to_le_bytes())?;
        std::io::copy(&mut self.0.clone(), writer)?;
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> FoyerResult<Self>
    where
        Self: Sized,
    {
        let mut len_bytes = [0u8; 8];
        reader.read_exact(&mut len_bytes)?;
        let len = u64::from_le_bytes(len_bytes) as usize;
        let mut buffer = vec![0u8; len];
        reader.read_exact(&mut buffer[..len])?;
        Ok(FoyerValue(buffer.into()))
    }

    fn estimated_size(&self) -> usize {
        8 + self.0.len()
    }
}

/// Hybrid cache layer for OpenDAL that uses [foyer](https://github.com/foyer-rs/foyer) for caching.
///
/// # Operation Behavior
/// - `write`: [`FoyerLayer`] will write to the foyer hybrid cache after the service's write operation is completed.
/// - `read`: [`FoyerLayer`] will first check the foyer hybrid cache for the data. If the data is not found, it will perform the read operation on the service and cache the result.
/// - `delete`: [`FoyerLayer`] will remove the data from the foyer hybrid cache regardless of whether the service's delete operation is successful.
/// - Other operations: [`FoyerLayer`] will not cache the results of other operations, such as `list`, `copy`, `rename`, etc. They will be passed through to the underlying service without caching.
///
/// # Examples
///
/// ```no_run
/// use opendal_core::{Operator, services::Memory};
/// use opendal_layer_foyer::FoyerLayer;
/// use foyer::HybridCacheBuilder;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let cache = HybridCacheBuilder::new()
///     .memory(64 * 1024 * 1024) // 64MB memory cache
///     .with_shards(4)
///     .storage()
///     .build()
///     .await?;
///
/// let op = Operator::new(Memory::default())?
///     .layer(FoyerLayer::new(cache));
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
    deleted_keys: Arc<Mutex<HashSet<FoyerKey>>>,
}

impl FoyerLayer {
    /// Creates a new `FoyerLayer` with the given foyer hybrid cache.
    pub fn new(cache: HybridCache<FoyerKey, FoyerValue>) -> Self {
        FoyerLayer {
            cache,
            size_limit: 0..usize::MAX,
            deleted_keys: Arc::default(),
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
}

impl Layer for FoyerLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(self.layer(inner))
    }
}

impl FoyerLayer {
    fn layer(&self, inner: Servicer) -> FoyerService {
        FoyerService {
            inner,
            cache: self.cache.clone(),
            size_limit: self.size_limit.clone(),
            deleted_keys: self.deleted_keys.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Inner {
    pub(crate) srv: Servicer,
    pub(crate) ctx: OperationContext,
    pub(crate) cache: HybridCache<FoyerKey, FoyerValue>,
    pub(crate) deleted_keys: Arc<Mutex<HashSet<FoyerKey>>>,
}

#[derive(Debug)]
pub struct FoyerService {
    inner: Servicer,
    cache: HybridCache<FoyerKey, FoyerValue>,
    size_limit: Range<usize>,
    deleted_keys: Arc<Mutex<HashSet<FoyerKey>>>,
}

impl FoyerService {
    fn operation_inner(&self, ctx: &OperationContext) -> Arc<Inner> {
        Arc::new(Inner {
            srv: self.inner.clone(),
            ctx: ctx.clone(),
            cache: self.cache.clone(),
            deleted_keys: self.deleted_keys.clone(),
        })
    }
}

impl Service for FoyerService {
    type Reader = full::FullReader;
    type Writer = Writer<oio::Writer>;
    type Lister = oio::Lister;
    type Deleter = Deleter<oio::Deleter>;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        Ok((
            RpRead::default(),
            full::FullReader::new(
                self.operation_inner(ctx),
                self.size_limit.clone(),
                path.to_string(),
                args,
            ),
        ))
    }

    async fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        let inner = self.operation_inner(ctx);
        let size_limit = self.size_limit.clone();
        let path = path.to_string();
        let (rp, w) = inner.srv.write(&inner.ctx, &path, args).await?;
        Ok((rp, Writer::new(w, path, inner, size_limit)))
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let inner = self.operation_inner(ctx);
        let (rp, d) = inner.srv.delete(&inner.ctx).await?;
        Ok((rp, Deleter::new(d, inner)))
    }

    async fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
        self.inner.copy(ctx, from, to, args, opts).await
    }

    async fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        self.inner.list(ctx, path, args).await
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }

    // TODO(MrCroxx): Implement copy, rename with foyer cache.
}

#[cfg(test)]
mod tests {
    use foyer::{
        BlockEngineConfig, DeviceBuilder, Error as FoyerError, ErrorKind as FoyerErrorKind,
        FsDeviceBuilder, HybridCache, HybridCacheBuilder, RecoverMode,
    };
    use opendal_core::raw::Layer as _;
    use opendal_core::raw::oio::Read as _;
    use opendal_core::raw::oio::ReadStream as _;
    use opendal_core::{Buffer, Operator, services::Memory};
    use size::consts::MiB;
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::error::extract_err;

    fn key(i: u8) -> String {
        format!("obj-{i}")
    }

    fn value(i: u8) -> Vec<u8> {
        // ~ 64KiB with metadata
        vec![i; 63 * 1024]
    }

    async fn memory_cache() -> HybridCache<FoyerKey, FoyerValue> {
        HybridCacheBuilder::new()
            .memory(1024 * 1024)
            .with_shards(1)
            .storage()
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap()
    }

    #[derive(Debug)]
    struct MockReadState {
        data: Buffer,
        stat_calls: AtomicUsize,
        open_calls: AtomicUsize,
        read_calls: AtomicUsize,
    }

    impl MockReadState {
        fn new(data: Buffer) -> Self {
            Self {
                data,
                stat_calls: AtomicUsize::new(0),
                open_calls: AtomicUsize::new(0),
                read_calls: AtomicUsize::new(0),
            }
        }

        fn metadata(&self) -> Metadata {
            Metadata::new(EntryMode::FILE).with_content_length(self.data.len() as _)
        }

        fn rp_read(&self) -> RpRead {
            RpRead::new(self.metadata())
        }

        fn read_range(&self, range: BytesRange) -> Buffer {
            self.data.slice(range.to_range_as_usize())
        }
    }

    #[derive(Debug, Clone)]
    struct MockReadService {
        state: Arc<MockReadState>,
    }

    impl MockReadService {
        fn new(data: impl Into<Buffer>) -> Self {
            Self {
                state: Arc::new(MockReadState::new(data.into())),
            }
        }
    }

    #[derive(Debug)]
    struct MockReadReader {
        state: Arc<MockReadState>,
    }

    impl oio::Read for MockReadReader {
        async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            self.state.open_calls.fetch_add(1, Ordering::Relaxed);
            let buffer = self.state.read_range(range);
            Ok((
                self.state.rp_read(),
                Box::new(buffer) as Box<dyn oio::ReadStreamDyn>,
            ))
        }

        async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
            self.state.read_calls.fetch_add(1, Ordering::Relaxed);
            if range.size().is_none() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "mock reader requires a bounded read range",
                ));
            }

            Ok((self.state.rp_read(), self.state.read_range(range)))
        }
    }

    impl Service for MockReadService {
        type Reader = MockReadReader;
        type Writer = ();
        type Lister = ();
        type Deleter = ();
        type Copier = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("mock")
        }

        fn capability(&self) -> Capability {
            Capability {
                read: true,
                stat: true,
                ..Default::default()
            }
        }

        async fn create_dir(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpCreateDir,
        ) -> Result<RpCreateDir> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
            self.state.stat_calls.fetch_add(1, Ordering::Relaxed);
            Ok(RpStat::new(self.state.metadata()))
        }

        async fn read(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpRead,
        ) -> Result<(RpRead, Self::Reader)> {
            Ok((
                self.state.rp_read(),
                MockReadReader {
                    state: self.state.clone(),
                },
            ))
        }

        async fn write(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpWrite,
        ) -> Result<(RpWrite, Self::Writer)> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn delete(&self, _: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn list(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpList,
        ) -> Result<(RpList, Self::Lister)> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn copy(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: OpCopy,
            _: OpCopier,
        ) -> Result<(RpCopy, Self::Copier)> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn rename(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: OpRename,
        ) -> Result<RpRename> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }
    }

    fn service_context(_: &Servicer) -> OperationContext {
        OperationContext::new(HttpClient::default(), Executor::default())
    }

    #[tokio::test]
    async fn test_full_reader_open_fallback_preserves_stream() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("0123456789"));
        let state = source.state.clone();
        let service = FoyerLayer::new(cache)
            .with_size_limit(0..1)
            .apply_service(source);
        let ctx = service_context(&service);

        let (_, reader) = service.read(&ctx, "test", OpRead::default()).await.unwrap();
        let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
        let buffer = stream.read_all().await.unwrap();

        assert_eq!(buffer.to_vec(), b"0123456789");
        assert_eq!(state.open_calls.load(Ordering::Relaxed), 1);
        assert_eq!(state.read_calls.load(Ordering::Relaxed), 0);
        assert_eq!(state.stat_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_full_reader_open_fills_cache() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("0123456789"));
        let state = source.state.clone();
        let service = FoyerLayer::new(cache)
            .with_size_limit(0..100)
            .apply_service(source);
        let ctx = service_context(&service);

        let (_, reader) = service.read(&ctx, "test", OpRead::default()).await.unwrap();
        let (_, mut stream) = reader.open(BytesRange::from(0_u64..2)).await.unwrap();
        let buffer = stream.read_all().await.unwrap();

        assert_eq!(buffer.to_vec(), b"01");
        assert_eq!(state.stat_calls.load(Ordering::Relaxed), 1);
        assert_eq!(state.open_calls.load(Ordering::Relaxed), 1);
        assert_eq!(state.read_calls.load(Ordering::Relaxed), 0);

        let (_, reader) = service.read(&ctx, "test", OpRead::default()).await.unwrap();
        let (_, mut stream) = reader.open(BytesRange::from(4_u64..7)).await.unwrap();
        let buffer = stream.read_all().await.unwrap();

        assert_eq!(buffer.to_vec(), b"456");
        assert_eq!(state.stat_calls.load(Ordering::Relaxed), 1);
        assert_eq!(state.open_calls.load(Ordering::Relaxed), 1);
        assert_eq!(state.read_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test() {
        let dir = tempfile::tempdir().unwrap();

        let cache = HybridCacheBuilder::new()
            .memory(10)
            .with_shards(1)
            .storage()
            .with_engine_config(
                BlockEngineConfig::new(
                    FsDeviceBuilder::new(dir.path())
                        .with_capacity(16 * MiB as usize)
                        .build()
                        .unwrap(),
                )
                .with_block_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()));

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
            .storage()
            .with_engine_config(
                BlockEngineConfig::new(
                    FsDeviceBuilder::new(dir.path())
                        .with_capacity(16 * MiB as usize)
                        .build()
                        .unwrap(),
                )
                .with_block_size(MiB as usize),
            )
            .with_recover_mode(RecoverMode::None)
            .build()
            .await
            .unwrap();

        // Set size limit: only cache files between 1KB and 10KB
        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()).with_size_limit(1024..10 * 1024));

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
        let fe = FoyerError::new(FoyerErrorKind::External, "external error").with_source(e);
        let oe = extract_err(fe);
        assert_eq!(oe.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn test_foyer_key_version_none_vs_empty() {
        let key_none = FoyerKey {
            path: "test/path".to_string(),
            version: None,
        };

        let key_empty = FoyerKey {
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

        let test_cases = vec![
            FoyerKey {
                path: "simple".to_string(),
                version: None,
            },
            FoyerKey {
                path: "with/slash/path".to_string(),
                version: None,
            },
            FoyerKey {
                path: "versioned".to_string(),
                version: Some("v1.0.0".to_string()),
            },
            FoyerKey {
                path: "empty-version".to_string(),
                version: Some("".to_string()),
            },
            FoyerKey {
                path: "".to_string(),
                version: None,
            },
            FoyerKey {
                path: "unicode/路径/🚀".to_string(),
                version: Some("版本-1".to_string()),
            },
            FoyerKey {
                path: "long/".to_string().repeat(100),
                version: Some("long-version-".to_string().repeat(50)),
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
}
