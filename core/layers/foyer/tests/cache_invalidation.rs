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

use foyer::{HybridCache, HybridCacheBuilder, RecoverMode};
use futures::StreamExt;
use opendal_core::raw::{
    OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite, RpCreateDir,
    RpPresign, RpRename, RpStat, Service, ServiceInfo, Servicer, oio,
};
use opendal_core::services::Memory;
use opendal_core::{
    Buffer, Builder, Capability, Error, ErrorKind, Metadata, OperationContext, Operator, Result,
};
use opendal_layer_foyer::{Deleter, FoyerKey, FoyerLayer, FoyerService, FoyerValue, Writer};
use opendal_service_fs::Fs;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

async fn cached_fs(dir: &tempfile::TempDir) -> (Operator, HybridCache<FoyerKey, FoyerValue>) {
    cached_fs_with_limit(dir, 0..usize::MAX).await
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

async fn cached_memory_with_limit(
    size_limit: Range<usize>,
) -> (Operator, HybridCache<FoyerKey, FoyerValue>) {
    let cache = memory_cache().await;
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(FoyerLayer::new(cache.clone()).with_size_limit(size_limit));
    (op, cache)
}

async fn cached_fs_with_limit(
    dir: &tempfile::TempDir,
    size_limit: Range<usize>,
) -> (Operator, HybridCache<FoyerKey, FoyerValue>) {
    let cache = memory_cache().await;
    let op = Operator::new(Fs::default().root(dir.path().to_str().unwrap()))
        .unwrap()
        .layer(FoyerLayer::new(cache.clone()).with_size_limit(size_limit));
    (op, cache)
}

fn unversioned_key(path: &str) -> FoyerKey {
    FoyerKey {
        path: path.to_string(),
        version: None,
    }
}

#[tokio::test]
async fn rename_invalidates_source_and_destination() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs(&dir).await;
    let source = unversioned_key("source");
    let destination = unversioned_key("destination");
    op.write(&source.path, "original").await.unwrap();
    op.write(&destination.path, "stale").await.unwrap();
    assert!(cache.contains(&source));
    assert!(cache.contains(&destination));

    op.rename(&source.path, &destination.path).await.unwrap();

    assert_eq!(
        op.read(&source.path).await.unwrap_err().kind(),
        ErrorKind::NotFound,
    );
    assert_eq!(
        op.read(&destination.path).await.unwrap().to_vec(),
        b"original",
    );
    assert!(!cache.contains(&source));
    assert!(!cache.contains(&destination));

    op.write(&destination.path, "new write").await.unwrap();
    assert!(cache.contains(&destination));
    assert_eq!(
        op.read(&destination.path).await.unwrap().to_vec(),
        b"new write",
    );
    op.delete(&destination.path).await.unwrap();
    assert!(!cache.contains(&destination));
    assert_eq!(
        op.read(&destination.path).await.unwrap_err().kind(),
        ErrorKind::NotFound,
    );
}

#[tokio::test]
async fn operator_copy_invalidates_destination_only() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs(&dir).await;
    let source = unversioned_key("source");
    let destination = unversioned_key("destination");
    op.write(&source.path, "original").await.unwrap();
    op.write(&destination.path, "stale").await.unwrap();
    assert!(cache.contains(&source));
    assert!(cache.contains(&destination));

    let metadata = op.copy(&source.path, &destination.path).await.unwrap();

    assert_eq!(metadata.content_length(), 8);
    assert_eq!(
        op.read(&destination.path).await.unwrap().to_vec(),
        b"original",
    );
    assert!(cache.contains(&source));
    assert!(!cache.contains(&destination));
}

#[tokio::test]
async fn copier_stream_invalidates_destination_at_end_of_stream() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs(&dir).await;
    let destination = unversioned_key("destination");
    op.write("source", "original").await.unwrap();
    op.write(&destination.path, "stale").await.unwrap();

    let mut copier = op.copier("source", &destination.path).await.unwrap();
    assert!(cache.contains(&destination));
    assert!(
        StreamExt::next(&mut copier)
            .await
            .transpose()
            .unwrap()
            .is_none()
    );

    assert_eq!(
        op.read(&destination.path).await.unwrap().to_vec(),
        b"original",
    );
    assert!(!cache.contains(&destination));

    op.write(&destination.path, "new write").await.unwrap();
    assert_eq!(copier.close().await.unwrap().content_length(), 8);
    assert!(cache.contains(&destination));
}

#[tokio::test]
async fn failed_mutations_and_aborted_copy_keep_cached_destination() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs(&dir).await;
    let destination = unversioned_key("destination");
    op.write(&destination.path, "cached").await.unwrap();

    let err = op.rename("missing", &destination.path).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(cache.contains(&destination));
    let err = op.copy("missing", &destination.path).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(cache.contains(&destination));

    op.write("source", "original").await.unwrap();
    let mut copier = op.copier("source", &destination.path).await.unwrap();
    copier.abort().await.unwrap();
    assert!(cache.contains(&destination));
    assert_eq!(
        op.read(&destination.path).await.unwrap().to_vec(),
        b"cached"
    );
}

/// A chunk that pushes the cumulative write size over the size limit must
/// stick: a later in-range chunk cannot turn the writer back into a caching
/// one and leave a truncated suffix in the cache.
#[tokio::test]
async fn overflow_chunk_then_small_chunk_does_not_truncate_cached_data() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs_with_limit(&dir, 0..10).await;
    let key = unversioned_key("k");

    let mut writer = op.writer("k").await.unwrap();
    writer.write(vec![1u8; 20]).await.unwrap();
    writer.write(vec![2u8; 5]).await.unwrap();
    writer.close().await.unwrap();

    assert!(!cache.contains(&key));
    let mut expected = vec![1u8; 20];
    expected.extend([2u8; 5]);
    assert_eq!(op.read("k").await.unwrap().to_vec(), expected);
    // The oversized object must not be refilled into the cache either.
    assert!(!cache.contains(&key));
}

/// Overwriting a cached object with a write that exceeds the size limit must
/// invalidate the superseded entry instead of leaving stale data behind.
#[tokio::test]
async fn overflow_overwrite_invalidates_stale_cache_entry() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs_with_limit(&dir, 0..10).await;
    let key = unversioned_key("k");

    op.write("k", "old").await.unwrap();
    assert!(cache.contains(&key));

    op.write("k", vec![7u8; 20]).await.unwrap();

    assert!(!cache.contains(&key));
    assert_eq!(op.read("k").await.unwrap().to_vec(), vec![7u8; 20]);
    assert!(!cache.contains(&key));
}

/// The size limit applies to the object's total size: several individually
/// in-range chunks that overflow in sum must not be cached, and a stale entry
/// must still be invalidated.
#[tokio::test]
async fn cumulative_chunks_exceeding_limit_are_not_cached() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs_with_limit(&dir, 0..10).await;
    let key = unversioned_key("k");

    op.write("k", "old").await.unwrap();
    assert!(cache.contains(&key));

    let mut writer = op.writer("k").await.unwrap();
    writer.write(vec![1u8; 6]).await.unwrap();
    writer.write(vec![2u8; 6]).await.unwrap();
    writer.close().await.unwrap();

    assert!(!cache.contains(&key));
    let mut expected = vec![1u8; 6];
    expected.extend([2u8; 6]);
    assert_eq!(op.read("k").await.unwrap().to_vec(), expected);
}

/// A multi-chunk write whose total size stays inside the limit still
/// populates the cache and reads back through it.
#[tokio::test]
async fn in_limit_multi_chunk_write_populates_cache() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs_with_limit(&dir, 0..10).await;
    let key = unversioned_key("k");

    let mut writer = op.writer("k").await.unwrap();
    writer.write(vec![1u8; 3]).await.unwrap();
    writer.write(vec![2u8; 4]).await.unwrap();
    writer.close().await.unwrap();

    let mut expected = vec![1u8; 3];
    expected.extend([2u8; 4]);
    assert!(cache.contains(&key));
    assert_eq!(op.read("k").await.unwrap().to_vec(), expected);
    assert!(cache.contains(&key));
}

/// A write that ends up below the size limit's start also supersedes the
/// cached entry, so the smaller object must be evicted and not re-cached.
#[tokio::test]
async fn write_below_size_limit_invalidates_stale_cache_entry() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs_with_limit(&dir, 10..20).await;
    let key = unversioned_key("k");

    op.write("k", vec![1u8; 15]).await.unwrap();
    assert!(cache.contains(&key));

    op.write("k", vec![2u8; 5]).await.unwrap();

    assert!(!cache.contains(&key));
    assert_eq!(op.read("k").await.unwrap().to_vec(), vec![2u8; 5]);
    assert!(!cache.contains(&key));
}

/// An append write only transfers the new suffix, so the writer must not
/// cache it as the whole object; the superseded entry is evicted and a later
/// read refills the full content.
#[tokio::test]
async fn append_write_evicts_superseded_cache_entry() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs_with_limit(&dir, 0..10).await;
    let key = unversioned_key("k");

    op.write("k", "old").await.unwrap();
    assert!(cache.contains(&key));

    op.write_with("k", "-new").append(true).await.unwrap();

    assert_eq!(op.read("k").await.unwrap().to_vec(), b"old-new");
    // The appended object stays inside the limit, so the refill repopulates
    // the cache with the full content.
    assert!(cache.contains(&key));
    assert_eq!(op.read("k").await.unwrap().to_vec(), b"old-new");
}

/// An aborted write does not modify the stored object, so the cached entry
/// must survive untouched.
#[tokio::test]
async fn aborted_write_keeps_cached_entry() {
    // Fs requires `atomic_write_dir` to support abort; memory aborts natively.
    let (op, cache) = cached_memory_with_limit(0..10).await;
    let key = unversioned_key("k");

    op.write("k", "old").await.unwrap();
    assert!(cache.contains(&key));

    let mut writer = op.writer("k").await.unwrap();
    writer.write(vec![1u8; 5]).await.unwrap();
    writer.abort().await.unwrap();

    assert!(cache.contains(&key));
    assert_eq!(op.read("k").await.unwrap().to_vec(), b"old");
}

#[derive(Debug, Default)]
struct CloseFaultBuilder {
    fail_next_close: bool,
    pending_next_close: bool,
}

impl Builder for CloseFaultBuilder {
    type Config = ();

    fn build(self) -> Result<impl Service> {
        Ok(CloseFaultService {
            inner: Arc::new(Memory::default().build()?),
            fail_next_close: Arc::new(AtomicBool::new(self.fail_next_close)),
            pending_next_close: Arc::new(AtomicBool::new(self.pending_next_close)),
        })
    }
}

#[derive(Debug)]
struct CloseFaultService {
    inner: Servicer,
    fail_next_close: Arc<AtomicBool>,
    pending_next_close: Arc<AtomicBool>,
}

struct CloseFaultWriter {
    inner: oio::Writer,
    fail_next_close: Arc<AtomicBool>,
    pending_next_close: Arc<AtomicBool>,
}

impl oio::Write for CloseFaultWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        if self.pending_next_close.swap(false, Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.fail_next_close.swap(false, Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Unexpected, "injected close failure").set_temporary());
        }
        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

impl Service for CloseFaultService {
    type Reader = oio::Reader;
    type Writer = CloseFaultWriter;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
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

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(CloseFaultWriter {
            inner: self.inner.write(ctx, path, args)?,
            fail_next_close: self.fail_next_close.clone(),
            pending_next_close: self.pending_next_close.clone(),
        })
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args)
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
}

#[tokio::test]
async fn retried_close_caches_buffered_write() {
    let cache = memory_cache().await;
    let op = Operator::new(CloseFaultBuilder {
        fail_next_close: true,
        ..Default::default()
    })
    .unwrap()
    .layer(FoyerLayer::new(cache.clone()));
    let key = unversioned_key("key");

    let mut writer = op.writer("key").await.unwrap();
    writer.write("hello").await.unwrap();
    let err = writer.close().await.unwrap_err();
    assert!(err.is_temporary());
    assert_eq!(err.message(), "injected close failure");
    writer.close().await.unwrap();

    assert!(cache.contains(&key));
    assert_eq!(op.read("key").await.unwrap().to_vec(), b"hello");
}

#[tokio::test]
async fn cancelled_close_preserves_buffered_write_for_retry() {
    let cache = memory_cache().await;
    let op = Operator::new(CloseFaultBuilder {
        pending_next_close: true,
        ..Default::default()
    })
    .unwrap()
    .layer(FoyerLayer::new(cache.clone()));

    let mut writer = op.writer("key").await.unwrap();
    writer.write("hello").await.unwrap();
    {
        let close = writer.close();
        futures::pin_mut!(close);
        assert!(futures::poll!(&mut close).is_pending());
    }
    writer.close().await.unwrap();

    assert_eq!(op.read("key").await.unwrap().to_vec(), b"hello");
}

#[allow(deprecated)]
#[tokio::test]
async fn recursive_delete_does_not_serve_cached_descendants() {
    let (op, cache) = cached_memory_with_limit(0..usize::MAX).await;
    let entries = [("dir/a", "v1"), ("dir/sub/b", "v2"), ("dir2/c", "v3")];

    for (path, value) in entries {
        op.write(path, value).await.unwrap();
    }
    for (path, value) in entries {
        assert_eq!(op.read(path).await.unwrap().to_vec(), value.as_bytes());
    }

    op.remove_all("dir/").await.unwrap();

    for path in ["dir/a", "dir/sub/b"] {
        assert_eq!(op.read(path).await.unwrap_err().kind(), ErrorKind::NotFound);
        assert!(!cache.contains(&unversioned_key(path)));
    }
    assert_eq!(op.read("dir2/c").await.unwrap().to_vec(), b"v3");
}

#[test]
fn operation_types_remain_available() {
    fn assert_operation_types<
        S: Service<
                Deleter = Deleter<oio::Deleter>,
                Writer = Writer<oio::Writer>,
                Copier = oio::Copier,
            >,
    >() {
    }

    assert_operation_types::<FoyerService>();
}
