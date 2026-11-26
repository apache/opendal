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

#![allow(deprecated)]

use foyer::{HybridCache, HybridCacheBuilder, RecoverMode};
use futures::StreamExt;
use opendal_core::raw::oio::Delete as _;
use opendal_core::raw::oio::ReadStream as _;
use opendal_core::raw::{
    OpCopy, OpCreateDir, OpDelete, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite,
    RpCreateDir, RpPresign, RpRead, RpRename, RpStat, Service, ServiceInfo, Servicer, oio,
};
use opendal_core::services::Memory;
use opendal_core::{
    Buffer, Builder, BytesRange, Capability, Error, ErrorKind, Metadata, OperationContext,
    Operator, Result,
};
use opendal_layer_foyer::{FoyerKey, FoyerLayer, FoyerValue};
use opendal_service_fs::Fs;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

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

#[derive(Debug)]
struct ReadGate {
    path: String,
    started: Notify,
    release: Notify,
    finished: Notify,
    armed: AtomicBool,
    open_calls: AtomicUsize,
}

impl ReadGate {
    fn new(path: &str) -> Arc<Self> {
        Arc::new(Self {
            path: path.to_string(),
            started: Notify::new(),
            release: Notify::new(),
            finished: Notify::new(),
            armed: AtomicBool::new(true),
            open_calls: AtomicUsize::new(0),
        })
    }
}

struct GatedStream {
    data: Option<Buffer>,
    gate: Arc<ReadGate>,
    notified: bool,
}

impl oio::ReadStream for GatedStream {
    async fn read(&mut self) -> Result<Buffer> {
        if !self.notified {
            self.notified = true;
            self.gate.started.notify_one();
            self.gate.release.notified().await;
        }
        Ok(self.data.take().unwrap_or_default())
    }
}

impl Drop for GatedStream {
    fn drop(&mut self) {
        self.gate.finished.notify_one();
    }
}

struct GatedReader {
    inner: oio::Reader,
    path: String,
    gate: Option<Arc<ReadGate>>,
}

impl oio::Read for GatedReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        if let Some(gate) = &self.gate
            && self.path == gate.path
        {
            gate.open_calls.fetch_add(1, Ordering::SeqCst);
            let (rp, mut stream) = self.inner.open(range).await?;
            if gate.armed.swap(false, Ordering::SeqCst) {
                // Snapshot pre-mutation bytes before pausing so the in-flight
                // fill carries genuinely stale bytes when released.
                let snapshot = stream.read_all().await?;
                return Ok((
                    rp,
                    Box::new(GatedStream {
                        data: Some(snapshot),
                        gate: gate.clone(),
                        notified: false,
                    }) as Box<dyn oio::ReadStreamDyn>,
                ));
            }
            return Ok((rp, stream));
        }
        self.inner.open(range).await
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        self.inner.read(range).await
    }
}

#[derive(Debug, Default)]
struct CloseFaultBuilder {
    fs_root: Option<String>,
    fail_next_close: bool,
    pending_next_close: bool,
    recursive_delete: Option<Arc<RecursiveDeleteState>>,
    buffered_delete: Option<Arc<RecursiveDeleteState>>,
    stat_gate: Option<(String, Arc<Notify>, Arc<Notify>)>,
    read_gate: Option<Arc<ReadGate>>,
}

impl Builder for CloseFaultBuilder {
    type Config = ();

    fn build(self) -> Result<impl Service> {
        let inner: Servicer = match self.fs_root {
            Some(root) => Arc::new(Fs::default().root(&root).build()?),
            None => Arc::new(Memory::default().build()?),
        };
        Ok(CloseFaultService {
            inner,
            fail_next_close: Arc::new(AtomicBool::new(self.fail_next_close)),
            pending_next_close: Arc::new(AtomicBool::new(self.pending_next_close)),
            recursive_delete: self.recursive_delete,
            buffered_delete: self.buffered_delete,
            stat_gate: self.stat_gate,
            read_gate: self.read_gate,
        })
    }
}

#[derive(Debug)]
struct CloseFaultService {
    inner: Servicer,
    fail_next_close: Arc<AtomicBool>,
    pending_next_close: Arc<AtomicBool>,
    recursive_delete: Option<Arc<RecursiveDeleteState>>,
    buffered_delete: Option<Arc<RecursiveDeleteState>>,
    stat_gate: Option<(String, Arc<Notify>, Arc<Notify>)>,
    read_gate: Option<Arc<ReadGate>>,
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

#[derive(Debug, Default)]
struct RecursiveDeleteState {
    close_started: Notify,
    finish_close: Notify,
    fail_close: bool,
}

struct DelayedRecursiveDeleter {
    inner: oio::Deleter,
    state: Option<Arc<RecursiveDeleteState>>,
    buffer_all: bool,
    pending: Vec<(String, OpDelete)>,
}

impl oio::Delete for DelayedRecursiveDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        if self.state.is_some() && (self.buffer_all || args.recursive()) {
            self.pending.push((path.to_string(), args));
            return Ok(());
        }
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> Result<()> {
        let Some(state) = self.state.clone() else {
            return self.inner.close().await;
        };

        state.close_started.notify_one();
        state.finish_close.notified().await;
        for (path, args) in self.pending.drain(..) {
            self.inner.delete(&path, args).await?;
        }
        self.inner.close().await?;
        if state.fail_close {
            return Err(
                Error::new(ErrorKind::Unexpected, "injected recursive close failure")
                    .set_temporary(),
            );
        }
        Ok(())
    }
}

impl Service for CloseFaultService {
    type Reader = GatedReader;
    type Writer = CloseFaultWriter;
    type Lister = oio::Lister;
    type Deleter = DelayedRecursiveDeleter;
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
        if let Some((gate_path, started, release)) = &self.stat_gate
            && path == gate_path
        {
            started.notify_one();
            release.notified().await;
        }
        self.inner.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        Ok(GatedReader {
            inner: self.inner.read(ctx, path, args)?,
            path: path.to_string(),
            gate: self.read_gate.clone(),
        })
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(CloseFaultWriter {
            inner: self.inner.write(ctx, path, args)?,
            fail_next_close: self.fail_next_close.clone(),
            pending_next_close: self.pending_next_close.clone(),
        })
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let (state, buffer_all) = if let Some(state) = &self.buffered_delete {
            (Some(state.clone()), true)
        } else {
            (self.recursive_delete.clone(), false)
        };
        Ok(DelayedRecursiveDeleter {
            inner: self.inner.delete(ctx)?,
            state,
            buffer_all,
            pending: vec![],
        })
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

async fn fault_operator(
    builder: CloseFaultBuilder,
) -> (Operator, HybridCache<FoyerKey, FoyerValue>) {
    let cache = memory_cache().await;
    let op = Operator::new(builder)
        .unwrap()
        .layer(FoyerLayer::new(cache.clone()));
    (op, cache)
}

#[tokio::test]
async fn retried_close_caches_buffered_write() {
    let (op, cache) = fault_operator(CloseFaultBuilder {
        fail_next_close: true,
        ..Default::default()
    })
    .await;
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
    let (op, cache) = fault_operator(CloseFaultBuilder {
        pending_next_close: true,
        ..Default::default()
    })
    .await;
    let key = unversioned_key("key");

    let mut writer = op.writer("key").await.unwrap();
    writer.write("hello").await.unwrap();
    {
        let close = writer.close();
        futures::pin_mut!(close);
        assert!(futures::poll!(&mut close).is_pending());
    }
    writer.close().await.unwrap();

    assert!(cache.contains(&key));
    assert_eq!(op.read("key").await.unwrap().to_vec(), b"hello");
}

async fn warm_recursive_delete_cache(op: &Operator) {
    for (path, value) in [("dir/a", "v1"), ("dir/sub/b", "v2"), ("dir2/c", "v3")] {
        op.write(path, value).await.unwrap();
    }
    for (path, value) in [("dir/a", "v1"), ("dir/sub/b", "v2"), ("dir2/c", "v3")] {
        assert_eq!(op.read(path).await.unwrap().to_vec(), value.as_bytes());
    }
}

async fn assert_delayed_recursive_close_clears_cache(fail_close: bool) {
    let state = Arc::new(RecursiveDeleteState {
        fail_close,
        ..Default::default()
    });
    let (op, cache) = fault_operator(CloseFaultBuilder {
        recursive_delete: Some(state.clone()),
        ..Default::default()
    })
    .await;
    warm_recursive_delete_cache(&op).await;

    let delete = {
        let op = op.clone();
        tokio::spawn(async move { op.delete_with("dir/").recursive(true).await })
    };
    state.close_started.notified().await;

    // Force the mid-close read to fetch the object that the buffered backend
    // deleter has not removed yet and repopulate the cache.
    let key = unversioned_key("dir/a");
    cache.remove(&key);
    assert_eq!(op.read("dir/a").await.unwrap().to_vec(), b"v1");
    assert!(cache.contains(&key));

    state.finish_close.notify_one();
    let result = delete.await.unwrap();
    if fail_close {
        let err = result.expect_err("injected close failure must be returned");
        assert!(err.is_temporary());
        assert_eq!(err.message(), "injected recursive close failure");
    } else {
        result.unwrap();
    }

    assert!(!cache.contains(&key));
    for path in ["dir/a", "dir/sub/b"] {
        assert_eq!(op.read(path).await.unwrap_err().kind(), ErrorKind::NotFound);
    }
    assert_eq!(op.read("dir2/c").await.unwrap().to_vec(), b"v3");
}

#[tokio::test]
async fn recursive_close_clears_cache_repopulated_before_backend_delete() {
    assert_delayed_recursive_close_clears_cache(false).await;
}

#[tokio::test]
async fn failed_recursive_close_clears_cache_repopulated_before_backend_delete() {
    assert_delayed_recursive_close_clears_cache(true).await;
}

#[tokio::test]
async fn recursive_delete_does_not_serve_cached_descendants() {
    let (op, cache) = cached_memory_with_limit(0..usize::MAX).await;
    warm_recursive_delete_cache(&op).await;

    op.delete_with("dir/").recursive(true).await.unwrap();

    for path in ["dir/a", "dir/sub/b"] {
        assert_eq!(op.read(path).await.unwrap_err().kind(), ErrorKind::NotFound);
        assert!(!cache.contains(&unversioned_key(path)));
    }
    assert_eq!(op.read("dir2/c").await.unwrap().to_vec(), b"v3");
}

#[tokio::test]
async fn buffered_delete_close_does_not_allow_mid_close_cache_repopulation() {
    let delete_state = Arc::new(RecursiveDeleteState::default());
    let stat_started = Arc::new(Notify::new());
    let stat_release = Arc::new(Notify::new());
    let (op, cache) = fault_operator(CloseFaultBuilder {
        buffered_delete: Some(delete_state.clone()),
        stat_gate: Some((
            "other".to_string(),
            stat_started.clone(),
            stat_release.clone(),
        )),
        ..Default::default()
    })
    .await;

    op.write("target", "v1").await.unwrap();
    op.write("other", "other-data").await.unwrap();
    // Clear "other" from cache so reading it triggers a backend stat that we can gate.
    cache.remove(&unversioned_key("other"));
    assert!(cache.contains(&unversioned_key("target")));

    let unrelated_read = tokio::spawn({
        let op = op.clone();
        async move { op.read("other").await.unwrap() }
    });
    stat_started.notified().await;

    // Start a buffered delete of "target"; it pauses inside `deleter.close().await`
    // before the backend removes "target".
    let delete_task = tokio::spawn({
        let op = op.clone();
        async move { op.delete("target").await.unwrap() }
    });
    delete_state.close_started.notified().await;

    // Finish the unrelated read while `deleter.close().await` is still running.
    stat_release.notify_one();
    assert_eq!(unrelated_read.await.unwrap().to_vec(), b"other-data");

    // Read "target" before `deleter.close().await` finishes deleting it from backend.
    assert_eq!(op.read("target").await.unwrap().to_vec(), b"v1");

    // Complete backend deletion.
    delete_state.finish_close.notify_one();
    delete_task.await.unwrap();

    assert!(
        !cache.contains(&unversioned_key("target")),
        "a read during buffered deleter close must not leave stale data cached after close"
    );
    assert_eq!(
        op.read("target").await.unwrap_err().kind(),
        ErrorKind::NotFound,
    );
}

#[tokio::test]
async fn buffered_delete_close_reinvalidates_mid_close_write() {
    let delete_state = Arc::new(RecursiveDeleteState::default());
    let (op, cache) = fault_operator(CloseFaultBuilder {
        buffered_delete: Some(delete_state.clone()),
        ..Default::default()
    })
    .await;
    let key = unversioned_key("target");

    op.write("target", "v1").await.unwrap();
    assert!(cache.contains(&key));

    // Start a buffered delete of "target"; it calls `begin_delete` and pauses
    // inside `deleter.close().await` before the backend removes "target".
    let delete_task = tokio::spawn({
        let op = op.clone();
        async move { op.delete("target").await.unwrap() }
    });
    delete_state.close_started.notified().await;

    // A write arriving mid-close repopulates the cache before the buffered
    // backend delete executes.
    op.write("target", "v2").await.unwrap();
    assert!(cache.contains(&key));

    // Complete backend deletion. `DeleteGuard::drop` must re-invalidate
    // "target" so the deleted object is not served stale from cache.
    delete_state.finish_close.notify_one();
    delete_task.await.unwrap();

    assert!(
        !cache.contains(&key),
        "a write during buffered deleter close must be re-invalidated when close completes"
    );
    assert_eq!(
        op.read("target").await.unwrap_err().kind(),
        ErrorKind::NotFound,
    );
}

#[tokio::test]
async fn cancelled_buffered_delete_close_reinvalidates_and_preserves_retry() {
    let delete_state = Arc::new(RecursiveDeleteState::default());
    let (op, cache) = fault_operator(CloseFaultBuilder {
        buffered_delete: Some(delete_state.clone()),
        ..Default::default()
    })
    .await;
    let key = unversioned_key("target");

    op.write("target", "v1").await.unwrap();
    let mut deleter = op.deleter().await.unwrap();
    deleter.delete("target").await.unwrap();

    {
        let close = deleter.close();
        futures::pin_mut!(close);
        assert!(futures::poll!(&mut close).is_pending());
        // Mid-close write repopulates cache before `close` is cancelled.
        op.write("target", "v2").await.unwrap();
        assert!(cache.contains(&key));
    }

    // Dropping the in-flight `close()` future must still run `DeleteGuard::drop`
    // and re-invalidate the key.
    assert!(
        !cache.contains(&key),
        "cancelling deleter close must still re-invalidate target keys"
    );

    // Retrying `close()` after cancellation must still invalidate any write
    // performed before the retry completes.
    op.write("target", "v3").await.unwrap();
    assert!(cache.contains(&key));
    delete_state.finish_close.notify_one();
    deleter.close().await.unwrap();

    assert!(!cache.contains(&key));
    assert_eq!(
        op.read("target").await.unwrap_err().kind(),
        ErrorKind::NotFound,
    );
}

#[tokio::test]
async fn append_evict_during_inflight_read_does_not_cache_stale_data() {
    let dir = tempfile::tempdir().unwrap();
    let gate = ReadGate::new("target");
    let (op, cache) = fault_operator(CloseFaultBuilder {
        fs_root: Some(dir.path().to_str().unwrap().to_string()),
        read_gate: Some(gate.clone()),
        ..Default::default()
    })
    .await;
    let key = unversioned_key("target");

    op.write("target", "old").await.unwrap();
    cache.remove(&key);

    // Start a read that snapshots "old" from backend and pauses before committing to cache.
    let read_task = tokio::spawn({
        let op = op.clone();
        async move { op.read("target").await.unwrap() }
    });
    gate.started.notified().await;

    // Append while the read is still in flight; this exercises `PinnedKind::Evict`.
    op.write_with("target", "-new").append(true).await.unwrap();

    // Release the older read. It returns the pre-append bytes it read, but must not
    // leave stale bytes cached.
    gate.release.notify_one();
    assert_eq!(read_task.await.unwrap().to_vec(), b"old");
    assert!(
        !cache.contains(&key),
        "older read completing after append eviction must not cache stale bytes"
    );

    // Subsequent reads must see the appended bytes and refill the cache.
    assert_eq!(op.read("target").await.unwrap().to_vec(), b"old-new");
    assert!(
        cache.contains(&key),
        "subsequent read after eviction release must refill the cache"
    );
    let opens_after_refill = gate.open_calls.load(Ordering::SeqCst);
    assert_eq!(
        opens_after_refill, 2,
        "initial read and post-eviction refill should each open backend once"
    );
    assert_eq!(op.read("target").await.unwrap().to_vec(), b"old-new");
    assert_eq!(
        gate.open_calls.load(Ordering::SeqCst),
        opens_after_refill,
        "refilled entry after eviction must serve subsequent reads from cache"
    );
}

#[tokio::test]
async fn cancelled_read_racing_with_evict_does_not_leave_stale_entry() {
    let dir = tempfile::tempdir().unwrap();
    let gate = ReadGate::new("target");
    let (op, cache) = fault_operator(CloseFaultBuilder {
        fs_root: Some(dir.path().to_str().unwrap().to_string()),
        read_gate: Some(gate.clone()),
        ..Default::default()
    })
    .await;
    let key = unversioned_key("target");

    op.write("target", "old").await.unwrap();
    cache.remove(&key);

    // Start a read that snapshots "old" and pauses before committing to cache.
    let read_task = tokio::spawn({
        let op = op.clone();
        async move { op.read("target").await.unwrap() }
    });
    gate.started.notified().await;

    // Cancel the caller future so only `fetch_guard` inside Foyer's detached fetch remains.
    read_task.abort();
    let _ = read_task.await;

    // Append triggers `Evict` while the detached fetch still holds stale "old" bytes.
    op.write_with("target", "-new").append(true).await.unwrap();

    // Attach a passive waiter to Foyer's inflight so we observe the detached fetch finishing.
    let wait_inflight = cache.get(&key);
    gate.release.notify_one();
    let _ = wait_inflight.await;
    gate.finished.notified().await;

    assert!(
        !cache.contains(&key),
        "detached fetch from a cancelled read must not insert stale bytes after eviction"
    );

    // Subsequent reads must observe the appended bytes and refill the cache.
    assert_eq!(op.read("target").await.unwrap().to_vec(), b"old-new");
    assert!(
        cache.contains(&key),
        "subsequent read after cancelled fetch and eviction must refill the cache"
    );
}

#[tokio::test]
async fn old_fill_racing_with_store_does_not_overwrite_fresh_entry() {
    let gate = ReadGate::new("target");
    let (op, cache) = fault_operator(CloseFaultBuilder {
        read_gate: Some(gate.clone()),
        ..Default::default()
    })
    .await;
    let key = unversioned_key("target");

    op.write("target", "old").await.unwrap();
    cache.remove(&key);

    // Start an older fill that snapshots "old" and pauses right before committing.
    let read_task = tokio::spawn({
        let op = op.clone();
        async move { op.read("target").await.unwrap() }
    });
    gate.started.notified().await;

    // Overwrite with "fresh" via `store()`; Foyer's `insert` populates "fresh" and
    // completes waiting readers while the detached older fill is still paused with "old".
    op.write("target", "fresh").await.unwrap();
    assert_eq!(read_task.await.unwrap().to_vec(), b"fresh");
    assert!(cache.contains(&key));

    // Release the detached older fill carrying "old" and wait for its fetch closure to finish.
    gate.release.notify_one();
    gate.finished.notified().await;
    tokio::task::yield_now().await;

    // The fresh entry written by `store()` must remain cached without being overwritten.
    assert!(cache.contains(&key));
    let opens_before = gate.open_calls.load(Ordering::SeqCst);
    assert_eq!(op.read("target").await.unwrap().to_vec(), b"fresh");
    assert_eq!(
        gate.open_calls.load(Ordering::SeqCst),
        opens_before,
        "subsequent read must hit the fresh cached entry rather than stale bytes or backend"
    );
}

#[tokio::test]
async fn cancelled_coalesced_waiter_does_not_deadlock_on_store() {
    let gate = ReadGate::new("target");
    let (op, cache) = fault_operator(CloseFaultBuilder {
        read_gate: Some(gate.clone()),
        ..Default::default()
    })
    .await;
    let key = unversioned_key("target");

    op.write("target", "old").await.unwrap();
    cache.remove(&key);

    // Leader read pauses inside backend stream with "old".
    let leader = tokio::spawn({
        let op = op.clone();
        async move { op.read("target").await.unwrap() }
    });
    gate.started.notified().await;

    // Second reader joins Foyer's inflight entry, placing its fetch future (and
    // `ReadGuard`) inside Foyer's `InflightManager`, and is then cancelled.
    {
        let waiter = op.read("target");
        futures::pin_mut!(waiter);
        assert!(futures::poll!(&mut waiter).is_pending());
    }

    // `store()` calls `cache.insert()`, which drops the cancelled waiter's stored
    // fetch future while holding Foyer's shard and inflight locks; this must not deadlock.
    tokio::time::timeout(Duration::from_secs(2), op.write("target", "fresh"))
        .await
        .expect("store() must not deadlock when Foyer drops a cancelled waiter's guard")
        .unwrap();

    gate.release.notify_one();
    let _ = leader.await.unwrap();
    gate.finished.notified().await;

    assert!(cache.contains(&key));
    assert_eq!(op.read("target").await.unwrap().to_vec(), b"fresh");
}

#[tokio::test]
async fn oneshot_delete_evicts_cached_entry_before_close() {
    let (op, cache) = cached_memory_with_limit(0..usize::MAX).await;
    let key = unversioned_key("a");

    op.write("a", "hello").await.unwrap();
    assert!(cache.contains(&key));

    let mut deleter = op.deleter().await.unwrap();
    deleter.delete("a").await.unwrap();

    // `Memory`'s `OneShotDeleter` deletes during `delete()`, so the cache must
    // not serve stale bytes even before `close()` is called.
    assert!(!cache.contains(&key));
    assert_eq!(op.read("a").await.unwrap_err().kind(), ErrorKind::NotFound);

    deleter.close().await.unwrap();
}

#[tokio::test]
async fn delete_iter_error_does_not_leave_earlier_oneshot_deletes_cached() {
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs(&dir).await;
    let key_a = unversioned_key("a");

    op.write("a", "hello").await.unwrap();
    op.write("dir/child", "nested").await.unwrap();
    assert!(cache.contains(&key_a));

    // `Fs` fails non-recursive deletion of non-empty `"dir/"` after already
    // deleting `"a"`; `delete_iter` returns on error without calling `close()`.
    let err = op.delete_iter(["a", "dir/"]).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);

    assert!(!cache.contains(&key_a));
    assert_eq!(op.read("a").await.unwrap_err().kind(), ErrorKind::NotFound);
}

#[derive(Debug, Default)]
struct PartialBatchDeleteBuilder {
    fail_path: String,
}

impl Builder for PartialBatchDeleteBuilder {
    type Config = ();

    fn build(self) -> Result<impl Service> {
        let inner: Servicer = Arc::new(Memory::default().build()?);
        Ok(PartialBatchDeleteService {
            inner,
            fail_path: self.fail_path,
        })
    }
}

#[derive(Debug)]
struct PartialBatchDeleteService {
    inner: Servicer,
    fail_path: String,
}

struct PartialBatchDelete {
    inner: Servicer,
    ctx: OperationContext,
    fail_path: String,
}

impl oio::BatchDelete for PartialBatchDelete {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        if path == self.fail_path {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "injected permanent batch delete failure",
            ));
        }
        let mut d = self.inner.delete(&self.ctx)?;
        d.delete(&path, args).await?;
        d.close().await
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<oio::BatchDeleteResult> {
        let mut succeeded = Vec::new();
        let mut failed = Vec::new();
        for (path, args) in batch {
            if path == self.fail_path {
                failed.push((
                    path,
                    args,
                    Error::new(
                        ErrorKind::PermissionDenied,
                        "injected permanent batch delete failure",
                    ),
                ));
            } else {
                let mut d = self.inner.delete(&self.ctx)?;
                d.delete(&path, args.clone()).await?;
                d.close().await?;
                succeeded.push((path, args));
            }
        }
        Ok(oio::BatchDeleteResult { succeeded, failed })
    }
}

impl Service for PartialBatchDeleteService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::BatchDeleter<PartialBatchDelete>;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        let mut cap = self.inner.capability();
        cap.delete_max_size = Some(2);
        cap
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
        self.inner.write(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        Ok(oio::BatchDeleter::new(
            PartialBatchDelete {
                inner: self.inner.clone(),
                ctx: ctx.clone(),
                fail_path: self.fail_path.clone(),
            },
            Some(2),
        ))
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
async fn batch_deleter_partial_flush_failure_evicts_committed_entries() {
    // Case 1: earlier key `"a"` succeeds while triggering key `"b"` fails permanently.
    let cache = memory_cache().await;
    let op = Operator::new(PartialBatchDeleteBuilder {
        fail_path: "b".to_string(),
    })
    .unwrap()
    .layer(FoyerLayer::new(cache.clone()));

    op.write("a", "va").await.unwrap();
    op.write("b", "vb").await.unwrap();

    let mut deleter = op.deleter().await.unwrap();
    deleter.delete("a").await.unwrap();
    // Simulate a concurrent write repopulating `"a"` before the batch flushes.
    op.write("a", "va-refilled").await.unwrap();
    assert!(cache.contains(&unversioned_key("a")));

    let err = deleter.delete("b").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    assert!(!cache.contains(&unversioned_key("a")));
    assert_eq!(op.read("a").await.unwrap_err().kind(), ErrorKind::NotFound);

    // Case 2: earlier key `"a"` fails permanently while triggering key `"b"` succeeds.
    let cache = memory_cache().await;
    let op = Operator::new(PartialBatchDeleteBuilder {
        fail_path: "a".to_string(),
    })
    .unwrap()
    .layer(FoyerLayer::new(cache.clone()));

    op.write("a", "va").await.unwrap();
    op.write("b", "vb").await.unwrap();
    assert!(cache.contains(&unversioned_key("b")));

    let err = op.delete_iter(["a", "b"]).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    assert!(!cache.contains(&unversioned_key("b")));
    assert_eq!(op.read("b").await.unwrap_err().kind(), ErrorKind::NotFound);
}

#[tokio::test]
async fn failed_standalone_delete_keeps_cached_entry() {
    let dir = tempfile::tempdir().unwrap();
    let (fs_op, fs_cache) = cached_fs(&dir).await;
    fs_op.write("dir/child", "nested").await.unwrap();
    let dir_key = unversioned_key("dir/");
    fs_cache.insert(dir_key.clone(), FoyerValue("sentinel".into()));

    let err = fs_op.delete("dir/").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(fs_cache.contains(&dir_key));
}

#[tokio::test]
async fn aborted_append_write_evicts_cached_entry() {
    // 1. Real `Fs` backend: append persists bytes during `write()` even when
    //    `abort()` fails with `Unsupported` (or the writer is dropped).
    let dir = tempfile::tempdir().unwrap();
    let (op, cache) = cached_fs(&dir).await;
    let key = unversioned_key("log.txt");

    op.write("log.txt", "hello").await.unwrap();
    assert!(cache.contains(&key));

    let mut writer = op.writer_with("log.txt").append(true).await.unwrap();
    writer.write(" world").await.unwrap();
    let err = writer.abort().await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported);

    assert!(!cache.contains(&key));
    assert_eq!(op.read("log.txt").await.unwrap().to_vec(), b"hello world");

    // 2. `oio::AppendWriter`-style backend where `abort()` returns `Ok(())`
    //    without rolling back already-appended bytes.
    struct NoopAbortAppendWriter {
        inner: oio::Writer,
    }

    impl oio::Write for NoopAbortAppendWriter {
        async fn write(&mut self, bs: Buffer) -> Result<()> {
            self.inner.write(bs).await
        }

        async fn close(&mut self) -> Result<Metadata> {
            self.inner.close().await
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct AppendAbortBuilder {
        fs_root: String,
    }

    #[derive(Debug)]
    struct AppendAbortService {
        inner: Servicer,
    }

    impl Builder for AppendAbortBuilder {
        type Config = ();

        fn build(self) -> Result<impl Service> {
            let inner: Servicer = Arc::new(Fs::default().root(&self.fs_root).build()?);
            Ok(AppendAbortService { inner })
        }
    }

    impl Service for AppendAbortService {
        type Reader = oio::Reader;
        type Writer = NoopAbortAppendWriter;
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
            Ok(NoopAbortAppendWriter {
                inner: self.inner.write(ctx, path, args)?,
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

    let dir2 = tempfile::tempdir().unwrap();
    let cache2 = memory_cache().await;
    let op2 = Operator::new(AppendAbortBuilder {
        fs_root: dir2.path().to_str().unwrap().to_string(),
    })
    .unwrap()
    .layer(FoyerLayer::new(cache2.clone()));

    op2.write("log.txt", "hello").await.unwrap();
    assert!(cache2.contains(&key));

    let mut writer = op2.writer_with("log.txt").append(true).await.unwrap();
    writer.write(" world").await.unwrap();
    writer.abort().await.unwrap();

    assert!(!cache2.contains(&key));
    assert_eq!(op2.read("log.txt").await.unwrap().to_vec(), b"hello world");
}

#[tokio::test]
async fn failed_append_close_evicts_cached_entry() {
    let dir = tempfile::tempdir().unwrap();
    let fs_root = dir.path().to_str().unwrap().to_string();
    let raw_fs = Operator::new(Fs::default().root(&fs_root)).unwrap();
    raw_fs.write("log.txt", "hello").await.unwrap();

    let (op, cache) = fault_operator(CloseFaultBuilder {
        fs_root: Some(fs_root),
        fail_next_close: true,
        ..Default::default()
    })
    .await;
    let key = unversioned_key("log.txt");

    assert_eq!(op.read("log.txt").await.unwrap().to_vec(), b"hello");
    assert!(cache.contains(&key));

    let mut writer = op.writer_with("log.txt").append(true).await.unwrap();
    writer.write(" world").await.unwrap();
    let err = writer.close().await.unwrap_err();
    assert!(err.is_temporary());

    assert!(!cache.contains(&key));
    assert_eq!(op.read("log.txt").await.unwrap().to_vec(), b"hello world");
}
