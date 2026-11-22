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
use opendal_core::raw::{Service, oio};
use opendal_core::services::Memory;
use opendal_core::{ErrorKind, Operator};
use opendal_layer_foyer::{Deleter, FoyerKey, FoyerLayer, FoyerService, FoyerValue, Writer};
use opendal_service_fs::Fs;
use std::ops::Range;

async fn cached_fs(dir: &tempfile::TempDir) -> (Operator, HybridCache<FoyerKey, FoyerValue>) {
    cached_fs_with_limit(dir, 0..usize::MAX).await
}

async fn cached_memory_with_limit(
    size_limit: Range<usize>,
) -> (Operator, HybridCache<FoyerKey, FoyerValue>) {
    let cache = HybridCacheBuilder::new()
        .memory(1024 * 1024)
        .with_shards(1)
        .storage()
        .with_recover_mode(RecoverMode::None)
        .build()
        .await
        .unwrap();
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(FoyerLayer::new(cache.clone()).with_size_limit(size_limit));
    (op, cache)
}

async fn cached_fs_with_limit(
    dir: &tempfile::TempDir,
    size_limit: Range<usize>,
) -> (Operator, HybridCache<FoyerKey, FoyerValue>) {
    let cache = HybridCacheBuilder::new()
        .memory(1024 * 1024)
        .with_shards(1)
        .storage()
        .with_recover_mode(RecoverMode::None)
        .build()
        .await
        .unwrap();
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
