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

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use foyer::BlockEngineConfig;
use foyer::DeviceBuilder;
use foyer::FsDeviceBuilder;
use foyer::HybridCache;
use foyer::HybridCacheBuilder;
use foyer::Load;
use foyer::RecoverMode;

use opendal_core::Buffer;
use opendal_core::Error;
use opendal_core::ErrorKind;
use opendal_core::Result;

use super::FoyerConfig;
use super::FoyerKey;
use super::FoyerValue;

/// Maximum number of deleted keys retained as negative-cache tombstones.
///
/// Foyer removes disk entries asynchronously, so recent deletes are tracked
/// in memory to avoid reading stale entries back from disk. Bounding the set
/// prevents unbounded memory growth across long-running delete workloads: once
/// a tombstone is evicted, a later read depends on Foyer's own delete having
/// completed. Reads that are already in flight do not depend on the tombstone;
/// [`DeletedKeys`] fences them individually until they finish.
const MAX_DELETED_KEYS: usize = 1024;

/// Reads of a single cache key that are currently loading it from disk.
#[derive(Default)]
struct ActiveReads {
    /// Number of reads that are loading this key right now.
    count: usize,
    /// Epoch of the newest delete of this key while those reads were in flight.
    deleted_epoch: u64,
    /// Epoch of the newest insert of this key while those reads were in flight.
    inserted_epoch: u64,
}

/// Mutations of a key that landed while a read was loading it from disk.
#[derive(Clone, Copy, Default)]
struct ReadRace {
    deleted: bool,
    inserted: bool,
}

#[derive(Default)]
struct DeletedKeys {
    /// Monotonic mutation counter, advanced by every insert and remove.
    ///
    /// Reads snapshot this value when they begin, so mutations that land later
    /// can be attributed to the reads that were already in flight. Tombstone
    /// recency uses the same counter, which keeps the bounded set in delete
    /// order without a second clock.
    epoch: u64,
    /// Tombstoned keys, mapped to the epoch at which they were deleted.
    keys: HashMap<Arc<str>, u64>,
    /// Tombstoned keys ordered by delete epoch.
    by_seq: BTreeMap<u64, Arc<str>>,
    /// Reads that are loading a key from disk right now.
    active_reads: HashMap<Arc<str>, ActiveReads>,
}

impl DeletedKeys {
    fn contains(&self, key: &str) -> bool {
        self.keys.contains_key(key)
    }

    fn len(&self) -> usize {
        self.keys.len()
    }

    /// Register a read of `key` and return the mutation epoch it starts from.
    ///
    /// Returns `None` when `key` is tombstoned: the caller must answer the read
    /// with a miss instead of loading the value back from disk.
    fn begin_read(&mut self, key: &str) -> Option<u64> {
        if self.contains(key) {
            return None;
        }

        let epoch = self.epoch;
        match self.active_reads.get_mut(key) {
            Some(active) => active.count += 1,
            None => {
                self.active_reads.insert(
                    Arc::from(key),
                    ActiveReads {
                        count: 1,
                        ..Default::default()
                    },
                );
            }
        }
        Some(epoch)
    }

    /// Remove the in-flight read registered by [`Self::begin_read`].
    ///
    /// Reports the mutations of `key` that landed after `epoch`.
    fn finish_read(&mut self, key: &str, epoch: u64) -> ReadRace {
        let Some(active) = self.active_reads.get_mut(key) else {
            return ReadRace::default();
        };

        let race = ReadRace {
            deleted: active.deleted_epoch > epoch,
            inserted: active.inserted_epoch > epoch,
        };
        active.count -= 1;
        if active.count == 0 {
            self.active_reads.remove(key);
        }
        race
    }

    /// Record an insert of `key`, clearing any tombstone it has.
    fn record_insert(&mut self, key: &str) {
        let epoch = self.epoch + 1;
        self.epoch = epoch;

        if let Some(seq) = self.keys.remove(key) {
            self.by_seq.remove(&seq);
        }
        if let Some(active) = self.active_reads.get_mut(key) {
            active.inserted_epoch = epoch;
        }
    }

    /// Record a delete of `key` and return the tombstone evicted by the bound.
    ///
    /// Recording a delete adds at most one key, so the bound evicts at most one
    /// tombstone and the caller never has to allocate a list of evictions.
    fn record_remove(&mut self, key: &str) -> Option<Arc<str>> {
        let epoch = self.epoch + 1;
        self.epoch = epoch;
        if let Some(active) = self.active_reads.get_mut(key) {
            active.deleted_epoch = epoch;
        }

        let shared: Arc<str> = match self.keys.remove_entry(key) {
            Some((existing, old_seq)) => {
                self.by_seq.remove(&old_seq);
                existing
            }
            None => Arc::from(key),
        };
        self.keys.insert(shared.clone(), epoch);
        self.by_seq.insert(epoch, shared);

        if self.keys.len() <= MAX_DELETED_KEYS {
            return None;
        }
        let (_, oldest) = self
            .by_seq
            .pop_first()
            .expect("tombstone table is never empty while above the bound");
        self.keys.remove(&oldest);
        Some(oldest)
    }
}

/// In-flight read registered with [`DeletedKeys::begin_read`].
///
/// The guard releases the registration on drop, so a cancelled `get` future
/// never leaves a stale in-flight read behind.
struct ReadGuard {
    deleted_keys: Arc<RwLock<DeletedKeys>>,
    key: Arc<str>,
    epoch: u64,
    finished: bool,
}

impl ReadGuard {
    /// Finish the read under an already held write lock and report races.
    fn finish(mut self, deleted_keys: &mut DeletedKeys) -> ReadRace {
        self.finished = true;
        deleted_keys.finish_read(&self.key, self.epoch)
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.deleted_keys
            .write()
            .expect("deleted keys lock poisoned")
            .finish_read(&self.key, self.epoch);
    }
}

/// FoyerCore holds the foyer HybridCache instance.
///
/// It supports lazy initialization when constructed from URI.
#[derive(Clone)]
pub struct FoyerCore {
    /// OnceLock for lazy cache initialization.
    cache: std::sync::OnceLock<Arc<HybridCache<FoyerKey, FoyerValue>>>,
    /// Tombstones, mutation epochs, and in-flight disk reads.
    ///
    /// Every mutation of the underlying cache (`insert` and `remove`) and every
    /// change to `DeletedKeys` happens under this lock's write side, so the
    /// cache and its fence always move together.
    deleted_keys: Arc<RwLock<DeletedKeys>>,
    /// Lock-free mirror of `DeletedKeys::len` for the read fast path.
    deleted_len: Arc<AtomicUsize>,
    /// Config
    config: FoyerConfig,
}

impl Debug for FoyerCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FoyerCore")
            .field("name", &self.config.name)
            .finish_non_exhaustive()
    }
}

const FOYER_DEFAULT_MEMORY_BYTES: usize = 1024 * 1024 * 1024;
const FOYER_DEFAULT_DISK_FILE_SIZE: usize = 1024 * 1024;
const FOYER_DEFAULT_SHARDS: usize = 1;

fn parse_recover_mode(mode_str: &str) -> Result<RecoverMode> {
    match mode_str.to_lowercase().as_str() {
        "none" => Ok(RecoverMode::None),
        "quiet" => Ok(RecoverMode::Quiet),
        "strict" => Ok(RecoverMode::Strict),
        _ => Err(Error::new(
            ErrorKind::ConfigInvalid,
            format!(
                "invalid recover_mode: {}, expected 'none', 'quiet', or 'strict'",
                mode_str
            ),
        )),
    }
}

impl FoyerCore {
    /// Create a new FoyerCore with a pre-built cache.
    ///
    /// This is used when the user provides a `HybridCache` via `Foyer::new(cache)`.
    pub fn new(config: FoyerConfig) -> Self {
        Self {
            cache: std::sync::OnceLock::new(),
            deleted_keys: Arc::new(RwLock::new(DeletedKeys::default())),
            deleted_len: Arc::new(AtomicUsize::new(0)),
            config,
        }
    }

    /// Initialize the cache with the given pre-built instance.
    pub(crate) fn with_cache(self, cache: Arc<HybridCache<FoyerKey, FoyerValue>>) -> Self {
        let _ = self.cache.set(cache);
        self
    }

    /// Get the cache, initializing lazily if needed.
    async fn get_cache(&self) -> Result<Arc<HybridCache<FoyerKey, FoyerValue>>> {
        if let Some(cache) = self.cache.get() {
            return Ok(cache.clone());
        }

        let memory = self.config.memory.unwrap_or(FOYER_DEFAULT_MEMORY_BYTES);
        let shards = self.config.shards.unwrap_or(FOYER_DEFAULT_SHARDS);
        let recover_mode = if let Some(ref mode_str) = self.config.recover_mode {
            parse_recover_mode(mode_str)?
        } else {
            RecoverMode::None
        };

        let mut builder = HybridCacheBuilder::new()
            .memory(memory)
            .with_shards(shards)
            .storage()
            .with_recover_mode(recover_mode);

        // Configure disk storage if disk_path is provided
        if let Some(ref disk_path) = self.config.disk_path {
            let path = PathBuf::from(disk_path);

            let mut device = FsDeviceBuilder::new(path);

            if let Some(capacity) = self.config.disk_capacity {
                device = device.with_capacity(capacity);
            }

            let block_size = self
                .config
                .disk_file_size
                .unwrap_or(FOYER_DEFAULT_DISK_FILE_SIZE);
            let device = device.build().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to build foyer cache device")
                    .set_source(e)
            })?;

            builder = builder
                .with_engine_config(BlockEngineConfig::new(device).with_block_size(block_size));
        }

        let cache = Arc::new(builder.build().await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to build foyer cache").set_source(e)
        })?);

        if self.cache.set(cache.clone()).is_err() {
            return self.cache.get().cloned().ok_or_else(|| {
                Error::new(ErrorKind::Unexpected, "failed to initialize foyer cache")
            });
        }

        Ok(cache)
    }

    pub fn name(&self) -> Option<&str> {
        self.config.name.as_deref()
    }

    pub async fn get(&self, key: &str) -> Result<Option<Buffer>> {
        let cache = self.get_cache().await?;
        let foyer_key = FoyerKey {
            path: key.to_string(),
        };

        // Fast path: without tombstones no lock is needed. A delete removes the
        // memory entry under `deleted_keys.write()`, so a memory hit cannot
        // observe a delete that has already completed.
        if self.deleted_len.load(Ordering::Acquire) != 0 {
            let deleted_keys = self
                .deleted_keys
                .read()
                .expect("deleted keys lock poisoned");
            if deleted_keys.contains(key) {
                return Ok(None);
            }
        }
        if let Some(entry) = cache.memory().get(&foyer_key) {
            return Ok(Some(entry.value().0.clone()));
        }

        // Slow path: the value can only come from disk. Load it here instead of
        // calling `HybridCache::get`, which promotes disk entries from a
        // detached task: a concurrent `remove` cannot fence that promotion and
        // a cancelled caller cannot stop it. Owning the load lets `ReadGuard`
        // fence the promotion against deletes and makes cancellation clean.
        //
        // Register the read before loading. `begin_read` and every cache
        // mutation run under the same write lock, so a mutation either lands
        // before the registration (and is visible here) or is recorded in the
        // registration and reported by `finish`.
        let read_guard = match self
            .deleted_keys
            .write()
            .expect("deleted keys lock poisoned")
            .begin_read(key)
        {
            Some(epoch) => ReadGuard {
                deleted_keys: self.deleted_keys.clone(),
                key: Arc::from(key),
                epoch,
                finished: false,
            },
            None => return Ok(None),
        };

        let load = cache.storage().load(&foyer_key).await;

        let mut deleted_keys = self
            .deleted_keys
            .write()
            .expect("deleted keys lock poisoned");
        let race = read_guard.finish(&mut deleted_keys);

        // A concurrent insert may have populated memory while this read was
        // loading. That value is newer than anything this read can promote.
        if let Some(entry) = cache.memory().get(&foyer_key) {
            return Ok(Some(entry.value().0.clone()));
        }
        if race.deleted {
            // Never return or promote bytes that predate the delete, even when
            // the tombstone has already been evicted by later deletes.
            return Ok(None);
        }

        match load {
            Ok(Load::Entry { value, .. }) => {
                if race.inserted {
                    // A newer insert already replaced this value and its memory
                    // entry is gone again. Do not put the older bytes back.
                    return Ok(Some(value.0));
                }
                let entry = cache.memory().insert(foyer_key, value);
                Ok(Some(entry.value().0.clone()))
            }
            // The value is still in Foyer's disk write queue, which lives in
            // memory. Leave it there: re-inserting it into the memory cache
            // would duplicate the pending disk write.
            Ok(Load::Piece { piece, .. }) => Ok(Some(piece.value().0.clone())),
            Ok(Load::Miss | Load::Throttled) => Ok(None),
            Err(_) => Ok(None),
        }
    }

    pub async fn insert(&self, key: &str, value: Buffer) -> Result<()> {
        let cache = self.get_cache().await?;
        let foyer_key = FoyerKey {
            path: key.to_string(),
        };

        let mut deleted_keys = self
            .deleted_keys
            .write()
            .expect("deleted keys lock poisoned");
        cache.insert(foyer_key, FoyerValue(value));
        deleted_keys.record_insert(key);
        // Publish the tombstone count only after the value is in memory, so a
        // reader that observes zero tombstones cannot reject this key as
        // deleted.
        self.deleted_len
            .store(deleted_keys.len(), Ordering::Release);
        Ok(())
    }

    pub async fn remove(&self, key: &str) -> Result<()> {
        let cache = self.get_cache().await?;
        let mut deleted_keys = self
            .deleted_keys
            .write()
            .expect("deleted keys lock poisoned");

        // Record the tombstone before touching the cache so a concurrent read
        // that misses memory cannot load the entry back from disk in between.
        let evicted = deleted_keys.record_remove(key);
        self.deleted_len
            .store(deleted_keys.len(), Ordering::Release);

        cache.remove(&FoyerKey {
            path: key.to_string(),
        });
        // Evicting a tombstone must remove the matching cache entry while the
        // lock is still held: once the tombstone is gone, a re-insert of that
        // key relies on the cache entry being gone too.
        if let Some(oldest) = evicted {
            cache.remove(&FoyerKey {
                path: oldest.to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Poll;

    use super::*;

    fn memory_core() -> FoyerCore {
        FoyerCore::new(FoyerConfig {
            memory: Some(4 * 1024 * 1024),
            shards: Some(4),
            ..Default::default()
        })
    }

    fn disk_core(dir: &tempfile::TempDir) -> FoyerCore {
        FoyerCore::new(FoyerConfig {
            // The memory cache capacity is an entry count, not a byte size.
            memory: Some(4),
            shards: Some(1),
            disk_path: Some(dir.path().to_string_lossy().into_owned()),
            disk_capacity: Some(64 * 1024 * 1024),
            disk_file_size: Some(4 * 1024 * 1024),
            ..Default::default()
        })
    }

    /// Poll a future exactly once without yielding to the runtime.
    async fn poll_once<F: Future>(future: &mut Pin<Box<F>>) -> Poll<F::Output> {
        std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
    }

    fn foyer_key(key: &str) -> FoyerKey {
        FoyerKey {
            path: key.to_string(),
        }
    }

    /// Force `key` out of the memory cache and onto disk.
    async fn move_to_disk(core: &FoyerCore, cache: &HybridCache<FoyerKey, FoyerValue>, key: &str) {
        // The memory cache holds four entries, so these inserts evict `key`
        // into the disk write queue without touching `key` itself.
        for i in 0..8 {
            core.insert(&format!("/filler-{i}"), Buffer::from(vec![b'f'; 1024]))
                .await
                .unwrap();
        }
        assert!(
            cache.memory().get(&foyer_key(key)).is_none(),
            "the value must leave the memory cache"
        );
        cache.storage().wait().await;
        match cache.storage().load(&foyer_key(key)).await.unwrap() {
            Load::Entry { .. } => {}
            other => panic!("the value must be readable from disk, got {other:?}"),
        }
    }

    #[test]
    fn deleted_keys_is_bounded_and_evicts_oldest() {
        let mut tombstones = DeletedKeys::default();

        for i in 0..(MAX_DELETED_KEYS + 32) {
            let evicted = tombstones.record_remove(&format!("k-{i}"));
            assert!(tombstones.len() <= MAX_DELETED_KEYS);
            if i < MAX_DELETED_KEYS {
                assert!(evicted.is_none());
            } else {
                assert_eq!(
                    evicted.unwrap().as_ref(),
                    format!("k-{}", i - MAX_DELETED_KEYS)
                );
            }
        }

        assert_eq!(tombstones.len(), MAX_DELETED_KEYS);
        assert_eq!(tombstones.by_seq.len(), MAX_DELETED_KEYS);
        assert!(!tombstones.contains("k-0"));
        assert!(tombstones.contains(&format!("k-{}", MAX_DELETED_KEYS + 31)));

        // Re-deleting an existing key refreshes its recency without growing the set.
        let refresh_key = "k-32";
        assert!(tombstones.record_remove(refresh_key).is_none());
        assert_eq!(tombstones.len(), MAX_DELETED_KEYS);
        assert_eq!(
            tombstones.record_remove("k-extra").unwrap().as_ref(),
            "k-33"
        );
        assert!(tombstones.contains(refresh_key));

        // Inserting a tombstoned key clears its entry without growing the set.
        tombstones.record_insert(refresh_key);
        assert!(!tombstones.contains(refresh_key));
        assert_eq!(tombstones.len(), MAX_DELETED_KEYS - 1);
        assert_eq!(tombstones.by_seq.len(), MAX_DELETED_KEYS - 1);
    }

    #[test]
    fn in_flight_reads_detect_racing_mutations() {
        let mut tombstones = DeletedKeys::default();

        // A read that starts after an insert sees no race.
        tombstones.record_insert("k");
        let epoch = tombstones.begin_read("k").expect("key must not be deleted");
        let race = tombstones.finish_read("k", epoch);
        assert!(!race.deleted && !race.inserted);
        assert!(tombstones.active_reads.is_empty());

        // A read that starts before a delete is reported even after the
        // tombstone is evicted by later deletes.
        let epoch = tombstones.begin_read("k").expect("key must not be deleted");
        tombstones.record_remove("k");
        for i in 0..MAX_DELETED_KEYS {
            tombstones.record_remove(&format!("other-{i}"));
        }
        assert!(!tombstones.contains("k"));
        let race = tombstones.finish_read("k", epoch);
        assert!(race.deleted, "the delete must be attributed to the read");
        assert!(tombstones.active_reads.is_empty());

        // Reads that start after the delete are rejected while the tombstone
        // exists.
        tombstones.record_remove("later");
        assert!(tombstones.begin_read("later").is_none());

        // A newer insert is reported to reads that started before it.
        let epoch = tombstones.begin_read("written").expect("no tombstone");
        tombstones.record_insert("written");
        let race = tombstones.finish_read("written", epoch);
        assert!(race.inserted && !race.deleted);

        // A delete wins over a later insert for reads that started before both.
        let epoch = tombstones.begin_read("both").expect("no tombstone");
        tombstones.record_remove("both");
        tombstones.record_insert("both");
        let race = tombstones.finish_read("both", epoch);
        assert!(race.deleted && race.inserted);
    }

    #[test]
    fn concurrent_reads_share_one_active_read_entry() {
        let mut tombstones = DeletedKeys::default();

        let first = tombstones.begin_read("k").expect("no tombstone");
        let second = tombstones.begin_read("k").expect("no tombstone");
        assert_eq!(tombstones.active_reads["k"].count, 2);

        tombstones.record_remove("k");
        assert!(tombstones.finish_read("k", first).deleted);
        assert_eq!(tombstones.active_reads["k"].count, 1);
        assert!(tombstones.finish_read("k", second).deleted);
        assert!(tombstones.active_reads.is_empty());
    }

    #[tokio::test]
    async fn core_bounded_deletes_and_reinsert_visibility() {
        let core = memory_core();

        for i in 0..(MAX_DELETED_KEYS + 16) {
            let k = format!("/obj-{i}");
            core.insert(&k, Buffer::from("val")).await.unwrap();
            core.remove(&k).await.unwrap();
        }
        assert_eq!(core.deleted_len.load(Ordering::Acquire), MAX_DELETED_KEYS);

        // Re-inserting a tombstoned key makes it readable again and clears its tombstone.
        let latest = format!("/obj-{}", MAX_DELETED_KEYS + 15);
        assert!(core.get(&latest).await.unwrap().is_none());
        core.insert(&latest, Buffer::from("fresh")).await.unwrap();
        assert_eq!(core.get(&latest).await.unwrap().unwrap().to_vec(), b"fresh");
        assert_eq!(
            core.deleted_len.load(Ordering::Acquire),
            MAX_DELETED_KEYS - 1
        );

        // Evicted tombstones stay deleted because the cache entry is gone too.
        assert!(core.get("/obj-0").await.unwrap().is_none());
        assert!(core.get("/obj-1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn clearing_the_last_tombstone_restores_the_fast_path() {
        let core = memory_core();

        for i in 0..8 {
            let key = format!("/fast-{i}");
            core.insert(&key, Buffer::from(format!("old-{i}")))
                .await
                .unwrap();
            core.remove(&key).await.unwrap();
        }
        assert_eq!(core.deleted_len.load(Ordering::Acquire), 8);
        assert_eq!(core.deleted_keys.read().unwrap().len(), 8);

        for i in 0..8 {
            let key = format!("/fast-{i}");
            assert!(core.get(&key).await.unwrap().is_none());
            core.insert(&key, Buffer::from(format!("new-{i}")))
                .await
                .unwrap();
        }
        assert_eq!(core.deleted_len.load(Ordering::Acquire), 0);
        assert!(core.deleted_keys.read().unwrap().active_reads.is_empty());

        for i in 0..8 {
            let key = format!("/fast-{i}");
            assert_eq!(
                core.get(&key).await.unwrap().unwrap().to_vec(),
                format!("new-{i}").as_bytes()
            );
        }

        // Deleting again after the fast path is back takes the normal path.
        core.remove("/fast-0").await.unwrap();
        assert_eq!(core.deleted_len.load(Ordering::Acquire), 1);
        assert!(core.get("/fast-0").await.unwrap().is_none());
        assert!(core.get("/fast-1").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn cancelled_cold_read_releases_its_pin() {
        let dir = tempfile::tempdir().unwrap();
        let core = disk_core(&dir);
        let cache = core.get_cache().await.unwrap();

        core.insert("/target", Buffer::from(vec![b't'; 512 * 1024]))
            .await
            .unwrap();
        move_to_disk(&core, &cache, "/target").await;

        let mut read = Box::pin(core.get("/target"));
        assert!(
            poll_once(&mut read).await.is_pending(),
            "the read must park on the disk load"
        );
        assert_eq!(core.deleted_keys.read().unwrap().active_reads.len(), 1);

        drop(read);
        assert!(
            core.deleted_keys.read().unwrap().active_reads.is_empty(),
            "dropping the future must release the in-flight read"
        );

        core.remove("/target").await.unwrap();
        assert!(core.get("/target").await.unwrap().is_none());
        assert!(cache.memory().get(&foyer_key("/target")).is_none());
    }

    #[tokio::test]
    async fn in_flight_disk_read_is_not_resurrected() {
        let dir = tempfile::tempdir().unwrap();
        let core = disk_core(&dir);
        let cache = core.get_cache().await.unwrap();

        core.insert("/target", Buffer::from(vec![b't'; 512 * 1024]))
            .await
            .unwrap();
        move_to_disk(&core, &cache, "/target").await;

        // Start the read and let it park on the disk load. The read registers
        // itself before the load, so the delete below can fence its promotion.
        let mut read = Box::pin(core.get("/target"));
        assert!(
            poll_once(&mut read).await.is_pending(),
            "the read must park on the disk load"
        );
        assert_eq!(core.deleted_keys.read().unwrap().active_reads.len(), 1);

        // Delete the key and then evict its tombstone with unrelated deletes.
        core.remove("/target").await.unwrap();
        for i in 0..MAX_DELETED_KEYS {
            core.remove(&format!("/other-{i}")).await.unwrap();
        }
        assert!(
            !core.deleted_keys.read().unwrap().contains("/target"),
            "later deletes must evict the tombstone of the target"
        );
        assert_eq!(core.deleted_len.load(Ordering::Acquire), MAX_DELETED_KEYS);

        // The parked read loaded stale bytes from disk before the delete. It
        // must neither return them nor promote them back into memory.
        assert!(
            read.await.unwrap().is_none(),
            "the in-flight read must observe the delete"
        );
        assert!(
            core.deleted_keys.read().unwrap().active_reads.is_empty(),
            "finishing the read must release its registration"
        );
        assert!(
            cache.memory().get(&foyer_key("/target")).is_none(),
            "the deleted value must not be resurrected"
        );
        assert!(core.get("/target").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn in_flight_disk_read_does_not_overwrite_a_newer_insert() {
        let dir = tempfile::tempdir().unwrap();
        let core = disk_core(&dir);
        let cache = core.get_cache().await.unwrap();

        core.insert("/target", Buffer::from(vec![b'o'; 512 * 1024]))
            .await
            .unwrap();
        move_to_disk(&core, &cache, "/target").await;

        let mut read = Box::pin(core.get("/target"));
        assert!(poll_once(&mut read).await.is_pending());

        // Rewrite the key while the older read is parked on the disk load.
        core.insert("/target", Buffer::from(vec![b'n'; 512 * 1024]))
            .await
            .unwrap();

        let value = read
            .await
            .unwrap()
            .expect("the read must still return a value");
        assert_eq!(
            value.to_vec(),
            vec![b'n'; 512 * 1024],
            "the read must observe the newer value instead of the bytes it loaded"
        );
        let cached = cache.memory().get(&foyer_key("/target")).unwrap();
        assert_eq!(
            cached.value().0.to_vec(),
            vec![b'n'; 512 * 1024],
            "the older read must not overwrite the newer value"
        );
    }

    #[tokio::test]
    async fn in_flight_disk_read_does_not_promote_over_an_evicted_newer_insert() {
        let dir = tempfile::tempdir().unwrap();
        let core = disk_core(&dir);
        let cache = core.get_cache().await.unwrap();

        core.insert("/target", Buffer::from(vec![b'o'; 512 * 1024]))
            .await
            .unwrap();
        move_to_disk(&core, &cache, "/target").await;

        let mut read = Box::pin(core.get("/target"));
        assert!(poll_once(&mut read).await.is_pending());

        // Rewrite the key, then evict the new value from memory again. The
        // older read must not put the bytes it loaded back into the cache.
        core.insert("/target", Buffer::from(vec![b'n'; 512 * 1024]))
            .await
            .unwrap();
        assert!(cache.memory().get(&foyer_key("/target")).is_some());
        for i in 0..8 {
            core.insert(&format!("/evictor-{i}"), Buffer::from(vec![b'e'; 1024]))
                .await
                .unwrap();
        }
        assert!(cache.memory().get(&foyer_key("/target")).is_none());

        let value = read
            .await
            .unwrap()
            .expect("the read must still return a value");
        assert_eq!(
            value.to_vec(),
            vec![b'o'; 512 * 1024],
            "a read that started before the rewrite returns the bytes it loaded"
        );
        assert!(
            cache.memory().get(&foyer_key("/target")).is_none(),
            "the older read must not promote its bytes over the newer insert"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_insert_and_remove_linearize_per_key() {
        let core = memory_core();
        // Initialize the cache before cloning: a clone made before the first
        // `get_cache` call owns an independent `OnceLock` and would build its
        // own cache instead of sharing this one.
        core.get_cache().await.unwrap();

        for round in 0..32 {
            let key = format!("/race-{round}");
            let value = format!("value-{round}");

            let insert_core = core.clone();
            let remove_core = core.clone();
            let insert_key = key.clone();
            let remove_key = key.clone();
            let insert_value = value.clone();
            let (insert, remove) = tokio::join!(
                tokio::spawn(async move {
                    insert_core
                        .insert(&insert_key, Buffer::from(insert_value))
                        .await
                }),
                tokio::spawn(async move { remove_core.remove(&remove_key).await }),
            );
            insert.unwrap().unwrap();
            remove.unwrap().unwrap();

            let tombstoned = core.deleted_keys.read().unwrap().contains(&key);
            let cached = core.get_cache().await.unwrap();
            let cached = cached.memory().get(&foyer_key(&key)).is_some();
            if tombstoned {
                assert!(
                    !cached,
                    "a remove that ran last must leave the key deleted in memory"
                );
                assert!(core.get(&key).await.unwrap().is_none());
            } else {
                assert!(
                    cached,
                    "an insert that ran last must clear the tombstone and keep the value"
                );
                assert_eq!(
                    core.get(&key).await.unwrap().unwrap().to_vec(),
                    value.as_bytes()
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn insert_during_evicting_remove_is_not_lost() {
        for round in 0..8 {
            let core = memory_core();

            // Seed the tombstone table so `/oldest-0` is the next key to evict.
            for i in 0..MAX_DELETED_KEYS {
                core.remove(&format!("/oldest-{i}")).await.unwrap();
            }
            {
                let deleted_keys = core.deleted_keys.read().unwrap();
                assert_eq!(deleted_keys.len(), MAX_DELETED_KEYS);
                assert_eq!(
                    deleted_keys
                        .by_seq
                        .first_key_value()
                        .map(|(_, key)| key.as_ref()),
                    Some("/oldest-0")
                );
            }

            // Re-insert the key whose tombstone the next remove will evict.
            let value = format!("fresh-{round}");
            let insert_core = core.clone();
            let remove_core = core.clone();
            let insert_value = value.clone();
            let (insert, remove) = tokio::join!(
                tokio::spawn(async move {
                    insert_core
                        .insert("/oldest-0", Buffer::from(insert_value))
                        .await
                }),
                tokio::spawn(async move { remove_core.remove(&format!("/extra-{round}")).await }),
            );
            insert.unwrap().unwrap();
            remove.unwrap().unwrap();

            assert_eq!(
                core.get("/oldest-0").await.unwrap().unwrap().to_vec(),
                value.as_bytes(),
                "evicting a tombstone must not delete a freshly inserted value"
            );
        }
    }
}
