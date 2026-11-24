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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]
mod copier;
mod deleter;
mod error;
mod full;
mod writer;

use std::{
    collections::{BTreeMap, HashMap},
    ops::{Bound, Deref, Range, RangeBounds},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
};

use foyer::{Code, HybridCache, Result as FoyerResult};

use opendal_core::raw::*;
use opendal_core::*;

use copier::FoyerCopier;
use deleter::Deleter;
use error::FetchError;
use writer::Writer;

/// [`FoyerKey`] is a key for the foyer cache. It implements foyer's [`Code`] trait
/// directly, so the layer does not depend on foyer's `serde` feature (and its bincode
/// dependency).
///
/// It's possible to specify a version in the [`OpRead`] args:
///
/// - If a version is given, the object is cached under that versioned key.
/// - If version is not supplied, the object is cached exactly as returned by the backend,
///   We do NOT interpret `None` as "latest" and we do not promote it to any other version.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FoyerKey {
    /// Object path used as the cache key.
    pub path: String,
    /// Object version, when the read targets a specific version.
    pub version: Option<String>,
}

impl Code for FoyerKey {
    fn encode(&self, writer: &mut impl std::io::Write) -> FoyerResult<()> {
        write_bytes(writer, self.path.as_bytes())?;
        match &self.version {
            None => writer.write_all(&[0u8])?,
            Some(version) => {
                writer.write_all(&[1u8])?;
                write_bytes(writer, version.as_bytes())?;
            }
        }
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> FoyerResult<Self> {
        let path = read_string(reader)?;
        let mut tag = [0u8; 1];
        reader.read_exact(&mut tag)?;
        let version = match tag[0] {
            0 => None,
            1 => Some(read_string(reader)?),
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid FoyerKey version tag: {other}"),
                )
                .into());
            }
        };
        Ok(FoyerKey { path, version })
    }

    fn estimated_size(&self) -> usize {
        8 + self.path.len() + 1 + self.version.as_ref().map_or(0, |v| 8 + v.len())
    }
}

fn write_bytes(writer: &mut impl std::io::Write, bytes: &[u8]) -> FoyerResult<()> {
    writer.write_all(&(bytes.len() as u64).to_le_bytes())?;
    writer.write_all(bytes)?;
    Ok(())
}

fn read_string(reader: &mut impl std::io::Read) -> FoyerResult<String> {
    let mut len = [0u8; 8];
    reader.read_exact(&mut len)?;
    let len = u64::from_le_bytes(len) as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    String::from_utf8(buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e).into())
}

/// [`FoyerValue`] stores buffered object data in Foyer.
#[derive(Debug)]
pub struct FoyerValue(
    /// Buffered object data.
    pub Buffer,
);

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

/// Maximum number of idle invalidated keys remembered by the layer.
///
/// The invalidation history is a negative cache, not a correctness requirement:
/// cache entries are removed as soon as they are invalidated. Bounding the
/// history keeps long-running deleters from retaining every deleted path.
const MAX_DELETED_KEYS: usize = 1024;

#[derive(Debug, Default)]
struct IdleTombstones {
    next_seq: u64,
    keys: HashMap<Arc<FoyerKey>, u64>,
    by_seq: BTreeMap<u64, Arc<FoyerKey>>,
}

impl IdleTombstones {
    fn contains(&self, key: &FoyerKey) -> bool {
        self.keys.contains_key(key)
    }

    fn insert_shared(
        &mut self,
        shared_key: Arc<FoyerKey>,
        mut on_evict: impl FnMut(Arc<FoyerKey>),
    ) {
        let seq = self.next_seq;
        self.next_seq += 1;

        if let Some(old_seq) = self.keys.insert(shared_key.clone(), seq) {
            self.by_seq.remove(&old_seq);
        }
        self.by_seq.insert(seq, shared_key);

        while self.keys.len() > MAX_DELETED_KEYS {
            let Some((_, oldest_key)) = self.by_seq.pop_first() else {
                break;
            };
            self.keys.remove(&oldest_key);
            on_evict(oldest_key);
        }
    }

    fn remove(&mut self, key: &FoyerKey) {
        if let Some(seq) = self.keys.remove(key) {
            self.by_seq.remove(&seq);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.keys.len()
    }
}

#[derive(Debug, Clone)]
enum PinnedKind {
    /// Invalidated while older reads of this key were in flight.
    /// New reads bypass the cache until those reads finish, at which point the
    /// key is scheduled for cache removal and recorded in the bounded idle
    /// tombstone history.
    Invalidate,
    /// Evicted without marking deleted while older reads of this key were in
    /// flight. New reads bypass the cache until those reads finish, at which
    /// point the key is scheduled for cache removal and released so later
    /// reads can refill it.
    Evict,
    /// Overwritten by a cacheable write while older reads of this key were in
    /// flight. New reads can hit the cached entry, while older origin fetches
    /// abort insertion under `cache_lock`. Holds a cheap ref-counted clone of
    /// the latest stored buffer so any late disk-load promotion
    /// (`optional_fetch`) or waking `WriteOnInsertion` enqueue by an older
    /// read is overwritten once those reads finish.
    Store(Buffer),
}

#[derive(Debug, Clone)]
struct PinnedEntry {
    epoch: u64,
    kind: PinnedKind,
}

#[derive(Debug, Clone)]
enum PendingRepair {
    Remove,
    Store(Buffer),
}

/// Active read counts per epoch for a single cache key.
///
/// Most concurrent reads of a key observe the same epoch, so the single-epoch
/// fast path avoids allocating a [`BTreeMap`] on every cache hit.
#[derive(Debug, PartialEq, Eq)]
enum ActiveReadEpochs {
    Single { epoch: u64, count: usize },
    Multiple(BTreeMap<u64, usize>),
}

impl ActiveReadEpochs {
    fn single(epoch: u64) -> Self {
        Self::Single { epoch, count: 1 }
    }

    fn insert(&mut self, epoch: u64) {
        match self {
            Self::Single {
                epoch: existing,
                count,
            } if *existing == epoch => {
                *count += 1;
            }
            Self::Single {
                epoch: existing,
                count,
            } => {
                let mut map = BTreeMap::new();
                map.insert(*existing, *count);
                map.insert(epoch, 1);
                *self = Self::Multiple(map);
            }
            Self::Multiple(map) => {
                *map.entry(epoch).or_insert(0) += 1;
            }
        }
    }

    /// Decrements the count for `epoch` and returns `(removed_epoch, min_active_epoch)`.
    /// When `min_active_epoch` is `None`, the caller must remove the key from `active_reads`.
    fn remove(&mut self, epoch: u64) -> (bool, Option<u64>) {
        match self {
            Self::Single {
                epoch: existing,
                count,
            } => {
                if *existing != epoch {
                    return (false, Some(*existing));
                }
                if *count > 1 {
                    *count -= 1;
                    (false, Some(*existing))
                } else {
                    *count = 0;
                    (true, None)
                }
            }
            Self::Multiple(map) => {
                let removed_epoch = match map.get_mut(&epoch) {
                    Some(count) if *count > 1 => {
                        *count -= 1;
                        false
                    }
                    Some(_) => {
                        map.remove(&epoch);
                        true
                    }
                    None => false,
                };
                match map.len() {
                    0 => (removed_epoch, None),
                    1 => {
                        let (&remaining_epoch, &remaining_count) =
                            map.first_key_value().expect("map has len 1");
                        *self = Self::Single {
                            epoch: remaining_epoch,
                            count: remaining_count,
                        };
                        (removed_epoch, Some(remaining_epoch))
                    }
                    _ => (removed_epoch, map.first_key_value().map(|(&e, _)| e)),
                }
            }
        }
    }
}

/// Invalidated cache keys and in-flight read/delete fences.
///
/// Origin cache fills commit under `cache_lock` alongside `store`, `evict`,
/// `invalidate`, and `begin_delete` so staleness checks and Foyer mutations
/// are serialized. A key also stays pinned while reads that started before a
/// mutation (or a `Deleter::close` call targeting the key) are still in
/// flight; once those reads finish, any deferred cache repair is queued into
/// `pending_repairs` and flushed outside `ReadGuardInner::drop`.
///
/// Lock hierarchy is strictly `cache_lock` -> Foyer internal locks -> `state`.
/// `state` is a leaf lock and never calls back into Foyer while held, because
/// Foyer may drop a coalesced waiter's fetch future (and its `ReadGuard`)
/// while holding its own shard and inflight locks.
#[derive(Debug, Default)]
struct DeletedKeys {
    cache_lock: Mutex<()>,
    state: Mutex<DeletedKeysState>,
    has_pending_repairs: AtomicBool,
}

#[derive(Debug, Default)]
struct DeletedKeysState {
    epoch: u64,
    active_reads: HashMap<Arc<FoyerKey>, ActiveReadEpochs>,
    pinned: HashMap<FoyerKey, PinnedEntry>,
    active_deletes: HashMap<FoyerKey, usize>,
    idle: IdleTombstones,
    pending_repairs: HashMap<FoyerKey, PendingRepair>,
}

impl DeletedKeysState {
    fn insert_idle(&mut self, shared_key: Arc<FoyerKey>) -> bool {
        let mut queued_repair = false;
        self.idle.insert_shared(shared_key, |evicted_key| {
            self.pending_repairs
                .insert(evicted_key.as_ref().clone(), PendingRepair::Remove);
            queued_repair = true;
        });
        queued_repair
    }

    fn apply_invalidation(&mut self, keys: &[FoyerKey], evict_keys: &[FoyerKey]) -> bool {
        let mut queued_repair = false;
        for key in evict_keys {
            self.idle.remove(key);
            self.pending_repairs.remove(key);
            if self.active_reads.contains_key(key) {
                self.epoch += 1;
                self.pinned.insert(
                    key.clone(),
                    PinnedEntry {
                        epoch: self.epoch,
                        kind: PinnedKind::Evict,
                    },
                );
            } else {
                self.pinned.remove(key);
            }
        }

        for key in keys {
            self.pending_repairs.remove(key);
            if self.active_reads.contains_key(key) {
                self.idle.remove(key);
                self.epoch += 1;
                self.pinned.insert(
                    key.clone(),
                    PinnedEntry {
                        epoch: self.epoch,
                        kind: PinnedKind::Invalidate,
                    },
                );
            } else {
                self.pinned.remove(key);
                queued_repair |= self.insert_idle(Arc::new(key.clone()));
            }
        }
        queued_repair
    }
}

#[derive(Debug, Clone)]
struct ReadGuard(Arc<ReadGuardInner>);

impl ReadGuard {
    fn key(&self) -> &FoyerKey {
        self.0.key.as_ref()
    }

    fn epoch(&self) -> u64 {
        self.0.epoch
    }
}

#[derive(Debug)]
struct ReadGuardInner {
    deleted_keys: Arc<DeletedKeys>,
    key: Arc<FoyerKey>,
    epoch: u64,
}

impl Drop for ReadGuardInner {
    fn drop(&mut self) {
        self.deleted_keys.finish_read(&self.key, self.epoch);
    }
}

#[derive(Debug)]
pub(crate) struct DeleteGuard<'a> {
    deleted_keys: &'a DeletedKeys,
    cache: &'a HybridCache<FoyerKey, FoyerValue>,
    keys: &'a [FoyerKey],
    evict_keys: &'a [FoyerKey],
}

impl Drop for DeleteGuard<'_> {
    fn drop(&mut self) {
        if self.keys.is_empty() && self.evict_keys.is_empty() {
            return;
        }
        let cache_guard = self.deleted_keys.lock_cache();
        self.deleted_keys
            .drain_repairs_locked(&cache_guard, self.cache);
        {
            let mut state = self.deleted_keys.lock_state();
            for key in self.keys.iter().chain(self.evict_keys.iter()) {
                if let Some(count) = state.active_deletes.get_mut(key) {
                    if *count > 1 {
                        *count -= 1;
                    } else {
                        state.active_deletes.remove(key);
                    }
                }
            }
            if state.apply_invalidation(self.keys, self.evict_keys) {
                self.deleted_keys
                    .has_pending_repairs
                    .store(true, Ordering::Release);
            }
        }
        for key in self.evict_keys.iter().chain(self.keys.iter()) {
            self.cache.remove(key);
        }
        self.deleted_keys
            .drain_repairs_locked(&cache_guard, self.cache);
    }
}

impl DeletedKeys {
    fn lock_cache(&self) -> MutexGuard<'_, ()> {
        self.cache_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_state(&self) -> MutexGuard<'_, DeletedKeysState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn drain_repairs_locked(
        &self,
        _cache_guard: &MutexGuard<'_, ()>,
        cache: &HybridCache<FoyerKey, FoyerValue>,
    ) {
        if !self.has_pending_repairs.load(Ordering::Acquire) {
            return;
        }
        loop {
            let repairs = {
                let mut state = self.lock_state();
                if state.pending_repairs.is_empty() {
                    // Only clear the flag once every previously popped batch
                    // has been applied to Foyer and no new repairs were queued.
                    self.has_pending_repairs.store(false, Ordering::Release);
                    return;
                }
                std::mem::take(&mut state.pending_repairs)
            };

            for (key, repair) in repairs {
                match repair {
                    PendingRepair::Remove => cache.remove(&key),
                    PendingRepair::Store(value) => {
                        cache.insert(key, FoyerValue(value));
                    }
                }
            }
        }
    }

    fn flush_repairs(&self, cache: &HybridCache<FoyerKey, FoyerValue>) {
        if !self.has_pending_repairs.load(Ordering::Acquire) {
            return;
        }
        let cache_guard = self.lock_cache();
        self.drain_repairs_locked(&cache_guard, cache);
    }

    fn begin_read(
        self: &Arc<Self>,
        cache: &HybridCache<FoyerKey, FoyerValue>,
        key: &Arc<FoyerKey>,
    ) -> Option<ReadGuard> {
        loop {
            if self.has_pending_repairs.load(Ordering::Acquire) {
                self.flush_repairs(cache);
            }
            let mut state = self.lock_state();
            if self.has_pending_repairs.load(Ordering::Acquire) {
                drop(state);
                self.flush_repairs(cache);
                continue;
            }
            if state.active_deletes.contains_key(key.as_ref())
                || state.idle.contains(key.as_ref())
                || state.pinned.get(key.as_ref()).is_some_and(|entry| {
                    matches!(entry.kind, PinnedKind::Invalidate | PinnedKind::Evict)
                })
            {
                return None;
            }

            let epoch = state.epoch;
            if let Some(epochs) = state.active_reads.get_mut(key.as_ref()) {
                epochs.insert(epoch);
            } else {
                state
                    .active_reads
                    .insert(key.clone(), ActiveReadEpochs::single(epoch));
            }

            return Some(ReadGuard(Arc::new(ReadGuardInner {
                deleted_keys: self.clone(),
                key: key.clone(),
                epoch,
            })));
        }
    }

    fn finish_read(&self, key: &Arc<FoyerKey>, epoch: u64) {
        let mut state = self.lock_state();
        let Some(epochs) = state.active_reads.get_mut(key.as_ref()) else {
            return;
        };
        let (removed_epoch, min_active) = epochs.remove(epoch);
        if min_active.is_none() {
            state.active_reads.remove(key.as_ref());
        }

        if !removed_epoch || state.pinned.is_empty() {
            return;
        }

        if let Some(min_epoch) = min_active
            && min_epoch < epoch
        {
            return;
        }

        let expired = state
            .pinned
            .get(key.as_ref())
            .is_some_and(|entry| min_active.is_none_or(|min_epoch| min_epoch >= entry.epoch));
        if expired && let Some(entry) = state.pinned.remove(key.as_ref()) {
            match entry.kind {
                PinnedKind::Invalidate => {
                    // The last read that could have refilled this entry has
                    // finished. Record the bounded idle tombstone and queue a
                    // cache removal (applied outside `ReadGuardInner::drop`).
                    state.insert_idle(key.clone());
                    state
                        .pending_repairs
                        .insert(key.as_ref().clone(), PendingRepair::Remove);
                    self.has_pending_repairs.store(true, Ordering::Release);
                }
                PinnedKind::Evict => {
                    // The last read that could have refilled this entry has
                    // finished. Queue a cache removal and allow subsequent
                    // reads to refill the cache.
                    state
                        .pending_repairs
                        .insert(key.as_ref().clone(), PendingRepair::Remove);
                    self.has_pending_repairs.store(true, Ordering::Release);
                }
                PinnedKind::Store(value) => {
                    // The last read that started before `store()` has finished.
                    // Queue a re-insert so any stale disk-load promotion
                    // (`optional_fetch`) or waking `WriteOnInsertion` enqueue
                    // cannot leave older bytes behind.
                    state
                        .pending_repairs
                        .insert(key.as_ref().clone(), PendingRepair::Store(value));
                    self.has_pending_repairs.store(true, Ordering::Release);
                }
            }
        }
    }

    fn commit_fetch(
        &self,
        cache: &HybridCache<FoyerKey, FoyerValue>,
        key: &FoyerKey,
        fetch_epoch: u64,
        buffer: Buffer,
    ) -> std::result::Result<(), FetchError> {
        let cache_guard = self.lock_cache();
        self.drain_repairs_locked(&cache_guard, cache);
        let stale = {
            let state = self.lock_state();
            state.active_deletes.contains_key(key)
                || state.idle.contains(key)
                || state
                    .pinned
                    .get(key)
                    .is_some_and(|entry| entry.epoch > fetch_epoch)
        };
        if stale {
            return Err(FetchError::Stale {
                buffer,
                fetch_epoch,
            });
        }
        cache.insert(key.clone(), FoyerValue(buffer));
        self.drain_repairs_locked(&cache_guard, cache);
        Ok(())
    }

    fn invalidate(
        &self,
        cache: &HybridCache<FoyerKey, FoyerValue>,
        keys: &[FoyerKey],
        evict_keys: &[FoyerKey],
    ) {
        if keys.is_empty() && evict_keys.is_empty() {
            return;
        }
        let cache_guard = self.lock_cache();
        self.drain_repairs_locked(&cache_guard, cache);
        {
            let mut state = self.lock_state();
            if state.apply_invalidation(keys, evict_keys) {
                self.has_pending_repairs.store(true, Ordering::Release);
            }
        }
        for key in evict_keys.iter().chain(keys.iter()) {
            cache.remove(key);
        }
        self.drain_repairs_locked(&cache_guard, cache);
    }

    pub(crate) fn begin_delete<'a>(
        &'a self,
        cache: &'a HybridCache<FoyerKey, FoyerValue>,
        keys: &'a [FoyerKey],
        evict_keys: &'a [FoyerKey],
    ) -> DeleteGuard<'a> {
        if !keys.is_empty() || !evict_keys.is_empty() {
            let cache_guard = self.lock_cache();
            self.drain_repairs_locked(&cache_guard, cache);
            {
                let mut state = self.lock_state();
                if state.apply_invalidation(keys, evict_keys) {
                    self.has_pending_repairs.store(true, Ordering::Release);
                }
                for key in keys.iter().chain(evict_keys.iter()) {
                    *state.active_deletes.entry(key.clone()).or_insert(0) += 1;
                }
            }
            for key in evict_keys.iter().chain(keys.iter()) {
                cache.remove(key);
            }
            self.drain_repairs_locked(&cache_guard, cache);
        }
        DeleteGuard {
            deleted_keys: self,
            cache,
            keys,
            evict_keys,
        }
    }

    pub(crate) fn downgrade_stored_pins(&self) {
        let _cache_guard = self.lock_cache();
        let mut state = self.lock_state();
        for entry in state.pinned.values_mut() {
            if matches!(entry.kind, PinnedKind::Store(_)) {
                entry.kind = PinnedKind::Evict;
            }
        }
        for repair in state.pending_repairs.values_mut() {
            if matches!(repair, PendingRepair::Store(_)) {
                *repair = PendingRepair::Remove;
            }
        }
    }

    fn store(
        &self,
        cache: &HybridCache<FoyerKey, FoyerValue>,
        key: FoyerKey,
        value: FoyerValue,
        evict_key: Option<&FoyerKey>,
    ) {
        let cache_guard = self.lock_cache();
        self.drain_repairs_locked(&cache_guard, cache);
        {
            let mut state = self.lock_state();
            state.idle.remove(&key);
            state.pending_repairs.remove(&key);
            if state.active_reads.contains_key(&key) {
                state.epoch += 1;
                let epoch = state.epoch;
                state.pinned.insert(
                    key.clone(),
                    PinnedEntry {
                        epoch,
                        kind: PinnedKind::Store(value.0.clone()),
                    },
                );
            } else {
                state.pinned.remove(&key);
            }

            if let Some(evict_key) = evict_key {
                state.idle.remove(evict_key);
                state.pending_repairs.remove(evict_key);
                if state.active_reads.contains_key(evict_key) {
                    state.epoch += 1;
                    let epoch = state.epoch;
                    state.pinned.insert(
                        evict_key.clone(),
                        PinnedEntry {
                            epoch,
                            kind: PinnedKind::Evict,
                        },
                    );
                } else {
                    state.pinned.remove(evict_key);
                }
            }
        }

        if let Some(evict_key) = evict_key {
            cache.remove(evict_key);
        }
        cache.insert(key, value);
        self.drain_repairs_locked(&cache_guard, cache);
    }

    fn evict(&self, cache: &HybridCache<FoyerKey, FoyerValue>, key: &FoyerKey) {
        let cache_guard = self.lock_cache();
        self.drain_repairs_locked(&cache_guard, cache);
        {
            let mut state = self.lock_state();
            state.idle.remove(key);
            state.pending_repairs.remove(key);
            if state.active_reads.contains_key(key) {
                state.epoch += 1;
                let epoch = state.epoch;
                state.pinned.insert(
                    key.clone(),
                    PinnedEntry {
                        epoch,
                        kind: PinnedKind::Evict,
                    },
                );
            } else {
                state.pinned.remove(key);
            }
        }

        cache.remove(key);
        self.drain_repairs_locked(&cache_guard, cache);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        let state = self.lock_state();
        state.idle.len() + state.pinned.len() + state.active_deletes.len()
    }

    #[cfg(test)]
    fn pinned_len(&self) -> usize {
        self.lock_state().pinned.len()
    }

    #[cfg(test)]
    fn auxiliary_len(&self) -> usize {
        let state = self.lock_state();
        state.idle.by_seq.len() + state.active_reads.len() + state.active_deletes.len()
    }
}

/// `FoyerLayer` caches OpenDAL data with [foyer](https://github.com/foyer-rs/foyer).
///
/// # Operation Behavior
/// - `write`: [`FoyerLayer`] caches data after the service completes the write
///   when the object's total size stays inside [`FoyerLayer::with_size_limit`].
///   Writes that end up outside the limit invalidate the superseded cache
///   entry. Append writes do not contain the whole object and may persist
///   bytes during `write`, so they evict the superseded unversioned cache
///   entry on `write` and `close` even if `abort` is called or `close` fails.
///   Aborting a non-append write leaves the cache untouched.
/// - `read`: [`FoyerLayer`] checks the cache first. On a cache miss, it reads
///   and caches the entire object if its size is within the configured
///   [`FoyerLayer::with_size_limit`] range, even when only a byte range is requested.
///   Reads with preconditions such as `if_match` or `if_none_match` bypass the
///   cache and read directly from the service, which evaluates the condition.
///   A conditional read never populates the cache.
/// - `delete`: [`FoyerLayer`] invalidates cached data after a successful `delete` call and
///   again when the deleter closes, so both immediate and buffered deleters stay consistent.
///   A failed standalone non-recursive `delete` call is not invalidated; if earlier deletes
///   were already queued on the same deleter or a recursive delete fails, affected entries are
///   invalidated or cleared because the backend may have partially committed them. Exact-key
///   invalidation happens before the underlying deleter is closed, so the data remains
///   invalidated if closing the deleter fails. Recursive deletes also clear the entire cache
///   on `delete` and after the underlying deleter closes, including when closing fails, because
///   Foyer does not support prefix invalidation and buffered deleters can allow concurrent
///   reads to refill deleted objects while closing.
/// - `copy`: Once the copy completes, [`FoyerLayer`] removes the unversioned
///   cache entry for the destination.
/// - `rename`: After a successful rename, [`FoyerLayer`] removes the unversioned
///   cache entries for both the source and the destination.
///
/// Reads of recently invalidated paths bypass the cache until a write through
/// the layer repopulates the corresponding entry. The layer retains a bounded
/// invalidation history, so older invalidated paths can be cached again.
///
/// # Range Reads
///
/// [`FoyerLayer`] caches entire objects, not individual byte ranges. For example,
/// `operator.read_with("some/path").range(0..1024)` returns only the first 1024
/// bytes, but on a cache miss the layer fetches and buffers the entire object
/// before returning that range if the object is eligible for caching. On a cache
/// hit, the layer returns the requested range from the cached object.
///
/// Reading a small range from a large object can therefore transfer and buffer
/// much more data than requested, increasing read latency and memory usage.
/// Use [`FoyerLayer::with_size_limit`] to exclude large objects from caching.
/// On a cache miss, objects outside the configured size range are read directly
/// from the service using the requested byte range.
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
/// When object versioning is enabled, `FoyerLayer` treats objects with the same
/// path but different versions as different objects.
#[derive(Debug)]
pub struct FoyerLayer {
    cache: HybridCache<FoyerKey, FoyerValue>,
    size_limit: Range<usize>,
    deleted_keys: Arc<DeletedKeys>,
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

    /// Set the object-size range eligible for caching.
    ///
    /// The layer reads through or writes through objects outside this range
    /// without retaining them in Foyer. The default range accepts every
    /// representable object size below `usize::MAX`.
    pub fn with_size_limit<R: RangeBounds<usize>>(mut self, size_limit: R) -> Self {
        let start = match size_limit.start_bound() {
            Bound::Included(v) => *v,
            Bound::Excluded(v) => v.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match size_limit.end_bound() {
            Bound::Included(v) => v.saturating_add(1),
            Bound::Excluded(v) => *v,
            Bound::Unbounded => usize::MAX,
        };
        self.size_limit = start..end;
        self
    }
}

impl Layer for FoyerLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(FoyerService {
            inner,
            cache: self.cache.clone(),
            size_limit: self.size_limit.clone(),
            deleted_keys: self.deleted_keys.clone(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct Inner {
    pub(crate) srv: Servicer,
    pub(crate) ctx: OperationContext,
    pub(crate) cache: HybridCache<FoyerKey, FoyerValue>,
    pub(crate) deleted_keys: Arc<DeletedKeys>,
}

impl Inner {
    fn invalidate_unversioned(&self, path: String) {
        let key = FoyerKey {
            path,
            version: None,
        };
        self.deleted_keys.invalidate(&self.cache, &[key], &[]);
    }
}

#[derive(Debug)]
struct FoyerService {
    inner: Servicer,
    cache: HybridCache<FoyerKey, FoyerValue>,
    size_limit: Range<usize>,
    deleted_keys: Arc<DeletedKeys>,
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
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        let mut capability = self.inner.capability();
        capability.write_can_copy_from = false;
        capability.compose = false;
        capability.compose_with_content_type = false;
        capability.compose_with_content_disposition = false;
        capability.compose_with_content_encoding = false;
        capability.compose_with_cache_control = false;
        capability.compose_with_user_metadata = false;
        capability.compose_with_if_match = false;
        capability.compose_with_if_none_match = false;
        capability.compose_with_if_version_match = false;
        capability.compose_with_if_version_not_match = false;
        capability.compose_with_if_not_exists = false;
        capability.compose_with_source_version = false;
        capability.compose_with_source_if_match = false;
        capability.restore = false;
        capability.restore_with_version = false;
        capability.restore_with_if_not_exists = false;
        capability
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        Ok(full::FullReader::new(
            self.operation_inner(ctx),
            self.size_limit.clone(),
            path.to_string(),
            args,
        ))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let inner = self.operation_inner(ctx);
        let size_limit = self.size_limit.clone();
        let path = path.to_string();
        let append = args.append();
        let w = inner.srv.write(&inner.ctx, &path, args)?;
        Ok(Writer::new(w, path, inner, size_limit, append))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let inner = self.operation_inner(ctx);
        let d = inner.srv.delete(&inner.ctx)?;
        Ok(Deleter::new(d, inner))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        let copier = self.inner.copy(ctx, from, to, args)?;
        Ok(Box::new(FoyerCopier::new(
            copier,
            to.to_string(),
            self.operation_inner(ctx),
        )))
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
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
        let rp = self.inner.rename(ctx, from, to, args).await?;
        let keys = [
            FoyerKey {
                path: from.to_string(),
                version: None,
            },
            FoyerKey {
                path: to.to_string(),
                version: None,
            },
        ];
        self.deleted_keys.invalidate(&self.cache, &keys, &[]);
        Ok(rp)
    }

    async fn restore(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpRestore,
    ) -> Result<RpRestore> {
        Err(
            Error::new(ErrorKind::Unsupported, "operation is not supported")
                .with_operation(Operation::Restore),
        )
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

#[cfg(test)]
mod tests {
    use foyer::{
        BlockEngineConfig, DeviceBuilder, Error as FoyerError, ErrorKind as FoyerErrorKind,
        FsDeviceBuilder, HybridCache, HybridCacheBuilder, RecoverMode,
    };
    use opendal_core::raw::oio::Copy as _;
    use opendal_core::raw::oio::Delete as _;
    use opendal_core::raw::oio::Read as _;
    use opendal_core::raw::oio::ReadStream as _;
    use opendal_core::raw::oio::Write as _;
    use opendal_core::{Buffer, Operator, services::Memory};
    use size::consts::MiB;
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::{Semaphore, oneshot};

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
        etag: String,
        stat_gate: Mutex<Option<(Option<String>, Arc<Semaphore>)>>,
        stat_started: Arc<Semaphore>,
        condition_gate: Mutex<Option<Arc<Semaphore>>>,
        condition_started: Arc<Semaphore>,
        stat_calls: AtomicUsize,
        open_calls: AtomicUsize,
        read_calls: AtomicUsize,
        last_stat_args: Mutex<Option<OpStat>>,
        last_read_args: Mutex<Option<OpRead>>,
    }

    impl MockReadState {
        fn new(data: Buffer) -> Self {
            Self {
                data,
                etag: "\"etag-1\"".to_string(),
                stat_gate: Mutex::new(None),
                stat_started: Arc::new(Semaphore::new(0)),
                condition_gate: Mutex::new(None),
                condition_started: Arc::new(Semaphore::new(0)),
                stat_calls: AtomicUsize::new(0),
                open_calls: AtomicUsize::new(0),
                read_calls: AtomicUsize::new(0),
                last_stat_args: Mutex::new(None),
                last_read_args: Mutex::new(None),
            }
        }

        fn gate_stat(&self) -> Arc<Semaphore> {
            let gate = Arc::new(Semaphore::new(0));
            *self.stat_gate.lock().unwrap() = Some((None, gate.clone()));
            gate
        }

        fn gate_stat_for(&self, path: &str) -> Arc<Semaphore> {
            let gate = Arc::new(Semaphore::new(0));
            *self.stat_gate.lock().unwrap() = Some((Some(path.to_string()), gate.clone()));
            gate
        }

        fn gate_condition(&self) -> Arc<Semaphore> {
            let gate = Arc::new(Semaphore::new(0));
            *self.condition_gate.lock().unwrap() = Some(gate.clone());
            gate
        }

        async fn check_conditions(
            &self,
            if_match: Option<&str>,
            if_none_match: Option<&str>,
        ) -> Result<()> {
            if if_match.is_some() || if_none_match.is_some() {
                let gate = self.condition_gate.lock().unwrap().clone();
                if let Some(gate) = gate {
                    self.condition_started.add_permits(1);
                    gate.acquire().await.unwrap().forget();
                }
            }

            if let Some(if_match) = if_match
                && if_match != "*"
                && if_match != self.etag
            {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    format!("if-match {if_match} does not match {}", self.etag),
                ));
            }
            if let Some(if_none_match) = if_none_match
                && (if_none_match == "*" || if_none_match == self.etag)
            {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    format!("if-none-match {if_none_match} matches {}", self.etag),
                ));
            }
            Ok(())
        }

        fn metadata(&self) -> Metadata {
            let mut metadata = MetadataBuilder::file(self.data.len() as _);
            metadata.etag(self.etag.clone());
            metadata.build()
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
        args: OpRead,
    }

    impl oio::Read for MockReadReader {
        async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            self.state.open_calls.fetch_add(1, Ordering::Relaxed);
            self.state
                .check_conditions(self.args.if_match(), self.args.if_none_match())
                .await?;
            let buffer = self.state.read_range(range);
            Ok((
                self.state.rp_read(),
                Box::new(buffer) as Box<dyn oio::ReadStreamDyn>,
            ))
        }

        async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
            self.state.read_calls.fetch_add(1, Ordering::Relaxed);
            self.state
                .check_conditions(self.args.if_match(), self.args.if_none_match())
                .await?;
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
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("mock")
        }

        fn capability(&self) -> Capability {
            Capability {
                read: true,
                read_with_version: true,
                stat: true,
                stat_with_version: true,
                restore: true,
                restore_with_version: true,
                restore_with_if_not_exists: true,
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

        async fn stat(&self, _: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
            self.state.stat_calls.fetch_add(1, Ordering::Relaxed);
            *self.state.last_stat_args.lock().unwrap() = Some(args.clone());

            let gate = {
                let guard = self.state.stat_gate.lock().unwrap();
                match guard.as_ref() {
                    Some((None, gate)) => Some(gate.clone()),
                    Some((Some(filter), gate)) if filter == path => Some(gate.clone()),
                    _ => None,
                }
            };
            if let Some(gate) = gate {
                self.state.stat_started.add_permits(1);
                gate.acquire().await.unwrap().forget();
            }

            self.state
                .check_conditions(args.if_match(), args.if_none_match())
                .await?;
            Ok(RpStat::new(self.state.metadata()))
        }

        fn read(&self, _ctx: &OperationContext, _: &str, args: OpRead) -> Result<Self::Reader> {
            *self.state.last_read_args.lock().unwrap() = Some(args.clone());
            Ok(MockReadReader {
                state: self.state.clone(),
                args,
            })
        }

        fn write(&self, _ctx: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn list(&self, _ctx: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
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
        OperationContext::new()
    }

    struct ChunkedCopier {
        chunks: std::array::IntoIter<usize, 2>,
    }

    impl oio::Copy for ChunkedCopier {
        async fn next(&mut self) -> Result<Option<usize>> {
            Ok(self.chunks.next())
        }

        async fn close(&mut self) -> Result<Metadata> {
            self.chunks.by_ref().for_each(drop);
            Ok(MetadataBuilder::file(8).build())
        }

        async fn abort(&mut self) -> Result<()> {
            self.chunks.by_ref().for_each(drop);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_chunked_copy_invalidation_waits_for_completion() {
        let cache = memory_cache().await;
        let inner = Arc::new(Inner {
            srv: Arc::new(Memory::default().build().unwrap()),
            ctx: OperationContext::new(),
            cache: cache.clone(),
            deleted_keys: Arc::default(),
        });
        let current = FoyerKey {
            path: "destination".to_string(),
            version: None,
        };
        let versioned = FoyerKey {
            path: current.path.clone(),
            version: Some("previous".to_string()),
        };
        cache.insert(current.clone(), FoyerValue("current".into()));
        cache.insert(versioned.clone(), FoyerValue("previous".into()));
        let mut copier = FoyerCopier::new(
            ChunkedCopier {
                chunks: [3, 5].into_iter(),
            },
            current.path.clone(),
            inner,
        );

        assert_eq!(copier.next().await.unwrap(), Some(3));
        assert!(cache.contains(&current));
        assert_eq!(copier.next().await.unwrap(), Some(5));
        assert!(cache.contains(&current));
        assert_eq!(copier.next().await.unwrap(), None);
        assert!(!cache.contains(&current));
        assert!(cache.contains(&versioned));
    }

    #[tokio::test]
    async fn test_restore_is_not_supported() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("0123456789"));
        assert!(source.capability().restore);

        let service = FoyerLayer::new(cache).apply_service(source);
        let capability = service.capability();
        assert!(!capability.restore);
        assert!(!capability.restore_with_version);
        assert!(!capability.restore_with_if_not_exists);

        let err = service
            .restore(&OperationContext::new(), "test", OpRestore::new())
            .await
            .expect_err("FoyerLayer must reject restore operations");
        assert_eq!(err.kind(), ErrorKind::Unsupported);
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

        let reader = service.read(&ctx, "test", OpRead::default()).unwrap();
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

        let reader = service.read(&ctx, "test", OpRead::default()).unwrap();
        let (_, mut stream) = reader.open(BytesRange::from(0_u64..2)).await.unwrap();
        let buffer = stream.read_all().await.unwrap();

        assert_eq!(buffer.to_vec(), b"01");
        assert_eq!(state.stat_calls.load(Ordering::Relaxed), 1);
        assert_eq!(state.open_calls.load(Ordering::Relaxed), 1);
        assert_eq!(state.read_calls.load(Ordering::Relaxed), 0);

        let reader = service.read(&ctx, "test", OpRead::default()).unwrap();
        let range = BytesRange::from(4_u64..12);
        let (_, mut stream) = reader.open(range).await.unwrap();
        let buffer = stream.read_all().await.unwrap();

        assert_eq!(buffer.to_vec(), b"456789");
        assert_eq!(
            reader.read(range).await.unwrap_err().kind(),
            ErrorKind::Unexpected
        );
        assert_eq!(state.stat_calls.load(Ordering::Relaxed), 1);
        assert_eq!(state.open_calls.load(Ordering::Relaxed), 1);
        assert_eq!(state.read_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_cache_fill_preserves_version() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("0123456789"));
        let state = source.state.clone();
        let service = FoyerLayer::new(cache.clone())
            .with_size_limit(0..100)
            .apply_service(source);
        let ctx = service_context(&service);

        let (_, args, _) = options::ReadOptions {
            version: Some("v1".to_owned()),
            ..Default::default()
        }
        .into();
        let reader = service.read(&ctx, "test", args).unwrap();
        let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
        stream.read_all().await.unwrap();

        let stat_args = state.last_stat_args.lock().unwrap().clone().unwrap();
        assert_eq!(stat_args.version(), Some("v1"));

        let read_args = state.last_read_args.lock().unwrap().clone().unwrap();
        assert_eq!(read_args.version(), Some("v1"));

        assert!(cache.contains(&FoyerKey {
            path: "test".to_owned(),
            version: Some("v1".to_owned()),
        }));
    }

    fn conditional_read_args(if_match: &str) -> OpRead {
        let (_, args, _) = options::ReadOptions {
            if_match: Some(if_match.to_owned()),
            ..Default::default()
        }
        .into();
        args
    }

    #[tokio::test]
    async fn test_conditional_read_bypasses_full_object_cache() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("0123456789"));
        let state = source.state.clone();
        let service = FoyerLayer::new(cache)
            .with_size_limit(0..100)
            .apply_service(source);
        let ctx = service_context(&service);

        // Fill the cache with an unconditional read.
        let reader = service.read(&ctx, "test", OpRead::default()).unwrap();
        let (_, mut stream) = reader.open(BytesRange::from(0_u64..2)).await.unwrap();
        let buffer = stream.read_all().await.unwrap();
        assert_eq!(buffer.to_vec(), b"01");
        assert_eq!(state.stat_calls.load(Ordering::Relaxed), 1);
        assert_eq!(state.open_calls.load(Ordering::Relaxed), 1);

        // A conditional read must not be served from the cache: the cached
        // entry carries no condition, so only the origin can observe it.
        let reader = service
            .read(&ctx, "test", conditional_read_args("\"etag-1\""))
            .unwrap();
        let (_, mut stream) = reader.open(BytesRange::from(0_u64..2)).await.unwrap();
        let buffer = stream.read_all().await.unwrap();
        assert_eq!(buffer.to_vec(), b"01");
        assert_eq!(state.open_calls.load(Ordering::Relaxed), 2);

        // Unconditional reads still hit the cache.
        let reader = service.read(&ctx, "test", OpRead::default()).unwrap();
        let (_, mut stream) = reader.open(BytesRange::from(0_u64..2)).await.unwrap();
        let buffer = stream.read_all().await.unwrap();
        assert_eq!(buffer.to_vec(), b"01");
        assert_eq!(state.open_calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_conditional_read_checks_precondition_on_cache_miss() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("0123456789"));
        let service = FoyerLayer::new(cache)
            .with_size_limit(0..100)
            .apply_service(source);
        let ctx = service_context(&service);

        let reader = service
            .read(&ctx, "test", conditional_read_args("\"wrong-etag\""))
            .unwrap();
        let err = reader
            .open(BytesRange::new(0, None))
            .await
            .err()
            .expect("a mismatched if-match must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
    }

    #[tokio::test]
    async fn test_conditional_read_checks_precondition_on_cache_hit() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("0123456789"));
        let service = FoyerLayer::new(cache)
            .with_size_limit(0..100)
            .apply_service(source);
        let ctx = service_context(&service);

        let reader = service.read(&ctx, "test", OpRead::default()).unwrap();
        let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
        stream.read_all().await.unwrap();

        let reader = service
            .read(&ctx, "test", conditional_read_args("\"wrong-etag\""))
            .unwrap();
        let err = reader
            .open(BytesRange::new(0, None))
            .await
            .err()
            .expect("a cached entry must not satisfy a mismatched if-match");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
    }

    #[tokio::test]
    async fn test_conditional_read_allows_matching_precondition() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("0123456789"));
        let state = source.state.clone();
        let service = FoyerLayer::new(cache.clone())
            .with_size_limit(0..100)
            .apply_service(source);
        let ctx = service_context(&service);

        let reader = service
            .read(&ctx, "test", conditional_read_args("\"etag-1\""))
            .unwrap();
        let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
        let buffer = stream.read_all().await.unwrap();
        assert_eq!(buffer.to_vec(), b"0123456789");
        assert_eq!(state.open_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            state
                .last_read_args
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .if_match(),
            Some("\"etag-1\"")
        );

        // A conditional read never populates the cache.
        assert_eq!(state.stat_calls.load(Ordering::Relaxed), 0);
        assert!(!cache.contains(&FoyerKey {
            path: "test".to_owned(),
            version: None,
        }));
    }

    #[tokio::test]
    async fn test_conditional_read_does_not_join_in_flight_fill() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("0123456789"));
        let state = source.state.clone();
        let gate = state.gate_stat();
        let service = FoyerLayer::new(cache)
            .with_size_limit(0..100)
            .apply_service(source);
        let ctx = service_context(&service);

        // Start an unconditional read and stop it inside the cache fill.
        let fill = tokio::spawn({
            let service = service.clone();
            let ctx = ctx.clone();
            async move {
                let reader = service.read(&ctx, "test", OpRead::default()).unwrap();
                let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
                stream.read_all().await.unwrap()
            }
        });
        state.stat_started.acquire().await.unwrap().forget();

        // Start a conditional read while the unconditional fill is in flight.
        // It must bypass the cache and reach the origin without waiting for the
        // fill, so await it before releasing the gate. The timeout turns a
        // regression into a fast failure instead of a hung test.
        let conditional = tokio::spawn({
            let service = service.clone();
            let ctx = ctx.clone();
            async move {
                let reader = service
                    .read(&ctx, "test", conditional_read_args("\"wrong-etag\""))
                    .unwrap();
                reader.open(BytesRange::new(0, None)).await
            }
        });
        let err = tokio::time::timeout(Duration::from_secs(2), conditional)
            .await
            .expect("a conditional read must not wait for an unconditional fill")
            .unwrap()
            .err()
            .expect("a conditional read must not receive an unconditional fill");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        gate.add_permits(1);

        let fill_buffer = fill.await.unwrap();
        assert_eq!(fill_buffer.to_vec(), b"0123456789");
    }

    #[tokio::test]
    async fn test_unconditional_read_does_not_join_conditional_fill() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("0123456789"));
        let state = source.state.clone();
        let gate = state.gate_condition();
        let service = FoyerLayer::new(cache)
            .with_size_limit(0..100)
            .apply_service(source);
        let ctx = service_context(&service);

        // Start a conditional read with a mismatched if-match and stop it
        // inside the origin precondition check.
        let conditional = tokio::spawn({
            let service = service.clone();
            let ctx = ctx.clone();
            async move {
                let reader = service
                    .read(&ctx, "test", conditional_read_args("\"wrong-etag\""))
                    .unwrap();
                reader.open(BytesRange::new(0, None)).await
            }
        });
        state.condition_started.acquire().await.unwrap().forget();

        // Start an unconditional read while the conditional fill is in flight.
        let (started_tx, started_rx) = oneshot::channel();
        let fill = tokio::spawn({
            let service = service.clone();
            let ctx = ctx.clone();
            async move {
                let reader = service.read(&ctx, "test", OpRead::default()).unwrap();
                started_tx.send(()).unwrap();
                reader.open(BytesRange::new(0, None)).await
            }
        });
        started_rx.await.unwrap();
        tokio::task::yield_now().await;

        gate.add_permits(1);

        let err = conditional
            .await
            .unwrap()
            .err()
            .expect("the conditional read must fail on its precondition");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        let (_, mut stream) = fill
            .await
            .unwrap()
            .expect("an unconditional read must not inherit the conditional failure");
        let buffer = stream.read_all().await.unwrap();
        assert_eq!(buffer.to_vec(), b"0123456789");
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

    #[tokio::test]
    async fn deleted_keys_stay_bounded() {
        let cache = memory_cache().await;
        let layer = FoyerLayer::new(cache);
        let service = layer.apply_service(Arc::new(Memory::default().build().unwrap()));
        let ctx = OperationContext::new();

        for i in 0..=(MAX_DELETED_KEYS * 2) {
            let path = format!("obj-{i}");
            let mut writer = service.write(&ctx, &path, OpWrite::new()).unwrap();
            writer.write(Buffer::from("value")).await.unwrap();
            writer.close().await.unwrap();

            let mut deleter = service.delete(&ctx).unwrap();
            deleter.delete(&path, OpDelete::new()).await.unwrap();
            deleter.close().await.unwrap();
        }

        let len = layer.deleted_keys.len();
        let aux_len = layer.deleted_keys.auxiliary_len();
        assert!(
            len <= MAX_DELETED_KEYS,
            "deleted key tombstones grew to {len}, expected at most {MAX_DELETED_KEYS}"
        );
        assert_eq!(
            aux_len, len,
            "auxiliary invalidation index grew to {aux_len}, expected {len}"
        );

        // Repeated write/delete cycles on the same key must not leave stale
        // ordering entries behind.
        for _ in 0..=(MAX_DELETED_KEYS * 2) {
            let mut writer = service.write(&ctx, "reused", OpWrite::new()).unwrap();
            writer.write(Buffer::from("value")).await.unwrap();
            writer.close().await.unwrap();

            let mut deleter = service.delete(&ctx).unwrap();
            deleter.delete("reused", OpDelete::new()).await.unwrap();
            deleter.close().await.unwrap();
        }

        assert!(layer.deleted_keys.len() <= MAX_DELETED_KEYS);
        assert_eq!(layer.deleted_keys.auxiliary_len(), layer.deleted_keys.len());
    }

    #[tokio::test]
    async fn invalidation_during_fill_does_not_repopulate_cache() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("0123456789"));
        let state = source.state.clone();
        let gate = state.gate_stat();
        let layer = FoyerLayer::new(cache.clone());
        let service = layer.apply_service(source);
        let ctx = OperationContext::new();

        let fill = tokio::spawn({
            let service = service.clone();
            let ctx = ctx.clone();
            async move {
                let reader = service.read(&ctx, "test", OpRead::default()).unwrap();
                let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
                stream.read_all().await.unwrap()
            }
        });
        state.stat_started.acquire().await.unwrap().forget();

        layer.deleted_keys.invalidate(
            &cache,
            &[FoyerKey {
                path: "test".to_string(),
                version: None,
            }],
            &[],
        );
        gate.add_permits(1);

        let buffer = fill.await.unwrap();
        assert_eq!(buffer.to_vec(), b"0123456789");
        assert!(
            !cache.contains(&FoyerKey {
                path: "test".to_string(),
                version: None,
            }),
            "a fill that started before invalidation must not repopulate the cache"
        );
        assert_eq!(
            layer.deleted_keys.pinned_len(),
            0,
            "pin must be released once the invalidation readers finish"
        );
        assert_eq!(
            layer.deleted_keys.len(),
            1,
            "released invalidation pin must transition to the bounded idle tombstone set"
        );
        // Recently invalidated paths keep bypassing the cache until a write repopulates them.
        let reader = service.read(&ctx, "test", OpRead::default()).unwrap();
        let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
        assert_eq!(stream.read_all().await.unwrap().to_vec(), b"0123456789");
        assert!(
            !cache.contains(&FoyerKey {
                path: "test".to_string(),
                version: None,
            }),
            "a read after invalidation pin release must still bypass the cache until a write"
        );
    }

    #[tokio::test]
    async fn interleaved_invalidation_keeps_older_read_pinned() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("stale-data"));
        let state = source.state.clone();
        let gate = state.gate_stat_for("target");
        let layer = FoyerLayer::new(cache.clone());
        let service = layer.apply_service(source);
        let ctx = OperationContext::new();

        // R1 starts reading "target" and pauses inside Foyer's fetch.
        let r1 = tokio::spawn({
            let service = service.clone();
            let ctx = ctx.clone();
            async move {
                let reader = service.read(&ctx, "target", OpRead::default()).unwrap();
                let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
                stream.read_all().await.unwrap()
            }
        });
        state.stat_started.acquire().await.unwrap().forget();

        // An unrelated invalidation followed by a newer read R2 that finishes
        // must not lose track of R1.
        layer.deleted_keys.invalidate(
            &cache,
            &[FoyerKey {
                path: "unrelated".to_string(),
                version: None,
            }],
            &[],
        );
        {
            let reader = service.read(&ctx, "other", OpRead::default()).unwrap();
            let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
            let _ = stream.read_all().await.unwrap();
        }

        // Now invalidate "target" while R1 is still in flight.
        layer.deleted_keys.invalidate(
            &cache,
            &[FoyerKey {
                path: "target".to_string(),
                version: None,
            }],
            &[],
        );
        gate.add_permits(1);

        let buffer = r1.await.unwrap();
        assert_eq!(buffer.to_vec(), b"stale-data");
        assert!(
            !cache.contains(&FoyerKey {
                path: "target".to_string(),
                version: None,
            }),
            "older read completing after interleaved invalidation must not leave target cached"
        );
    }

    #[tokio::test]
    async fn cancelled_read_does_not_repopulate_cache_after_invalidation() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("stale-data"));
        let state = source.state.clone();
        let gate = state.gate_stat_for("target");
        let layer = FoyerLayer::new(cache.clone());
        let service = layer.apply_service(source);
        let ctx = OperationContext::new();
        let target_key = FoyerKey {
            path: "target".to_string(),
            version: None,
        };

        let r1 = tokio::spawn({
            let service = service.clone();
            let ctx = ctx.clone();
            async move {
                let reader = service.read(&ctx, "target", OpRead::default()).unwrap();
                let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
                stream.read_all().await.unwrap()
            }
        });
        state.stat_started.acquire().await.unwrap().forget();

        layer
            .deleted_keys
            .invalidate(&cache, std::slice::from_ref(&target_key), &[]);

        // Cancel the reader future while Foyer's detached fetch is still blocked.
        r1.abort();
        let _ = r1.await;

        // Attach a passive waiter to Foyer's inflight before releasing the backend gate
        // so we deterministically observe the detached fetch finishing.
        let wait_inflight = cache.get(&target_key);
        gate.add_permits(1);
        let _ = wait_inflight.await;

        assert!(
            !cache.contains(&target_key),
            "detached fetch from a cancelled read must not repopulate an invalidated key"
        );
    }

    #[derive(Debug)]
    struct GatedMemoryService {
        inner: Servicer,
        gate_path: String,
        stat_started: Arc<Semaphore>,
        stat_gate: Arc<Semaphore>,
        stat_done: Arc<Semaphore>,
        gate_once: AtomicUsize,
        stat_calls: AtomicUsize,
    }

    impl Service for GatedMemoryService {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
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
            self.stat_calls.fetch_add(1, Ordering::Relaxed);
            if path == self.gate_path
                && self
                    .gate_once
                    .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                struct SignalOnDrop(Arc<Semaphore>);
                impl Drop for SignalOnDrop {
                    fn drop(&mut self) {
                        self.0.add_permits(1);
                    }
                }
                let _signal = SignalOnDrop(self.stat_done.clone());
                self.stat_started.add_permits(1);
                self.stat_gate.acquire().await.unwrap().forget();
            }
            self.inner.stat(ctx, path, args).await
        }

        fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
            self.inner.read(ctx, path, args)
        }

        fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
            self.inner.write(ctx, path, args)
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
    async fn older_read_completing_after_delete_and_write_preserves_new_entry() {
        let cache = memory_cache().await;
        let mem: Servicer = Arc::new(Memory::default().build().unwrap());
        let ctx = OperationContext::new();

        // Populate backend directly so the initial read misses cache and fetches from backend.
        let mut init_writer = mem.write(&ctx, "target", OpWrite::new()).unwrap();
        init_writer.write(Buffer::from("old")).await.unwrap();
        init_writer.close().await.unwrap();

        let stat_started = Arc::new(Semaphore::new(0));
        let stat_gate = Arc::new(Semaphore::new(0));
        let stat_done = Arc::new(Semaphore::new(0));
        let gated = Arc::new(GatedMemoryService {
            inner: mem,
            gate_path: "target".to_string(),
            stat_started: stat_started.clone(),
            stat_gate: stat_gate.clone(),
            stat_done: stat_done.clone(),
            gate_once: AtomicUsize::new(1),
            stat_calls: AtomicUsize::new(0),
        });
        let layer = FoyerLayer::new(cache.clone());
        let service = layer.apply_service(gated.clone());
        let target_key = FoyerKey {
            path: "target".to_string(),
            version: None,
        };

        let older_read = tokio::spawn({
            let service = service.clone();
            let ctx = ctx.clone();
            async move {
                let reader = service.read(&ctx, "target", OpRead::default()).unwrap();
                let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
                stream.read_all().await.unwrap()
            }
        });
        stat_started.acquire().await.unwrap().forget();

        // Delete then write a fresh value while the older read is still in flight.
        let mut deleter = service.delete(&ctx).unwrap();
        deleter.delete("target", OpDelete::new()).await.unwrap();
        deleter.close().await.unwrap();

        let mut writer = service.write(&ctx, "target", OpWrite::new()).unwrap();
        writer.write(Buffer::from("new")).await.unwrap();
        writer.close().await.unwrap();
        assert!(cache.contains(&target_key));

        // Let the older read and its detached fetch task finish completely.
        stat_gate.add_permits(1);
        let _ = older_read.await.unwrap();
        stat_done.acquire().await.unwrap().forget();
        tokio::task::yield_now().await;

        // The newly written cache entry must remain cached with its fresh value.
        assert!(
            cache.contains(&target_key),
            "older read completing after delete+write must not evict the new cache entry"
        );
        let stat_before = gated.stat_calls.load(Ordering::Relaxed);
        let reader = service.read(&ctx, "target", OpRead::default()).unwrap();
        let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
        let fresh = stream.read_all().await.unwrap();
        assert_eq!(fresh.to_vec(), b"new");
        assert_eq!(
            gated.stat_calls.load(Ordering::Relaxed),
            stat_before,
            "subsequent read should hit the newly written cache entry"
        );
    }

    #[test]
    fn active_read_epochs_promotes_and_demotes() {
        let mut epochs = ActiveReadEpochs::single(1);
        assert_eq!(epochs, ActiveReadEpochs::Single { epoch: 1, count: 1 });

        epochs.insert(1);
        assert_eq!(epochs, ActiveReadEpochs::Single { epoch: 1, count: 2 });

        // Distinct epoch promotes from Single to Multiple.
        epochs.insert(2);
        assert!(matches!(epochs, ActiveReadEpochs::Multiple(_)));

        // Decrementing epoch 1 from 2 to 1 keeps both epochs active.
        assert_eq!(epochs.remove(1), (false, Some(1)));
        assert!(matches!(epochs, ActiveReadEpochs::Multiple(_)));

        // Removing epoch 1 leaves only epoch 2 and demotes back to Single.
        assert_eq!(epochs.remove(1), (true, Some(2)));
        assert_eq!(epochs, ActiveReadEpochs::Single { epoch: 2, count: 1 });

        // Removing the last epoch returns None so the caller drops the key.
        assert_eq!(epochs.remove(2), (true, None));
    }

    #[tokio::test]
    async fn newer_reader_joining_stale_leader_fetch_falls_back_to_origin() {
        struct StaleJoinReader {
            inner: oio::Reader,
            should_gate: bool,
            open_started: Arc<Semaphore>,
            open_gate: Arc<Semaphore>,
        }

        impl oio::Read for StaleJoinReader {
            async fn open(
                &self,
                range: BytesRange,
            ) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
                let (rp, mut stream) = self.inner.open(range).await?;
                if self.should_gate {
                    let data = stream.read_all().await?;
                    self.open_started.add_permits(1);
                    self.open_gate.acquire().await.unwrap().forget();
                    return Ok((rp, Box::new(data) as Box<dyn oio::ReadStreamDyn>));
                }
                Ok((rp, stream))
            }

            async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
                self.inner.read(range).await
            }
        }

        #[derive(Debug)]
        struct StaleJoinService {
            inner: Servicer,
            other_stat_started: Arc<Semaphore>,
            other_stat_gate: Arc<Semaphore>,
            other_gate_once: AtomicUsize,
            target_open_started: Arc<Semaphore>,
            target_open_gate: Arc<Semaphore>,
            target_gate_once: AtomicUsize,
        }

        impl Service for StaleJoinService {
            type Reader = StaleJoinReader;
            type Writer = oio::Writer;
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

            async fn stat(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpStat,
            ) -> Result<RpStat> {
                if path == "other"
                    && self
                        .other_gate_once
                        .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                {
                    self.other_stat_started.add_permits(1);
                    self.other_stat_gate.acquire().await.unwrap().forget();
                }
                self.inner.stat(ctx, path, args).await
            }

            fn read(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpRead,
            ) -> Result<Self::Reader> {
                let should_gate = path == "target"
                    && self
                        .target_gate_once
                        .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok();
                Ok(StaleJoinReader {
                    inner: self.inner.read(ctx, path, args)?,
                    should_gate,
                    open_started: self.target_open_started.clone(),
                    open_gate: self.target_open_gate.clone(),
                })
            }

            fn write(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpWrite,
            ) -> Result<Self::Writer> {
                self.inner.write(ctx, path, args)
            }

            fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
                self.inner.delete(ctx)
            }

            fn list(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpList,
            ) -> Result<Self::Lister> {
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

        let cache = memory_cache().await;
        let mem: Servicer = Arc::new(Memory::default().build().unwrap());
        let ctx = OperationContext::new();
        for (path, data) in [("target", "old"), ("other", "other")] {
            let mut w = mem.write(&ctx, path, OpWrite::new()).unwrap();
            w.write(Buffer::from(data)).await.unwrap();
            w.close().await.unwrap();
        }

        let other_stat_started = Arc::new(Semaphore::new(0));
        let other_stat_gate = Arc::new(Semaphore::new(0));
        let target_open_started = Arc::new(Semaphore::new(0));
        let target_open_gate = Arc::new(Semaphore::new(0));
        let srv = Arc::new(StaleJoinService {
            inner: mem.clone(),
            other_stat_started: other_stat_started.clone(),
            other_stat_gate: other_stat_gate.clone(),
            other_gate_once: AtomicUsize::new(1),
            target_open_started: target_open_started.clone(),
            target_open_gate: target_open_gate.clone(),
            target_gate_once: AtomicUsize::new(1),
        });
        let layer = FoyerLayer::new(cache.clone());
        let service = layer.apply_service(srv);

        // Hold an active read on "other" so invalidating "other" advances the global epoch.
        let r_other = tokio::spawn({
            let service = service.clone();
            let ctx = ctx.clone();
            async move {
                let reader = service.read(&ctx, "other", OpRead::default()).unwrap();
                let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
                stream.read_all().await.unwrap()
            }
        });
        other_stat_started.acquire().await.unwrap().forget();

        // Leader R1 starts at epoch 0, reads "old", and pauses before returning to Foyer.
        let r1 = tokio::spawn({
            let service = service.clone();
            let ctx = ctx.clone();
            async move {
                let reader = service.read(&ctx, "target", OpRead::default()).unwrap();
                let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
                stream.read_all().await.unwrap()
            }
        });
        target_open_started.acquire().await.unwrap().forget();

        // Advance the global epoch via an unrelated invalidation.
        layer.deleted_keys.invalidate(
            &cache,
            &[FoyerKey {
                path: "other".to_string(),
                version: None,
            }],
            &[],
        );

        // Newer reader R2 starts at epoch 1 and joins R1's inflight Foyer fetch.
        let r2_reader = service.read(&ctx, "target", OpRead::default()).unwrap();
        let r2_fut = async {
            let (_, mut stream) = r2_reader.open(BytesRange::new(0, None)).await.unwrap();
            stream.read_all().await.unwrap()
        };
        futures::pin_mut!(r2_fut);
        assert!(futures::poll!(&mut r2_fut).is_pending());

        // Update backend to "new" and invalidate "target" so R1's fetch becomes Stale.
        let mut w = mem.write(&ctx, "target", OpWrite::new()).unwrap();
        w.write(Buffer::from("new")).await.unwrap();
        w.close().await.unwrap();
        layer.deleted_keys.invalidate(
            &cache,
            &[FoyerKey {
                path: "target".to_string(),
                version: None,
            }],
            &[],
        );

        target_open_gate.add_permits(1);
        other_stat_gate.add_permits(1);

        let r2_buf = r2_fut.await;
        let r1_buf = r1.await.unwrap();
        let _ = r_other.await.unwrap();

        assert_eq!(r1_buf.to_vec(), b"old");
        assert_eq!(
            r2_buf.to_vec(),
            b"new",
            "a newer reader joining an older leader's stale fetch must fall back to origin"
        );
        assert!(!cache.contains(&FoyerKey {
            path: "target".to_string(),
            version: None,
        }));
    }

    #[tokio::test]
    async fn pending_store_repair_restores_fresh_entry_before_next_read() {
        let cache = memory_cache().await;
        let deleted_keys = Arc::new(DeletedKeys::default());
        let key = Arc::new(FoyerKey {
            path: "target".to_string(),
            version: None,
        });

        let read_guard = deleted_keys.begin_read(&cache, &key).unwrap();
        deleted_keys.store(
            &cache,
            key.as_ref().clone(),
            FoyerValue(Buffer::from("fresh")),
            None,
        );

        // Simulate a late Foyer disk-load (`optional_fetch`) promoting stale bytes
        // into memory before the older read finishes.
        cache.insert(key.as_ref().clone(), FoyerValue(Buffer::from("stale-disk")));
        drop(read_guard);

        // The next read must flush `PendingRepair::Store` before consulting Foyer.
        let next_guard = deleted_keys.begin_read(&cache, &key).unwrap();
        let entry = cache.get(key.as_ref()).await.unwrap().unwrap();
        assert_eq!(entry.0.to_vec(), b"fresh");
        drop(next_guard);
    }

    #[tokio::test]
    async fn pending_evict_repair_removes_late_disk_insert_before_next_read() {
        let cache = memory_cache().await;
        let deleted_keys = Arc::new(DeletedKeys::default());
        let key = Arc::new(FoyerKey {
            path: "target".to_string(),
            version: None,
        });

        let read_guard = deleted_keys.begin_read(&cache, &key).unwrap();
        deleted_keys.evict(&cache, key.as_ref());

        // Simulate a late Foyer disk-load (`optional_fetch`) promoting stale bytes
        // into memory before the older read finishes.
        cache.insert(key.as_ref().clone(), FoyerValue(Buffer::from("stale-disk")));
        drop(read_guard);

        // The next read must flush `PendingRepair::Remove` before consulting Foyer
        // so it misses and refills fresh data instead of serving stale bytes.
        let next_guard = deleted_keys.begin_read(&cache, &key).unwrap();
        assert!(!cache.contains(key.as_ref()));
        drop(next_guard);
    }

    #[tokio::test]
    async fn downgrade_stored_pins_prevents_resurrection_after_clear() {
        let cache = memory_cache().await;
        let deleted_keys = Arc::new(DeletedKeys::default());
        let key = Arc::new(FoyerKey {
            path: "dir/target".to_string(),
            version: None,
        });

        let read_guard = deleted_keys.begin_read(&cache, &key).unwrap();
        deleted_keys.store(
            &cache,
            key.as_ref().clone(),
            FoyerValue(Buffer::from("pre-clear")),
            None,
        );

        deleted_keys.downgrade_stored_pins();
        cache.clear().await.unwrap();
        deleted_keys.downgrade_stored_pins();

        drop(read_guard);
        let next_guard = deleted_keys.begin_read(&cache, &key).unwrap();
        assert!(
            !cache.contains(key.as_ref()),
            "downgraded store pin must not resurrect an entry after recursive cache clear"
        );
        drop(next_guard);
    }

    #[test]
    fn test_error() {
        let e = Error::new(ErrorKind::NotFound, "not found");
        let fe = FoyerError::new(FoyerErrorKind::External, "external error").with_source(e);
        let oe = extract_err(fe);
        assert_eq!(oe.kind(), ErrorKind::NotFound);
        assert!(oe.is_permanent());

        let temp = Error::new(ErrorKind::RateLimited, "throttled")
            .with_context("attempt", "1")
            .set_temporary();
        let fe_temp = FoyerError::new(FoyerErrorKind::External, "fetch failed")
            .with_source(FetchError::from_error(temp));
        let oe_temp = extract_err(fe_temp);
        assert_eq!(oe_temp.kind(), ErrorKind::RateLimited);
        assert_eq!(oe_temp.message(), "throttled");
        assert!(oe_temp.is_temporary());
        assert!(oe_temp.to_string().contains("attempt: 1"));

        let persistent = Error::new(ErrorKind::Unexpected, "exhausted").set_persistent();
        let fe_persistent = FoyerError::new(FoyerErrorKind::External, "fetch failed")
            .with_source(FetchError::from_error(persistent));
        let oe_persistent = extract_err(fe_persistent);
        assert_eq!(oe_persistent.kind(), ErrorKind::Unexpected);
        assert!(oe_persistent.is_persistent());
        assert!(!oe_persistent.is_temporary());

        let direct_temp = Error::new(ErrorKind::RateLimited, "direct").set_temporary();
        let fe_direct =
            FoyerError::new(FoyerErrorKind::External, "direct").with_source(direct_temp);
        let oe_direct = extract_err(fe_direct);
        assert_eq!(oe_direct.kind(), ErrorKind::RateLimited);
        assert!(oe_direct.is_temporary());
    }

    #[tokio::test]
    async fn test_with_size_limit_usize_max_bounds() {
        let cache = memory_cache().await;

        let inclusive_unbounded = FoyerLayer::new(cache.clone()).with_size_limit(..=usize::MAX);
        assert_eq!(inclusive_unbounded.size_limit, 0..usize::MAX);
        assert!(inclusive_unbounded.size_limit.contains(&0));
        assert!(inclusive_unbounded.size_limit.contains(&(usize::MAX - 1)));

        let inclusive_zero = FoyerLayer::new(cache.clone()).with_size_limit(0..=usize::MAX);
        assert_eq!(inclusive_zero.size_limit, 0..usize::MAX);

        let from_max = FoyerLayer::new(cache.clone()).with_size_limit(usize::MAX..);
        assert!(from_max.size_limit.is_empty());

        let excluded_max = FoyerLayer::new(cache.clone())
            .with_size_limit((Bound::Excluded(usize::MAX), Bound::Unbounded));
        assert!(excluded_max.size_limit.is_empty());
        assert!(!excluded_max.size_limit.contains(&0));

        let shifted =
            FoyerLayer::new(cache).with_size_limit((Bound::Excluded(5), Bound::Included(10)));
        assert_eq!(shifted.size_limit, 6..11);
    }

    #[derive(Debug)]
    struct FaultReadBuilder {
        capability: Capability,
        stat_mode: EntryMode,
        stat_length: u64,
        data: Vec<u8>,
        chunk_size: usize,
        stat_fail_once: Arc<Mutex<Option<Error>>>,
        open_fail_once: Arc<Mutex<Option<Error>>>,
        stat_calls: Arc<AtomicUsize>,
        open_calls: Arc<AtomicUsize>,
        read_calls: Arc<AtomicUsize>,
    }

    impl Default for FaultReadBuilder {
        fn default() -> Self {
            Self::new(&[])
        }
    }

    impl FaultReadBuilder {
        fn new(data: &[u8]) -> Self {
            Self {
                capability: Capability {
                    read: true,
                    stat: true,
                    ..Default::default()
                },
                stat_mode: EntryMode::FILE,
                stat_length: data.len() as u64,
                data: data.to_vec(),
                chunk_size: data.len().max(1),
                stat_fail_once: Arc::new(Mutex::new(None)),
                open_fail_once: Arc::new(Mutex::new(None)),
                stat_calls: Arc::new(AtomicUsize::new(0)),
                open_calls: Arc::new(AtomicUsize::new(0)),
                read_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[derive(Debug)]
    struct FaultReadService {
        capability: Capability,
        stat_mode: EntryMode,
        stat_length: u64,
        data: Vec<u8>,
        chunk_size: usize,
        stat_fail_once: Arc<Mutex<Option<Error>>>,
        open_fail_once: Arc<Mutex<Option<Error>>>,
        stat_calls: Arc<AtomicUsize>,
        open_calls: Arc<AtomicUsize>,
        read_calls: Arc<AtomicUsize>,
    }

    impl Builder for FaultReadBuilder {
        type Config = ();

        fn build(self) -> Result<impl Service> {
            Ok(FaultReadService {
                capability: self.capability,
                stat_mode: self.stat_mode,
                stat_length: self.stat_length,
                data: self.data,
                chunk_size: self.chunk_size,
                stat_fail_once: self.stat_fail_once,
                open_fail_once: self.open_fail_once,
                stat_calls: self.stat_calls,
                open_calls: self.open_calls,
                read_calls: self.read_calls,
            })
        }
    }

    struct ChunkedSliceStream {
        data: Vec<u8>,
        offset: usize,
        chunk_size: usize,
    }

    impl oio::ReadStream for ChunkedSliceStream {
        async fn read(&mut self) -> Result<Buffer> {
            if self.offset >= self.data.len() {
                return Ok(Buffer::new());
            }
            let end = (self.offset + self.chunk_size).min(self.data.len());
            let chunk = Buffer::from(self.data[self.offset..end].to_vec());
            self.offset = end;
            Ok(chunk)
        }
    }

    struct FaultReader {
        data: Vec<u8>,
        chunk_size: usize,
        open_fail_once: Arc<Mutex<Option<Error>>>,
        open_calls: Arc<AtomicUsize>,
        read_calls: Arc<AtomicUsize>,
    }

    impl oio::Read for FaultReader {
        async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            self.open_calls.fetch_add(1, Ordering::Relaxed);
            if let Some(err) = self.open_fail_once.lock().unwrap().take() {
                return Err(err);
            }
            let content_range = range.to_content_range(self.data.len())?;
            let slice = self.data[content_range].to_vec();
            let rp = RpRead::new(MetadataBuilder::file(self.data.len() as u64).build());
            Ok((
                rp,
                Box::new(ChunkedSliceStream {
                    data: slice,
                    offset: 0,
                    chunk_size: self.chunk_size,
                }) as Box<dyn oio::ReadStreamDyn>,
            ))
        }

        async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
            self.read_calls.fetch_add(1, Ordering::Relaxed);
            let content_range = range.to_content_range(self.data.len())?;
            let rp = RpRead::new(MetadataBuilder::file(self.data.len() as u64).build());
            Ok((rp, Buffer::from(self.data[content_range].to_vec())))
        }
    }

    impl Service for FaultReadService {
        type Reader = FaultReader;
        type Writer = ();
        type Lister = ();
        type Deleter = ();
        type Copier = ();
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("fault-read")
        }

        fn capability(&self) -> Capability {
            self.capability
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

        async fn stat(&self, _: &OperationContext, _: &str, args: OpStat) -> Result<RpStat> {
            self.stat_calls.fetch_add(1, Ordering::Relaxed);
            if !self.capability.stat
                || (args.version().is_some() && !self.capability.stat_with_version)
            {
                return Err(Error::new(ErrorKind::Unsupported, "stat is not supported"));
            }
            if let Some(err) = self.stat_fail_once.lock().unwrap().take() {
                return Err(err);
            }
            let metadata = match self.stat_mode {
                EntryMode::FILE => MetadataBuilder::file(self.stat_length).build(),
                EntryMode::DIR => MetadataBuilder::dir().build(),
                EntryMode::Unknown => MetadataBuilder::unknown().build(),
            };
            Ok(RpStat::new(metadata))
        }

        fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
            Ok(FaultReader {
                data: self.data.clone(),
                chunk_size: self.chunk_size,
                open_fail_once: self.open_fail_once.clone(),
                open_calls: self.open_calls.clone(),
                read_calls: self.read_calls.clone(),
            })
        }

        fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
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

    #[tokio::test]
    async fn test_temporary_error_on_cache_miss_preserves_status_and_retries() {
        use opendal_layer_retry::RetryLayer;

        // 1. Transient error from `stat` preserves `is_temporary()` and succeeds via `RetryLayer`.
        let cache = memory_cache().await;
        let builder = FaultReadBuilder::new(b"hello");
        *builder.stat_fail_once.lock().unwrap() =
            Some(Error::new(ErrorKind::RateLimited, "stat throttled").set_temporary());
        let stat_calls = builder.stat_calls.clone();
        let op = Operator::new(builder)
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()));

        let err = op.read("k").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::RateLimited);
        assert!(err.is_temporary());
        assert_eq!(stat_calls.load(Ordering::Relaxed), 1);

        let cache = memory_cache().await;
        let builder = FaultReadBuilder::new(b"hello");
        *builder.stat_fail_once.lock().unwrap() =
            Some(Error::new(ErrorKind::RateLimited, "stat throttled").set_temporary());
        let stat_calls = builder.stat_calls.clone();
        let op = Operator::new(builder)
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()))
            .layer(RetryLayer::new().with_min_delay(Duration::from_millis(1)));

        assert_eq!(op.read("k").await.unwrap().to_vec(), b"hello");
        assert_eq!(stat_calls.load(Ordering::Relaxed), 2);
        assert!(cache.contains(&FoyerKey {
            path: "k".to_string(),
            version: None,
        }));

        // 2. Transient error from `open` also retries through `RetryLayer`.
        let cache = memory_cache().await;
        let builder = FaultReadBuilder::new(b"world");
        *builder.open_fail_once.lock().unwrap() =
            Some(Error::new(ErrorKind::Unexpected, "transient open").set_temporary());
        let open_calls = builder.open_calls.clone();
        let op = Operator::new(builder)
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()))
            .layer(RetryLayer::new().with_min_delay(Duration::from_millis(1)));

        assert_eq!(op.read("k").await.unwrap().to_vec(), b"world");
        assert_eq!(open_calls.load(Ordering::Relaxed), 2);

        // 3. Persistent error stays persistent and is not retried.
        let cache = memory_cache().await;
        let builder = FaultReadBuilder::new(b"nope");
        *builder.stat_fail_once.lock().unwrap() =
            Some(Error::new(ErrorKind::Unexpected, "already retried").set_persistent());
        let stat_calls = builder.stat_calls.clone();
        let op = Operator::new(builder)
            .unwrap()
            .layer(FoyerLayer::new(cache))
            .layer(RetryLayer::new().with_min_delay(Duration::from_millis(1)));

        let err = op.read("k").await.unwrap_err();
        assert!(err.is_persistent());
        assert!(!err.is_temporary());
        assert_eq!(stat_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_read_falls_back_when_stat_or_stat_with_version_unsupported() {
        // 1. Service without `stat` capability falls back to direct read.
        let cache = memory_cache().await;
        let mut builder = FaultReadBuilder::new(b"0123456789");
        builder.capability.stat = false;
        let stat_calls = builder.stat_calls.clone();
        let op = Operator::new(builder)
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()));

        assert_eq!(op.read("k").await.unwrap().to_vec(), b"0123456789");
        assert_eq!(
            op.read_with("k").range(2..5).await.unwrap().to_vec(),
            b"234"
        );
        assert_eq!(stat_calls.load(Ordering::Relaxed), 0);
        assert!(!cache.contains(&FoyerKey {
            path: "k".to_string(),
            version: None,
        }));

        // 2. Service with `stat` + `read_with_version` but without `stat_with_version`
        //    falls back for versioned reads while still caching unversioned reads.
        let cache = memory_cache().await;
        let mut builder = FaultReadBuilder::new(b"versioned-body");
        builder.capability.read_with_version = true;
        builder.capability.stat_with_version = false;
        let stat_calls = builder.stat_calls.clone();
        let op = Operator::new(builder)
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()));

        assert_eq!(
            op.read_with("k").version("v1").await.unwrap().to_vec(),
            b"versioned-body"
        );
        assert_eq!(stat_calls.load(Ordering::Relaxed), 0);
        assert!(!cache.contains(&FoyerKey {
            path: "k".to_string(),
            version: Some("v1".to_string()),
        }));

        assert_eq!(op.read("k").await.unwrap().to_vec(), b"versioned-body");
        assert_eq!(stat_calls.load(Ordering::Relaxed), 1);
        assert!(cache.contains(&FoyerKey {
            path: "k".to_string(),
            version: None,
        }));
    }

    #[tokio::test]
    async fn test_read_rechecks_size_limit_and_entry_mode_without_caching_oversized() {
        // 1. Object grows past `size_limit.end` between `stat` and `read`:
        //    caller receives all 32 bytes and cache remains empty.
        let cache = memory_cache().await;
        let data = vec![b'x'; 32];
        let mut builder = FaultReadBuilder::new(&data);
        builder.stat_length = 4;
        builder.chunk_size = 6;
        let op = Operator::new(builder)
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()).with_size_limit(0..10));
        let key = FoyerKey {
            path: "grown".to_string(),
            version: None,
        };

        assert_eq!(op.read("grown").await.unwrap().to_vec(), data);
        assert!(!cache.contains(&key));

        // 2. Object shrinks below `size_limit.start` between `stat` and `read`:
        //    caller receives the 5 bytes and cache remains empty.
        let cache = memory_cache().await;
        let mut builder = FaultReadBuilder::new(b"short");
        builder.stat_length = 15;
        let op = Operator::new(builder)
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()).with_size_limit(10..20));
        let key = FoyerKey {
            path: "shrunk".to_string(),
            version: None,
        };

        assert_eq!(op.read("shrunk").await.unwrap().to_vec(), b"short");
        assert!(!cache.contains(&key));

        // 3. `EntryMode::Unknown` from `stat` bypasses cache instead of treating
        //    `content_length == 0` as a cacheable empty file.
        let cache = memory_cache().await;
        let mut builder = FaultReadBuilder::new(b"unknown-mode-data");
        builder.stat_mode = EntryMode::Unknown;
        builder.stat_length = 0;
        let op = Operator::new(builder)
            .unwrap()
            .layer(FoyerLayer::new(cache.clone()).with_size_limit(0..100));
        let key = FoyerKey {
            path: "unknown".to_string(),
            version: None,
        };

        assert_eq!(
            op.read("unknown").await.unwrap().to_vec(),
            b"unknown-mode-data"
        );
        assert!(!cache.contains(&key));
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
