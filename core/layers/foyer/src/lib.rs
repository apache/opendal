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
    sync::{Arc, Mutex, MutexGuard},
};

use foyer::{Code, HybridCache, Result as FoyerResult};

use opendal_core::raw::*;
use opendal_core::*;

use copier::FoyerCopier;
pub use deleter::Deleter;
pub use writer::Writer;

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

    fn insert(&mut self, key: FoyerKey, cache: &HybridCache<FoyerKey, FoyerValue>) {
        let seq = self.next_seq;
        self.next_seq += 1;

        let shared_key = Arc::new(key);
        if let Some(old_seq) = self.keys.insert(shared_key.clone(), seq) {
            self.by_seq.remove(&old_seq);
        }
        self.by_seq.insert(seq, shared_key);

        while self.keys.len() > MAX_DELETED_KEYS {
            let Some((_, oldest_key)) = self.by_seq.pop_first() else {
                break;
            };
            self.keys.remove(&oldest_key);
            cache.remove(oldest_key.as_ref());
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PinnedKind {
    /// Invalidated or evicted while older reads of this key were in flight.
    /// New reads bypass the cache until those reads finish, at which point the
    /// key is removed from the cache again and released.
    Invalidate,
    /// Overwritten by a cacheable write while older reads of this key were in
    /// flight. New reads can hit the cached entry, while older fetches abort
    /// cache insertion without evicting the newer value when they finish.
    Store,
}

#[derive(Debug, Clone, Copy)]
struct PinnedEntry {
    epoch: u64,
    kind: PinnedKind,
}

/// Invalidated cache keys and in-flight read/delete fences.
///
/// A key stays pinned while reads that started before its invalidation (or a
/// `Deleter::close` call targeting the key) are still in flight, so racing
/// cache fills cannot leave stale data behind. Once those reads finish, any
/// entry they could have inserted is removed and the pin is released.
/// Invalidations without in-flight reads are retained up to
/// [`MAX_DELETED_KEYS`] so recently mutated paths keep bypassing the cache
/// until a write through the layer repopulates them.
#[derive(Debug, Default)]
struct DeletedKeys {
    state: Mutex<DeletedKeysState>,
}

#[derive(Debug, Default)]
struct DeletedKeysState {
    epoch: u64,
    active_reads: HashMap<FoyerKey, BTreeMap<u64, usize>>,
    pinned: HashMap<FoyerKey, PinnedEntry>,
    active_deletes: HashMap<FoyerKey, usize>,
    idle: IdleTombstones,
}

impl DeletedKeysState {
    fn apply_invalidation(
        &mut self,
        cache: &HybridCache<FoyerKey, FoyerValue>,
        keys: &[FoyerKey],
        evict_keys: &[FoyerKey],
    ) {
        for key in evict_keys {
            self.idle.remove(key);
            cache.remove(key);
            if self.active_reads.contains_key(key) {
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
            }
        }

        for key in keys {
            cache.remove(key);
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
                self.idle.insert(key.clone(), cache);
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ReadGuard(Arc<ReadGuardInner>);

impl ReadGuard {
    fn epoch(&self) -> u64 {
        self.0.epoch
    }
}

#[derive(Debug)]
struct ReadGuardInner {
    deleted_keys: Arc<DeletedKeys>,
    cache: HybridCache<FoyerKey, FoyerValue>,
    key: FoyerKey,
    epoch: u64,
}

impl Drop for ReadGuardInner {
    fn drop(&mut self) {
        self.deleted_keys
            .finish_read(&self.cache, &self.key, self.epoch);
    }
}

#[derive(Debug)]
pub(crate) struct DeleteGuard {
    deleted_keys: Arc<DeletedKeys>,
    cache: HybridCache<FoyerKey, FoyerValue>,
    keys: Vec<FoyerKey>,
    evict_keys: Vec<FoyerKey>,
}

impl Drop for DeleteGuard {
    fn drop(&mut self) {
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
        state.apply_invalidation(&self.cache, &self.keys, &self.evict_keys);
    }
}

impl DeletedKeys {
    fn lock_state(&self) -> MutexGuard<'_, DeletedKeysState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn begin_read(
        self: &Arc<Self>,
        cache: &HybridCache<FoyerKey, FoyerValue>,
        key: &FoyerKey,
    ) -> Option<ReadGuard> {
        let mut state = self.lock_state();
        if state.active_deletes.contains_key(key)
            || state.idle.contains(key)
            || state
                .pinned
                .get(key)
                .is_some_and(|entry| entry.kind == PinnedKind::Invalidate)
        {
            return None;
        }

        let epoch = state.epoch;
        *state
            .active_reads
            .entry(key.clone())
            .or_default()
            .entry(epoch)
            .or_insert(0) += 1;

        Some(ReadGuard(Arc::new(ReadGuardInner {
            deleted_keys: self.clone(),
            cache: cache.clone(),
            key: key.clone(),
            epoch,
        })))
    }

    fn finish_read(&self, cache: &HybridCache<FoyerKey, FoyerValue>, key: &FoyerKey, epoch: u64) {
        let mut state = self.lock_state();
        let Some(epochs) = state.active_reads.get_mut(key) else {
            return;
        };
        let removed_epoch = match epochs.get_mut(&epoch) {
            Some(count) if *count > 1 => {
                *count -= 1;
                false
            }
            Some(_) => {
                epochs.remove(&epoch);
                true
            }
            None => false,
        };

        let min_active = epochs.first_key_value().map(|(&e, _)| e);
        if min_active.is_none() {
            state.active_reads.remove(key);
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
            .get(key)
            .is_some_and(|entry| min_active.is_none_or(|min_epoch| min_epoch >= entry.epoch));
        if expired
            && let Some(entry) = state.pinned.remove(key)
            && entry.kind == PinnedKind::Invalidate
        {
            // The last read that could have refilled this entry has finished.
            // Drop whatever it may have inserted before releasing the pin.
            cache.remove(key);
        }
    }

    fn is_fetch_stale(&self, key: &FoyerKey, fetch_epoch: u64) -> bool {
        let state = self.lock_state();
        state.active_deletes.contains_key(key)
            || state.idle.contains(key)
            || state
                .pinned
                .get(key)
                .is_some_and(|entry| entry.epoch > fetch_epoch)
    }

    fn invalidate(
        &self,
        cache: &HybridCache<FoyerKey, FoyerValue>,
        keys: &[FoyerKey],
        evict_keys: &[FoyerKey],
    ) {
        let mut state = self.lock_state();
        state.apply_invalidation(cache, keys, evict_keys);
    }

    pub(crate) fn begin_delete(
        self: &Arc<Self>,
        cache: &HybridCache<FoyerKey, FoyerValue>,
        keys: Vec<FoyerKey>,
        evict_keys: Vec<FoyerKey>,
    ) -> DeleteGuard {
        let mut state = self.lock_state();
        state.apply_invalidation(cache, &keys, &evict_keys);
        for key in keys.iter().chain(evict_keys.iter()) {
            *state.active_deletes.entry(key.clone()).or_insert(0) += 1;
        }
        DeleteGuard {
            deleted_keys: self.clone(),
            cache: cache.clone(),
            keys,
            evict_keys,
        }
    }

    fn store(
        &self,
        cache: &HybridCache<FoyerKey, FoyerValue>,
        key: FoyerKey,
        value: FoyerValue,
        evict_key: Option<&FoyerKey>,
    ) {
        let mut state = self.lock_state();
        state.idle.remove(&key);
        if state.active_reads.contains_key(&key) {
            state.epoch += 1;
            let epoch = state.epoch;
            state.pinned.insert(
                key.clone(),
                PinnedEntry {
                    epoch,
                    kind: PinnedKind::Store,
                },
            );
        } else {
            state.pinned.remove(&key);
        }

        if let Some(evict_key) = evict_key {
            state.idle.remove(evict_key);
            cache.remove(evict_key);
            if state.active_reads.contains_key(evict_key) {
                state.epoch += 1;
                let epoch = state.epoch;
                state.pinned.insert(
                    evict_key.clone(),
                    PinnedEntry {
                        epoch,
                        kind: PinnedKind::Invalidate,
                    },
                );
            } else {
                state.pinned.remove(evict_key);
            }
        }

        cache.insert(key, value);
    }

    fn evict(&self, cache: &HybridCache<FoyerKey, FoyerValue>, key: &FoyerKey) {
        let mut state = self.lock_state();
        state.idle.remove(key);
        cache.remove(key);
        if state.active_reads.contains_key(key) {
            state.epoch += 1;
            let epoch = state.epoch;
            state.pinned.insert(
                key.clone(),
                PinnedEntry {
                    epoch,
                    kind: PinnedKind::Invalidate,
                },
            );
        } else {
            state.pinned.remove(key);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        let state = self.lock_state();
        state.idle.len() + state.pinned.len() + state.active_deletes.len()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.len() == 0
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
///   Writes that end up outside the limit, and append writes whose buffered
///   chunks do not contain the whole object, invalidate the superseded cache
///   entry instead. An aborted write leaves the cache untouched.
/// - `read`: [`FoyerLayer`] checks the cache first. On a cache miss, it reads
///   and caches the entire object if its size is within the configured
///   [`FoyerLayer::with_size_limit`] range, even when only a byte range is requested.
///   Reads with preconditions such as `if_match` or `if_none_match` bypass the
///   cache and read directly from the service, which evaluates the condition.
///   A conditional read never populates the cache.
/// - `delete`: [`FoyerLayer`] removes cached data after a successful delete when the deleter
///   closes. A failed delete call is not invalidated. Exact-key invalidation happens before the
///   underlying deleter is closed, so the data remains invalidated if closing the deleter fails.
///   Recursive deletes also clear the entire cache after the underlying deleter closes, including
///   when closing fails, because Foyer does not support prefix invalidation and buffered deleters
///   can allow concurrent reads to refill deleted objects while closing.
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
#[doc(hidden)]
pub struct FoyerService {
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
        let inner = self.operation_inner(ctx);
        inner.invalidate_unversioned(from.to_string());
        inner.invalidate_unversioned(to.to_string());
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
                stat: true,
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
        let layer = FoyerLayer::new(cache.clone());
        let service = layer.layer(Arc::new(Memory::default().build().unwrap()));
        let ctx = OperationContext::new();
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
            service.operation_inner(&ctx),
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
        let service = layer.layer(Arc::new(Memory::default().build().unwrap()));
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
        let service = Arc::new(layer.layer(source));
        let ctx = OperationContext::new();
        let inner = service.operation_inner(&ctx);

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

        inner.invalidate_unversioned("test".to_string());
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
        assert!(
            layer.deleted_keys.is_empty(),
            "tombstones must be released once the invalidation readers finish"
        );
    }

    #[tokio::test]
    async fn interleaved_invalidation_keeps_older_read_pinned() {
        let cache = memory_cache().await;
        let source = Arc::new(MockReadService::new("stale-data"));
        let state = source.state.clone();
        let gate = state.gate_stat_for("target");
        let layer = FoyerLayer::new(cache.clone());
        let service = Arc::new(layer.layer(source));
        let ctx = OperationContext::new();
        let inner = service.operation_inner(&ctx);

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
        inner.invalidate_unversioned("unrelated".to_string());
        {
            let reader = service.read(&ctx, "other", OpRead::default()).unwrap();
            let (_, mut stream) = reader.open(BytesRange::new(0, None)).await.unwrap();
            let _ = stream.read_all().await.unwrap();
        }

        // Now invalidate "target" while R1 is still in flight.
        inner.invalidate_unversioned("target".to_string());
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
        let service = Arc::new(layer.layer(source));
        let ctx = OperationContext::new();
        let inner = service.operation_inner(&ctx);
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

        inner.invalidate_unversioned("target".to_string());

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
        let service = Arc::new(layer.layer(gated.clone()));
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
