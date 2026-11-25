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

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Mutex;

use opendal_core::raw::Timestamp;
use opendal_core::{ErrorKind, Metadata, Operator};

use crate::flight::Flights;
use crate::key::{Identity, Subject, meta_key};
use crate::reader::{
    DiscoveryKey, DiscoveryResult, FillKey, FillResult, RevalidationKey, RevalidationResult,
    Snapshot,
};
use crate::record::Pointer;

const GENERATION_KEY: &str = "v1/gen";
pub(crate) const GENERATION_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const NEVER_CHECKED: i64 = i64::MIN;
const SHARDS: usize = 256;

struct ColdRead {
    id: u64,
    subject: Subject,
    observed_at: Timestamp,
    /// Latest stat observation for this exact subject recorded after `observed_at`.
    /// `None` means no newer stat ran; `Some(None)` means `NotFound`.
    latest_stat: Option<Option<Metadata>>,
}

struct Shard {
    epoch: AtomicU64,
    meta_io: Mutex<()>,
    cold_reads: std::sync::Mutex<Vec<ColdRead>>,
    cold_read_count: AtomicUsize,
}

impl Shard {
    fn new() -> Self {
        Self {
            epoch: AtomicU64::new(0),
            meta_io: Mutex::new(()),
            cold_reads: std::sync::Mutex::new(Vec::new()),
            cold_read_count: AtomicUsize::new(0),
        }
    }
}

pub(crate) struct ColdReadGuard {
    state: Arc<State>,
    subject: Subject,
    id: u64,
    observed_at: Timestamp,
}

impl ColdReadGuard {
    pub(crate) fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    fn contradicts(&self, pointer: &Pointer) -> bool {
        let shard = self.state.shard(&self.subject);
        let reads = shard.cold_reads.lock().unwrap();
        let Some(entry) = reads.iter().find(|entry| entry.id == self.id) else {
            return false;
        };
        let Some(stat_meta) = &entry.latest_stat else {
            return false;
        };
        !metadata_matches_pointer(stat_meta.as_ref(), pointer)
    }
}

impl Drop for ColdReadGuard {
    fn drop(&mut self) {
        let shard = self.state.shard(&self.subject);
        let mut reads = shard.cold_reads.lock().unwrap();
        if let Some(pos) = reads.iter().position(|entry| entry.id == self.id) {
            reads.swap_remove(pos);
            shard.cold_read_count.fetch_sub(1, Ordering::Release);
        }
    }
}

pub(crate) struct State {
    pub(crate) cache: Operator,
    pub(crate) discoveries: Flights<DiscoveryKey, DiscoveryResult>,
    pub(crate) revalidations: Flights<RevalidationKey, RevalidationResult>,
    pub(crate) fills: Flights<FillKey, FillResult>,
    shards: Box<[Shard; SHARDS]>,
    next_cold_id: AtomicU64,
    generation: AtomicU64,
    generation_checked_ms: AtomicI64,
    generation_update: Mutex<()>,
}

fn now_ms() -> i64 {
    Timestamp::now().into_inner().as_millisecond()
}

fn metadata_matches_pointer(metadata: Option<&Metadata>, pointer: &Pointer) -> bool {
    let Some(metadata) = metadata else {
        return false;
    };
    match Identity::verifiable(metadata) {
        Some(identity) => {
            identity == pointer.identity
                && metadata.content_length() == pointer.metadata.content_length()
        }
        None => metadata == &pointer.metadata,
    }
}

impl State {
    pub(crate) fn new(cache: Operator) -> Self {
        Self {
            cache,
            discoveries: Flights::default(),
            revalidations: Flights::default(),
            fills: Flights::default(),
            shards: Box::new(std::array::from_fn(|_| Shard::new())),
            next_cold_id: AtomicU64::new(1),
            generation: AtomicU64::new(0),
            generation_checked_ms: AtomicI64::new(NEVER_CHECKED),
            generation_update: Mutex::new(()),
        }
    }

    pub(crate) async fn generation(&self, max_age: Duration) -> Option<u64> {
        let refresh_window = max_age.min(GENERATION_REFRESH_INTERVAL);
        if !refresh_window.is_zero() {
            let checked_ms = self.generation_checked_ms.load(Ordering::Acquire);
            if checked_ms != NEVER_CHECKED {
                let now = now_ms();
                if now >= checked_ms
                    && u128::from((now - checked_ms) as u64) < refresh_window.as_millis()
                {
                    return Some(self.generation.load(Ordering::Acquire));
                }
            }
        }
        self.refresh_generation().await
    }

    pub(crate) async fn refresh_generation(&self) -> Option<u64> {
        let observed = match self.cache.read(GENERATION_KEY).await {
            Ok(value) if value.len() <= 16 => {
                let bytes = value.to_vec();
                if !bytes
                    .iter()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
                {
                    return None;
                }
                u64::from_str_radix(std::str::from_utf8(&bytes).ok()?, 16).ok()?
            }
            Err(err) if err.kind() == ErrorKind::NotFound => 0,
            _ => return None,
        };
        let current = self
            .generation
            .fetch_max(observed, Ordering::AcqRel)
            .max(observed);
        self.generation_checked_ms
            .store(now_ms(), Ordering::Release);
        Some(current)
    }

    fn shard(&self, subject: &Subject) -> &Shard {
        &self.shards[usize::from(subject[0]) % SHARDS]
    }

    pub(crate) fn epoch(&self, subject: &Subject) -> u64 {
        self.shard(subject).epoch.load(Ordering::Acquire)
    }

    pub(crate) fn begin_mutation(&self, subjects: &[Subject]) {
        for subject in subjects {
            self.shard(subject).epoch.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Register an in-flight cold read before issuing the origin request.
    ///
    /// Captures `observed_at` and increments `cold_read_count` (Release) before
    /// the origin read begins, while `ColdReadGuard::drop` decrements it
    /// (Release) only after `publish` finishes. Together with `observe_stat`
    /// checking `cold_read_count` (Acquire) before reading the cached pointer,
    /// this lets `observe_stat` skip locking when no cold read is active.
    pub(crate) fn register_cold_read(self: &Arc<Self>, subject: Subject) -> ColdReadGuard {
        let id = self.next_cold_id.fetch_add(1, Ordering::Relaxed);
        let shard = self.shard(&subject);
        let observed_at = {
            let mut reads = shard.cold_reads.lock().unwrap();
            let observed_at = Timestamp::now();
            reads.push(ColdRead {
                id,
                subject,
                observed_at,
                latest_stat: None,
            });
            shard.cold_read_count.fetch_add(1, Ordering::Release);
            observed_at
        };
        ColdReadGuard {
            state: self.clone(),
            subject,
            id,
            observed_at,
        }
    }

    pub(crate) async fn invalidate(&self, subjects: &[Subject]) {
        let generation = self
            .refresh_generation()
            .await
            .unwrap_or_else(|| self.generation.load(Ordering::Acquire));
        for subject in subjects {
            let shard = self.shard(subject);
            let _io = shard.meta_io.lock().await;
            shard.epoch.fetch_add(1, Ordering::AcqRel);
            let _ = self.cache.delete(&meta_key(generation, subject)).await;
        }
    }

    pub(crate) async fn bump_generation(&self) {
        let _update = self.generation_update.lock().await;
        let _ = self.refresh_generation().await;
        let generation = self
            .generation
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        let _ = self
            .cache
            .write(GENERATION_KEY, format!("{generation:x}"))
            .await;
        self.generation_checked_ms
            .store(now_ms(), Ordering::Release);
    }

    pub(crate) async fn pointer(&self, generation: u64, subject: &Subject) -> Option<Pointer> {
        Pointer::decode(self.cache.read(&meta_key(generation, subject)).await.ok()?)
    }

    pub(crate) async fn publish(
        &self,
        snapshot: Snapshot,
        subject: &Subject,
        pointer: &Pointer,
        cold: Option<&ColdReadGuard>,
    ) -> bool {
        let shard = self.shard(subject);
        if shard.epoch.load(Ordering::Acquire) != snapshot.epoch {
            return false;
        }
        let _io = shard.meta_io.lock().await;
        if shard.epoch.load(Ordering::Acquire) != snapshot.epoch {
            return false;
        }
        if cold.is_some_and(|guard| guard.contradicts(pointer)) {
            shard.epoch.fetch_add(1, Ordering::AcqRel);
            return false;
        }
        // A slow read may finish after a newer source observation was cached.
        if self
            .pointer(snapshot.generation, subject)
            .await
            .is_some_and(|cached| cached.observed_at > pointer.observed_at)
        {
            return false;
        }
        self.cache
            .write(&meta_key(snapshot.generation, subject), pointer.encode())
            .await
            .is_ok()
    }

    pub(crate) async fn observe_stat(
        &self,
        subject: &Subject,
        snapshot_epoch: u64,
        observed_at: Timestamp,
        metadata: Option<&Metadata>,
    ) {
        let Some(generation) = self.generation(GENERATION_REFRESH_INTERVAL).await else {
            return;
        };
        let shard = self.shard(subject);
        if shard.epoch.load(Ordering::Acquire) != snapshot_epoch {
            return;
        }
        // Check for in-flight cold reads BEFORE probing the cached pointer so a
        // cold read cannot publish and drop its guard between a `None` pointer
        // lookup and a `cold_read_count == 0` fast-path check.
        // Fast path: when `cold_read_count == 0`, no cold read that started
        // before `inner.stat()` completed is still unregistered or un-published
        // (assuming wall-clock timestamps do not jump backwards).
        let has_matching_cold = shard.cold_read_count.load(Ordering::Acquire) > 0 && {
            let reads = shard.cold_reads.lock().unwrap();
            reads
                .iter()
                .any(|entry| entry.subject == *subject && entry.observed_at <= observed_at)
        };
        if let Some(pointer) = self.pointer(generation, subject).await {
            if pointer.observed_at > observed_at || metadata_matches_pointer(metadata, &pointer) {
                return;
            }
            let _io = shard.meta_io.lock().await;
            if shard.epoch.load(Ordering::Acquire) != snapshot_epoch {
                return;
            }
            if self
                .pointer(generation, subject)
                .await
                .is_some_and(|current| {
                    current.observed_at > observed_at
                        || metadata_matches_pointer(metadata, &current)
                })
            {
                return;
            }
            let _ = self.cache.delete(&meta_key(generation, subject)).await;
            shard.epoch.fetch_add(1, Ordering::AcqRel);
            return;
        }

        if !has_matching_cold {
            return;
        }

        // Serialize against a concurrent cold-read `publish` for the same subject:
        // either we observe the newly published pointer and delete it if stale,
        // or we record this stat on the cold-read entry before `publish` checks it.
        let _io = shard.meta_io.lock().await;
        if shard.epoch.load(Ordering::Acquire) != snapshot_epoch {
            return;
        }
        if let Some(pointer) = self.pointer(generation, subject).await {
            if pointer.observed_at > observed_at || metadata_matches_pointer(metadata, &pointer) {
                return;
            }
            let _ = self.cache.delete(&meta_key(generation, subject)).await;
            shard.epoch.fetch_add(1, Ordering::AcqRel);
            return;
        }
        let mut reads = shard.cold_reads.lock().unwrap();
        for entry in reads.iter_mut() {
            if entry.subject == *subject && entry.observed_at <= observed_at {
                entry.latest_stat = Some(metadata.cloned());
            }
        }
    }

    pub(crate) async fn discard(&self, snapshot: Snapshot, subject: &Subject, pointer: &Pointer) {
        let shard = self.shard(subject);
        if shard.epoch.load(Ordering::Acquire) != snapshot.epoch {
            return;
        }
        let _io = shard.meta_io.lock().await;
        if shard.epoch.load(Ordering::Acquire) == snapshot.epoch
            && self
                .pointer(snapshot.generation, subject)
                .await
                .is_some_and(|cached| {
                    cached.identity == pointer.identity && cached.observed_at <= pointer.observed_at
                })
        {
            let _ = self
                .cache
                .delete(&meta_key(snapshot.generation, subject))
                .await;
        }
    }
}
