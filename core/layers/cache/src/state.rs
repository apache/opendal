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

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Mutex;

use opendal_core::raw::Timestamp;
use opendal_core::{ErrorKind, Metadata, Operator};

use crate::flight::Flights;
use crate::key::{Identity, Subject, meta_key};
use crate::reader::{DiscoveryKey, DiscoveryResult, FillKey, FillResult, Snapshot};
use crate::record::Pointer;

const GENERATION_KEY: &str = "v1/gen";
const SHARDS: usize = 64;

pub(crate) struct State {
    pub(crate) cache: Operator,
    pub(crate) discoveries: Flights<DiscoveryKey, DiscoveryResult>,
    pub(crate) fills: Flights<FillKey, FillResult>,
    epochs: [Mutex<u64>; SHARDS],
    generation: AtomicU64,
    generation_update: Mutex<()>,
}

impl State {
    pub(crate) fn new(cache: Operator) -> Self {
        Self {
            cache,
            discoveries: Flights::default(),
            fills: Flights::default(),
            epochs: std::array::from_fn(|_| Mutex::new(0)),
            generation: AtomicU64::new(0),
            generation_update: Mutex::new(()),
        }
    }

    pub(crate) async fn generation(&self) -> Option<u64> {
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
        Some(
            self.generation
                .fetch_max(observed, Ordering::Relaxed)
                .max(observed),
        )
    }

    fn shard(&self, subject: &Subject) -> &Mutex<u64> {
        &self.epochs[usize::from(subject[0]) % SHARDS]
    }

    pub(crate) async fn epoch(&self, subject: &Subject) -> u64 {
        *self.shard(subject).lock().await
    }

    pub(crate) async fn begin_mutation(&self, subjects: &[Subject]) {
        for subject in subjects {
            let mut epoch = self.shard(subject).lock().await;
            *epoch = epoch.wrapping_add(1);
        }
    }

    pub(crate) async fn invalidate(&self, subjects: &[Subject]) {
        let generation = self
            .generation()
            .await
            .unwrap_or_else(|| self.generation.load(Ordering::Relaxed));
        for subject in subjects {
            let mut epoch = self.shard(subject).lock().await;
            *epoch = epoch.wrapping_add(1);
            let _ = self.cache.delete(&meta_key(generation, subject)).await;
        }
    }

    pub(crate) async fn bump_generation(&self) {
        let _update = self.generation_update.lock().await;
        let _ = self.generation().await;
        let generation = self
            .generation
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        let _ = self
            .cache
            .write(GENERATION_KEY, format!("{generation:x}"))
            .await;
    }

    pub(crate) async fn pointer(&self, generation: u64, subject: &Subject) -> Option<Pointer> {
        Pointer::decode(self.cache.read(&meta_key(generation, subject)).await.ok()?)
    }

    pub(crate) async fn publish(&self, snapshot: Snapshot, subject: &Subject, pointer: &Pointer) {
        let epoch = self.shard(subject).lock().await;
        if *epoch != snapshot.epoch {
            return;
        }
        // A slow read may finish after a newer source observation was cached.
        if self
            .pointer(snapshot.generation, subject)
            .await
            .is_some_and(|cached| cached.observed_at > pointer.observed_at)
        {
            return;
        }
        let _ = self
            .cache
            .write(&meta_key(snapshot.generation, subject), pointer.encode())
            .await;
    }

    pub(crate) async fn observe_stat(
        &self,
        subject: &Subject,
        snapshot_epoch: u64,
        observed_at: Timestamp,
        metadata: Option<&Metadata>,
    ) {
        let Some(generation) = self.generation().await else {
            return;
        };
        let mut epoch = self.shard(subject).lock().await;
        if *epoch != snapshot_epoch {
            return;
        }
        if let Some(pointer) = self.pointer(generation, subject).await {
            if pointer.observed_at > observed_at {
                return;
            }
            if let Some(metadata) = metadata {
                let matches = match Identity::verifiable(metadata) {
                    Some(identity) => {
                        identity == pointer.identity
                            && metadata.content_length() == pointer.metadata.content_length()
                    }
                    None => metadata == &pointer.metadata,
                };
                if matches {
                    return;
                }
            }
            let _ = self.cache.delete(&meta_key(generation, subject)).await;
        }
        // A cold read might be preparing an older pointer even when the cache
        // has no pointer yet.
        *epoch = epoch.wrapping_add(1);
    }

    pub(crate) async fn discard(&self, snapshot: Snapshot, subject: &Subject, pointer: &Pointer) {
        let epoch = self.shard(subject).lock().await;
        if *epoch == snapshot.epoch
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
