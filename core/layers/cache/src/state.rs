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
use opendal_core::{Metadata, Operator};

use crate::flight::Flights;
use crate::key::{Identity, Subject, meta_key};
use crate::reader::{
    DiscoveryKey, DiscoveryResult, FillKey, FillResult, RevalidationKey, RevalidationResult,
};
use crate::record::{MAX_RECORD_LEN, Pointer};

const SHARDS: usize = 256;

struct Shard {
    epoch: AtomicU64,
    meta_io: Mutex<()>,
}

impl Shard {
    fn new() -> Self {
        Self {
            epoch: AtomicU64::new(0),
            meta_io: Mutex::new(()),
        }
    }
}

pub(crate) struct State {
    pub(crate) cache: Operator,
    pub(crate) discoveries: Flights<DiscoveryKey, DiscoveryResult>,
    pub(crate) revalidations: Flights<RevalidationKey, RevalidationResult>,
    pub(crate) fills: Flights<FillKey, FillResult>,
    shards: Box<[Shard; SHARDS]>,
}

fn metadata_matches_pointer(metadata: Option<&Metadata>, pointer: &Pointer) -> bool {
    let Some(metadata) = metadata else {
        return false;
    };
    metadata.mode() == pointer.metadata.mode()
        && metadata.content_length() == pointer.metadata.content_length()
        && Identity::from_metadata(metadata) == Some(pointer.identity)
}

impl State {
    pub(crate) fn new(cache: Operator) -> Self {
        Self {
            cache,
            discoveries: Flights::default(),
            revalidations: Flights::default(),
            fills: Flights::default(),
            shards: Box::new(std::array::from_fn(|_| Shard::new())),
        }
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

    pub(crate) async fn invalidate(&self, subjects: &[Subject]) {
        for subject in subjects {
            let shard = self.shard(subject);
            let _io = shard.meta_io.lock().await;
            shard.epoch.fetch_add(1, Ordering::AcqRel);
            let _ = self.cache.delete(&meta_key(subject)).await;
        }
    }

    pub(crate) async fn pointer(&self, subject: &Subject) -> Option<Pointer> {
        let data = self
            .cache
            .read_with(&meta_key(subject))
            .limit((MAX_RECORD_LEN + 1) as u64)
            .await
            .ok()?;
        Pointer::decode(data)
    }

    pub(crate) async fn publish(&self, epoch: u64, subject: &Subject, pointer: &Pointer) -> bool {
        let shard = self.shard(subject);
        if shard.epoch.load(Ordering::Acquire) != epoch {
            return false;
        }
        let _io = shard.meta_io.lock().await;
        if shard.epoch.load(Ordering::Acquire) != epoch {
            return false;
        }
        // A slow read may finish after a newer source observation was cached.
        if self
            .pointer(subject)
            .await
            .is_some_and(|cached| cached.observed_at > pointer.observed_at)
        {
            return false;
        }
        self.cache
            .write(&meta_key(subject), pointer.encode())
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
        let shard = self.shard(subject);
        if shard.epoch.load(Ordering::Acquire) != snapshot_epoch {
            return;
        }
        let Some(pointer) = self.pointer(subject).await else {
            return;
        };
        if pointer.observed_at > observed_at || metadata_matches_pointer(metadata, &pointer) {
            return;
        }
        let _io = shard.meta_io.lock().await;
        if shard.epoch.load(Ordering::Acquire) != snapshot_epoch {
            return;
        }
        if self.pointer(subject).await.is_some_and(|current| {
            current.observed_at > observed_at || metadata_matches_pointer(metadata, &current)
        }) {
            return;
        }
        let _ = self.cache.delete(&meta_key(subject)).await;
        shard.epoch.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) async fn discard(&self, epoch: u64, subject: &Subject, pointer: &Pointer) {
        let shard = self.shard(subject);
        if shard.epoch.load(Ordering::Acquire) != epoch {
            return;
        }
        let _io = shard.meta_io.lock().await;
        if shard.epoch.load(Ordering::Acquire) == epoch
            && self.pointer(subject).await.is_some_and(|cached| {
                cached.identity == pointer.identity && cached.observed_at <= pointer.observed_at
            })
        {
            let _ = self.cache.delete(&meta_key(subject)).await;
            shard.epoch.fetch_add(1, Ordering::AcqRel);
        }
    }
}
