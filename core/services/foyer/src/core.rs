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
/// prevents unbounded memory growth across long-running delete workloads.
const MAX_DELETED_KEYS: usize = 1024;

#[derive(Default)]
struct DeletedKeys {
    next_seq: u64,
    keys: HashMap<Arc<str>, u64>,
    by_seq: BTreeMap<u64, Arc<str>>,
}

impl DeletedKeys {
    fn contains(&self, key: &str) -> bool {
        self.keys.contains_key(key)
    }

    fn insert(&mut self, key: &str, evicted: &mut Vec<Arc<str>>) -> usize {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);

        let shared: Arc<str> = if let Some((existing, old_seq)) = self.keys.remove_entry(key) {
            self.by_seq.remove(&old_seq);
            existing
        } else {
            Arc::from(key)
        };
        self.keys.insert(shared.clone(), seq);
        self.by_seq.insert(seq, shared);

        while self.keys.len() > MAX_DELETED_KEYS {
            let Some((_, oldest)) = self.by_seq.pop_first() else {
                break;
            };
            self.keys.remove(&oldest);
            evicted.push(oldest);
        }
        self.keys.len()
    }

    fn remove(&mut self, key: &str) -> usize {
        if let Some(seq) = self.keys.remove(key) {
            self.by_seq.remove(&seq);
        }
        self.keys.len()
    }
}

/// FoyerCore holds the foyer HybridCache instance.
///
/// It supports lazy initialization when constructed from URI.
#[derive(Clone)]
pub struct FoyerCore {
    /// OnceLock for lazy cache initialization.
    cache: std::sync::OnceLock<Arc<HybridCache<FoyerKey, FoyerValue>>>,
    deleted_keys: Arc<RwLock<DeletedKeys>>,
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
        if self.deleted_len.load(Ordering::Acquire) != 0
            && self
                .deleted_keys
                .read()
                .expect("deleted keys lock poisoned")
                .contains(key)
        {
            return Ok(None);
        }

        let foyer_key = FoyerKey {
            path: key.to_string(),
        };
        match cache.get(&foyer_key).await {
            Ok(Some(entry)) => Ok(Some(entry.value().0.clone())),
            Ok(None) => Ok(None),
            Err(_) => Ok(None),
        }
    }

    pub async fn insert(&self, key: &str, value: Buffer) -> Result<()> {
        let cache = self.get_cache().await?;
        let foyer_key = FoyerKey {
            path: key.to_string(),
        };
        cache.insert(foyer_key, FoyerValue(value));
        if self.deleted_len.load(Ordering::Acquire) != 0 {
            let mut guard = self
                .deleted_keys
                .write()
                .expect("deleted keys lock poisoned");
            let len = guard.remove(key);
            self.deleted_len.store(len, Ordering::Release);
        }
        Ok(())
    }

    pub async fn remove(&self, key: &str) -> Result<()> {
        let cache = self.get_cache().await?;
        let mut evicted = Vec::new();
        {
            let mut guard = self
                .deleted_keys
                .write()
                .expect("deleted keys lock poisoned");
            let len = guard.insert(key, &mut evicted);
            self.deleted_len.store(len, Ordering::Release);
        }
        cache.remove(&FoyerKey {
            path: key.to_string(),
        });
        for oldest in evicted {
            cache.remove(&FoyerKey {
                path: oldest.to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deleted_keys_is_bounded_and_evicts_oldest() {
        let mut tombstones = DeletedKeys::default();
        let mut evicted = Vec::new();

        for i in 0..(MAX_DELETED_KEYS + 32) {
            let len = tombstones.insert(&format!("k-{i}"), &mut evicted);
            assert!(len <= MAX_DELETED_KEYS);
        }

        assert_eq!(tombstones.keys.len(), MAX_DELETED_KEYS);
        assert_eq!(tombstones.by_seq.len(), MAX_DELETED_KEYS);
        assert_eq!(evicted.len(), 32);
        assert_eq!(evicted[0].as_ref(), "k-0");
        assert_eq!(evicted[31].as_ref(), "k-31");
        assert!(!tombstones.contains("k-0"));
        assert!(tombstones.contains(&format!("k-{}", MAX_DELETED_KEYS + 31)));

        // Re-inserting an existing key refreshes its recency without growing size.
        let refresh_key = "k-32";
        evicted.clear();
        assert_eq!(
            tombstones.insert(refresh_key, &mut evicted),
            MAX_DELETED_KEYS
        );
        assert!(evicted.is_empty());
        assert_eq!(tombstones.insert("k-extra", &mut evicted), MAX_DELETED_KEYS);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].as_ref(), "k-33");
        assert!(tombstones.contains(refresh_key));

        assert_eq!(tombstones.remove(refresh_key), MAX_DELETED_KEYS - 1);
        assert!(!tombstones.contains(refresh_key));
    }

    #[tokio::test]
    async fn core_bounded_deletes_and_reinsert_visibility() {
        let core = FoyerCore::new(FoyerConfig {
            memory: Some(4 * 1024 * 1024),
            shards: Some(4),
            ..Default::default()
        });

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
    }
}
