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

use std::fmt::Debug;
use std::sync::Arc;

use foyer::Engine;
use foyer::HybridCache;
use foyer::HybridCacheBuilder;
use foyer::LargeEngineOptions;
use foyer::RecoverMode;

use opendal_core::Buffer;
use opendal_core::Result;

use super::FoyerConfig;
use super::FoyerKey;
use super::FoyerValue;

/// FoyerCore holds the foyer HybridCache instance.
///
/// It supports lazy initialization when constructed from URI.
#[derive(Clone)]
pub struct FoyerCore {
    /// OnceLock for lazy cache initialization.
    cache: std::sync::OnceLock<Arc<HybridCache<FoyerKey, FoyerValue>>>,
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

impl FoyerCore {
    /// Create a new FoyerCore with a pre-built cache.
    ///
    /// This is used when the user provides a `HybridCache` via `Foyer::new(cache)`.
    pub fn new(config: FoyerConfig) -> Self {
        Self {
            cache: std::sync::OnceLock::new(),
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

        let cache = Arc::new(
            HybridCacheBuilder::new()
                .memory(self.config.memory.unwrap_or(FOYER_DEFAULT_MEMORY_BYTES))
                .with_shards(1)
                .storage(Engine::Large(LargeEngineOptions::new()))
                .with_recover_mode(RecoverMode::None)
                .build()
                .await
                .map_err(|e| {
                    opendal_core::Error::new(
                        opendal_core::ErrorKind::Unexpected,
                        "failed to build foyer cache",
                    )
                    .set_source(e)
                })?,
        );

        self.cache.set(cache.clone()).map_err(|_| {
            opendal_core::Error::new(
                opendal_core::ErrorKind::Unexpected,
                "failed to initialize foyer cache",
            )
        })?;

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

        match cache.get(&foyer_key).await {
            Ok(Some(entry)) => Ok(Some(entry.value().0.clone())),
            Ok(None) => Ok(None),
            Err(_) => Ok(None),
        }
    }

    pub async fn insert(&self, key: &str, value: Buffer) -> Result<()> {
        let cache = self.get_cache().await?;
        cache.insert(
            FoyerKey {
                path: key.to_string(),
            },
            FoyerValue(value),
        );
        Ok(())
    }

    pub async fn remove(&self, key: &str) -> Result<()> {
        let cache = self.get_cache().await?;
        cache.remove(&FoyerKey {
            path: key.to_string(),
        });
        Ok(())
    }
}
