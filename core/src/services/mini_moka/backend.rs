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
use std::time::Duration;

use log::debug;
use mini_moka::sync::Cache;
use mini_moka::sync::CacheBuilder;

use crate::raw::adapters::typed_kv;
use crate::raw::Access;
use crate::services::MiniMokaConfig;
use crate::*;

impl Configurator for MiniMokaConfig {
    type Builder = MiniMokaBuilder;
    fn into_builder(self) -> Self::Builder {
        MiniMokaBuilder { config: self }
    }
}

/// [mini-moka](https://github.com/moka-rs/mini-moka) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct MiniMokaBuilder {
    config: MiniMokaConfig,
}

impl MiniMokaBuilder {
    /// Sets the max capacity of the cache.
    ///
    /// Refer to [`mini-moka::sync::CacheBuilder::max_capacity`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.max_capacity)
    pub fn max_capacity(mut self, v: u64) -> Self {
        if v != 0 {
            self.config.max_capacity = Some(v);
        }
        self
    }

    /// Sets the time to live of the cache.
    ///
    /// Refer to [`mini-moka::sync::CacheBuilder::time_to_live`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.time_to_live)
    pub fn time_to_live(mut self, v: Duration) -> Self {
        if !v.is_zero() {
            self.config.time_to_live = Some(v);
        }
        self
    }

    /// Sets the time to idle of the cache.
    ///
    /// Refer to [`mini-moka::sync::CacheBuilder::time_to_idle`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.time_to_idle)
    pub fn time_to_idle(mut self, v: Duration) -> Self {
        if !v.is_zero() {
            self.config.time_to_idle = Some(v);
        }
        self
    }

    /// Set root path of this backend
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = if path.is_empty() {
            None
        } else {
            Some(path.to_string())
        };

        self
    }
}

impl Builder for MiniMokaBuilder {
    const SCHEME: Scheme = Scheme::MiniMoka;
    type Config = MiniMokaConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let mut builder: CacheBuilder<String, typed_kv::Value, _> = Cache::builder();
        // Use entries' bytes as capacity weigher.
        builder = builder.weigher(|k, v| (k.len() + v.size()) as u32);
        if let Some(v) = self.config.max_capacity {
            builder = builder.max_capacity(v)
        }
        if let Some(v) = self.config.time_to_live {
            builder = builder.time_to_live(v)
        }
        if let Some(v) = self.config.time_to_idle {
            builder = builder.time_to_idle(v)
        }

        debug!("backend build finished: {:?}", &self);
        let mut backend = MiniMokaBackend::new(Adapter {
            inner: builder.build(),
        });
        if let Some(v) = self.config.root {
            backend = backend.with_root(&v);
        }

        Ok(backend)
    }
}

/// Backend is used to serve `Accessor` support in mini-moka.
pub type MiniMokaBackend = typed_kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    inner: Cache<String, typed_kv::Value>,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Adapter")
            .field("size", &self.inner.weighted_size())
            .field("count", &self.inner.entry_count())
            .finish()
    }
}

impl typed_kv::Adapter for Adapter {
    fn info(&self) -> typed_kv::Info {
        typed_kv::Info::new(
            Scheme::MiniMoka,
            "mini-moka",
            typed_kv::Capability {
                get: true,
                set: true,
                delete: true,
                scan: true,
                shared: false,
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<typed_kv::Value>> {
        self.blocking_get(path)
    }

    fn blocking_get(&self, path: &str) -> Result<Option<typed_kv::Value>> {
        match self.inner.get(&path.to_string()) {
            None => Ok(None),
            Some(bs) => Ok(Some(bs)),
        }
    }

    async fn set(&self, path: &str, value: typed_kv::Value) -> Result<()> {
        self.blocking_set(path, value)
    }

    fn blocking_set(&self, path: &str, value: typed_kv::Value) -> Result<()> {
        self.inner.insert(path.to_string(), value);

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.blocking_delete(path)
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        self.inner.invalidate(&path.to_string());

        Ok(())
    }

    async fn scan(&self, path: &str) -> Result<Vec<String>> {
        self.blocking_scan(path)
    }

    fn blocking_scan(&self, path: &str) -> Result<Vec<String>> {
        let keys = self.inner.iter().map(|kv| kv.key().to_string());
        if path.is_empty() {
            Ok(keys.collect())
        } else {
            Ok(keys.filter(|k| k.starts_with(path)).collect())
        }
    }
}
