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
use std::fmt::Formatter;
use std::future;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use log::debug;
use moka::future::Cache;
use moka::future::CacheBuilder;
use moka::future::FutureExt;
use moka::notification::ListenerFuture;
use moka::notification::RemovalCause;
pub use moka::policy::EvictionPolicy;
use moka::policy::Expiry;

use crate::raw::adapters::typed_kv;
use crate::raw::*;
use crate::services::MokaConfig;
use crate::*;

impl Configurator for MokaConfig {
    type Builder = MokaBuilder;
    fn into_builder(self) -> Self::Builder {
        MokaBuilder {
            config: self,
            ..Default::default()
        }
    }
}

type Weigher<K, V> = Box<dyn Fn(&K, &V) -> u32 + Send + Sync + 'static>;

type AsyncEvictionListener<K, V> =
    Box<dyn Fn(Arc<K>, V, RemovalCause) -> ListenerFuture + Send + Sync + 'static>;

/// [moka](https://github.com/moka-rs/moka) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct MokaBuilder {
    config: MokaConfig,
    expiry: Option<ExpiryWrapper<String, typed_kv::Value>>,
    weigher: Option<Weigher<String, typed_kv::Value>>,
    eviction_listener: Option<AsyncEvictionListener<String, typed_kv::Value>>,
    eviction_policy: Option<EvictionPolicy>,
}

impl Debug for MokaBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MokaBuilder")
            .field("config", &self.config)
            .finish()
    }
}

impl MokaBuilder {
    /// Sets the name of the cache.
    ///
    /// Refer to [`moka::future::CacheBuilder::name`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.name)
    pub fn name(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.name = Some(v.to_owned());
        }
        self
    }

    /// Sets the initial capacity (number of entries) of the cache.
    ///
    /// Refer to [`moka::future::CacheBuilder::max_capacity`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.initial_capacity)
    pub fn initial_capacity(mut self, v: usize) -> Self {
        if v != 0 {
            self.config.initial_capacity = Some(v);
        }
        self
    }

    /// Sets the max capacity of the cache.
    ///
    /// Refer to [`moka::future::CacheBuilder::max_capacity`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.max_capacity)
    pub fn max_capacity(mut self, v: u64) -> Self {
        if v != 0 {
            self.config.max_capacity = Some(v);
        }
        self
    }

    /// Sets the time to live of the cache.
    ///
    /// Refer to [`moka::future::CacheBuilder::time_to_live`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.time_to_live)
    pub fn time_to_live(mut self, v: Duration) -> Self {
        if !v.is_zero() {
            self.config.time_to_live = Some(v);
        }
        self
    }

    /// Sets the time to idle of the cache.
    ///
    /// Refer to [`moka::future::CacheBuilder::time_to_idle`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.time_to_idle)
    pub fn time_to_idle(mut self, v: Duration) -> Self {
        if !v.is_zero() {
            self.config.time_to_idle = Some(v);
        }
        self
    }

    /// Sets the given expiry to the cache.
    ///
    /// Refer to [`moka::future::CacheBuilder::expire_after`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.expire_after)
    pub fn expire_after(
        self,
        expiry: impl Expiry<String, typed_kv::Value> + Send + Sync + 'static,
    ) -> Self {
        Self {
            expiry: Some(ExpiryWrapper {
                inner: Box::new(expiry),
            }),
            ..self
        }
    }

    /// Sets the weigher closure to the cache.
    ///
    /// Refer to [`moka::future::CacheBuilder::weigher`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.weigher)
    pub fn weigher(
        self,
        weigher: impl Fn(&String, &typed_kv::Value) -> u32 + Send + Sync + 'static,
    ) -> Self {
        Self {
            weigher: Some(Box::new(weigher)),
            ..self
        }
    }

    /// Sets the eviction listener closure to the cache.
    ///
    /// Refer to [`moka::future::CacheBuilder::eviction_listener`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.eviction_listener)
    pub fn eviction_listener<F>(self, listener: F) -> Self
    where
        F: Fn(Arc<String>, typed_kv::Value, RemovalCause) + Send + Sync + 'static,
    {
        let async_listener = move |k, v, c| {
            {
                listener(k, v, c);
                future::ready(())
            }
            .boxed()
        };

        self.async_eviction_listener(async_listener)
    }

    /// Sets the eviction listener closure to the cache.
    ///
    /// Refer to [`moka::future::CacheBuilder::async_eviction_listener`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.async_eviction_listener)
    pub fn async_eviction_listener<F>(self, listener: F) -> Self
    where
        F: Fn(Arc<String>, typed_kv::Value, RemovalCause) -> ListenerFuture + Send + Sync + 'static,
    {
        Self {
            eviction_listener: Some(Box::new(listener)),
            ..self
        }
    }

    /// Sets the eviction (and admission) policy of the cache.
    ///
    /// Refer to [`moka::future::CacheBuilder::eviction_policy`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.eviction_policy)
    pub fn eviction_policy(self, policy: EvictionPolicy) -> Self {
        Self {
            eviction_policy: Some(policy),
            ..self
        }
    }

    /// Set the root path of this backend
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = if path.is_empty() {
            None
        } else {
            Some(path.to_string())
        };
        self
    }
}

impl Builder for MokaBuilder {
    const SCHEME: Scheme = Scheme::Moka;
    type Config = MokaConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let mut builder: CacheBuilder<String, typed_kv::Value, _> = Cache::builder();

        if let Some(v) = &self.config.name {
            builder = builder.name(v);
        }
        if let Some(v) = self.config.initial_capacity {
            builder = builder.initial_capacity(v);
        }
        if let Some(v) = self.config.max_capacity {
            builder = builder.max_capacity(v);
        }
        if let Some(v) = self.config.time_to_live {
            builder = builder.time_to_live(v);
        }
        if let Some(v) = self.config.time_to_idle {
            builder = builder.time_to_idle(v);
        }
        if let Some(expiry) = self.expiry {
            builder = builder.expire_after(expiry);
        }
        if let Some(weigher) = self.weigher {
            builder = builder.weigher(move |k, v| weigher(k, v));
        }
        if let Some(v) = self.eviction_listener {
            builder = builder.async_eviction_listener(v);
        }
        if let Some(v) = self.eviction_policy {
            builder = builder.eviction_policy(v);
        }

        debug!("backend build finished: {:?}", self.config);

        let mut backend = MokaBackend::new(Adapter {
            inner: builder.build(),
        });
        if let Some(v) = self.config.root {
            backend = backend.with_root(&v);
        }

        Ok(backend)
    }
}

/// Backend is used to serve `Accessor` support in moka.
pub type MokaBackend = typed_kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    inner: Cache<String, typed_kv::Value>,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Adapter")
            .field("size", &self.inner.weighted_size())
            .field("count", &self.inner.entry_count())
            .finish()
    }
}

impl typed_kv::Adapter for Adapter {
    fn info(&self) -> typed_kv::Info {
        typed_kv::Info::new(
            Scheme::Moka,
            self.inner.name().unwrap_or("moka"),
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
        match self.inner.get(path).await {
            None => Ok(None),
            Some(bs) => Ok(Some(bs)),
        }
    }

    async fn set(&self, path: &str, value: typed_kv::Value) -> Result<()> {
        self.inner.insert(path.to_string(), value).await;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.inner.invalidate(path).await;
        Ok(())
    }

    async fn scan(&self, path: &str) -> Result<Vec<String>> {
        let keys = self.inner.iter().map(|kv| kv.0.to_string());
        if path.is_empty() {
            Ok(keys.collect())
        } else {
            Ok(keys.filter(|k| k.starts_with(path)).collect())
        }
    }
}

// Remove the struct once upstream releases a version containing the following implementation:
// `impl Expiry<K, V> for Box<dyn Expiry<K, V> + Send + Sync> {...}`
struct ExpiryWrapper<K, V> {
    inner: Box<dyn Expiry<K, V> + Send + Sync + 'static>,
}

impl<K, V> Expiry<K, V> for ExpiryWrapper<K, V> {
    fn expire_after_create(&self, key: &K, value: &V, created_at: Instant) -> Option<Duration> {
        self.inner.expire_after_create(key, value, created_at)
    }

    fn expire_after_read(
        &self,
        key: &K,
        value: &V,
        read_at: Instant,
        duration_until_expiry: Option<Duration>,
        last_modified_at: Instant,
    ) -> Option<Duration> {
        self.inner
            .expire_after_read(key, value, read_at, duration_until_expiry, last_modified_at)
    }

    fn expire_after_update(
        &self,
        key: &K,
        value: &V,
        updated_at: Instant,
        duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        self.inner
            .expire_after_update(key, value, updated_at, duration_until_expiry)
    }
}
