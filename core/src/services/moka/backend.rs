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

use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use log::debug;
use moka::sync::CacheBuilder;
use moka::sync::SegmentedCache;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;

/// [moka](https://github.com/moka-rs/moka) backend support.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [ ] ~~list~~
/// - [ ] ~~scan~~
/// - [ ] ~~presign~~
/// - [x] blocking
#[derive(Default, Debug)]
pub struct MokaBuilder {
    /// Name for this cache instance.
    name: Option<String>,
    /// Sets the max capacity of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::max_capacity`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.max_capacity)
    max_capacity: Option<u64>,
    /// Sets the time to live of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::time_to_live`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.time_to_live)
    time_to_live: Option<Duration>,
    /// Sets the time to idle of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::time_to_idle`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.time_to_idle)
    time_to_idle: Option<Duration>,
    /// Sets the segments number of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::segments`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.segments)
    num_segments: Option<usize>,
    /// Decides whether to enable thread pool of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::thread_pool_enabled`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.thread_pool_enabled)
    thread_pool_enabled: Option<bool>,
}

impl MokaBuilder {
    /// Name for this cache instance.
    pub fn name(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.name = Some(v.to_owned());
        }
        self
    }

    /// Sets the max capacity of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::max_capacity`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.max_capacity)
    pub fn max_capacity(&mut self, v: u64) -> &mut Self {
        if v != 0 {
            self.max_capacity = Some(v);
        }
        self
    }

    /// Sets the time to live of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::time_to_live`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.time_to_live)
    pub fn time_to_live(&mut self, v: Duration) -> &mut Self {
        if !v.is_zero() {
            self.time_to_live = Some(v);
        }
        self
    }

    /// Sets the time to idle of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::time_to_idle`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.time_to_idle)
    pub fn time_to_idle(&mut self, v: Duration) -> &mut Self {
        if !v.is_zero() {
            self.time_to_idle = Some(v);
        }
        self
    }

    /// Sets the segments number of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::segments`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.segments)
    pub fn segments(&mut self, v: usize) -> &mut Self {
        assert!(v != 0);
        self.num_segments = Some(v);
        self
    }

    /// Decides whether to enable thread pool of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::thread_pool_enabled`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.thread_pool_enabled)
    pub fn thread_pool_enabled(&mut self, v: bool) -> &mut Self {
        self.thread_pool_enabled = Some(v);
        self
    }
}

impl Builder for MokaBuilder {
    const SCHEME: Scheme = Scheme::Moka;
    type Accessor = MokaBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = MokaBuilder::default();

        map.get("name").map(|v| builder.name(v));
        map.get("max_capacity")
            .map(|v| v.parse::<u64>().map(|v| builder.max_capacity(v)));
        map.get("time_to_live").map(|v| {
            v.parse::<u64>()
                .map(|v| builder.time_to_live(Duration::from_secs(v)))
        });
        map.get("time_to_idle").map(|v| {
            v.parse::<u64>()
                .map(|v| builder.time_to_idle(Duration::from_secs(v)))
        });
        map.get("num_segments")
            .map(|v| v.parse::<usize>().map(|v| builder.segments(v)));
        map.get("thread_pool_enabled")
            .map(|v| v.parse::<bool>().map(|v| builder.thread_pool_enabled(v)));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let mut builder: CacheBuilder<String, Vec<u8>, _> =
            SegmentedCache::builder(self.num_segments.unwrap_or(1))
                .thread_pool_enabled(self.thread_pool_enabled.unwrap_or(false));
        // Use entries' bytes as capacity weigher.
        builder = builder.weigher(|k, v| (k.len() + v.len()) as u32);
        if let Some(v) = &self.name {
            builder = builder.name(v);
        }
        if let Some(v) = self.max_capacity {
            builder = builder.max_capacity(v)
        }
        if let Some(v) = self.time_to_live {
            builder = builder.time_to_live(v)
        }
        if let Some(v) = self.time_to_idle {
            builder = builder.time_to_idle(v)
        }

        debug!("backend build finished: {:?}", &self);
        Ok(MokaBackend::new(Adapter {
            inner: builder.build(),
        }))
    }
}

/// Backend is used to serve `Accessor` support in moka.
pub type MokaBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    inner: SegmentedCache<String, Vec<u8>>,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Adapter")
            .field("size", &self.inner.weighted_size())
            .field("count", &self.inner.entry_count())
            .finish()
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Moka,
            self.inner.name().unwrap_or("moka"),
            AccessorCapability::Read | AccessorCapability::Write,
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        self.blocking_get(path)
    }

    fn blocking_get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        match self.inner.get(path) {
            None => Ok(None),
            Some(bs) => Ok(Some(bs)),
        }
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        self.blocking_set(path, value)
    }

    fn blocking_set(&self, path: &str, value: &[u8]) -> Result<()> {
        self.inner.insert(path.to_string(), value.to_vec());

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.blocking_delete(path)
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        self.inner.invalidate(path);

        Ok(())
    }
}
