// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use async_trait::async_trait;
use log::debug;
use moka::sync::CacheBuilder;
use moka::sync::SegmentedCache;

use crate::adapters::kv;
use crate::wrappers::wrapper;
use crate::Accessor;
use crate::AccessorCapability;
use crate::Result;
use crate::Scheme;

/// Builder for moka backend
#[derive(Default, Debug)]
pub struct Builder {
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

impl Builder {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Self {
        let mut builder = Builder::default();
        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "name" => builder.name(v),
                "max_capacity" => match v.parse::<u64>() {
                    Ok(v) => builder.max_capacity(v),
                    _ => continue,
                },
                "time_to_live" => match v.parse::<u64>() {
                    Ok(v) => builder.time_to_live(Duration::from_secs(v)),
                    _ => continue,
                },
                "time_to_idle" => match v.parse::<u64>() {
                    Ok(v) => builder.time_to_idle(Duration::from_secs(v)),
                    _ => continue,
                },
                "num_segments" => match v.parse::<usize>() {
                    Ok(v) => builder.segments(v),
                    _ => continue,
                },
                "thread_pool_enabled" => match v.parse::<bool>() {
                    Ok(v) => builder.thread_pool_enabled(v),
                    _ => continue,
                },
                _ => continue,
            };
        }
        builder
    }

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

    /// Consume builder to build a moka backend.
    pub fn build(&mut self) -> Result<impl Accessor> {
        debug!("backend build started: {:?}", &self);

        let mut builder: CacheBuilder<String, Vec<u8>, _> =
            SegmentedCache::builder(self.num_segments.unwrap_or(1))
                .thread_pool_enabled(self.thread_pool_enabled.unwrap_or(false));
        // Use entries's bytes as capacity weigher.
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
        Ok(wrapper(Backend::new(Adapter {
            inner: builder.build(),
        })))
    }
}

/// Backend is used to serve `Accessor` support in moka.
pub type Backend = kv::Backend<Adapter>;

#[derive(Debug, Clone)]
pub struct Adapter {
    inner: SegmentedCache<String, Vec<u8>>,
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
