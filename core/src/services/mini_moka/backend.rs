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
use std::sync::Arc;
use std::time::Duration;

use log::debug;

use super::core::*;
use super::delete::MiniMokaDeleter;
use super::lister::MiniMokaLister;
use super::writer::MiniMokaWriter;
use super::DEFAULT_SCHEME;
use crate::raw::oio;
use crate::raw::oio::HierarchyLister;
use crate::raw::*;
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
#[derive(Default)]
pub struct MiniMokaBuilder {
    config: MiniMokaConfig,
}

impl Debug for MiniMokaBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiniMokaBuilder")
            .field("config", &self.config)
            .finish()
    }
}

impl MiniMokaBuilder {
    /// Create a [`MiniMokaBuilder`] with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

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
    type Config = MiniMokaConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let mut builder: mini_moka::sync::CacheBuilder<String, MiniMokaValue, _> =
            mini_moka::sync::Cache::builder();

        // Use entries' bytes as capacity weigher.
        builder = builder.weigher(|k, v| (k.len() + v.content.len()) as u32);

        if let Some(v) = self.config.max_capacity {
            builder = builder.max_capacity(v);
        }
        if let Some(v) = self.config.time_to_live {
            builder = builder.time_to_live(v);
        }
        if let Some(v) = self.config.time_to_idle {
            builder = builder.time_to_idle(v);
        }

        let cache = builder.build();

        let root = normalize_root(self.config.root.as_deref().unwrap_or("/"));

        let core = Arc::new(MiniMokaCore { cache });

        debug!("backend build finished: {root}");
        Ok(MiniMokaBackend::new(core, root))
    }
}

#[derive(Debug)]
struct MiniMokaBackend {
    core: Arc<MiniMokaCore>,
    root: String,
}

impl MiniMokaBackend {
    fn new(core: Arc<MiniMokaCore>, root: String) -> Self {
        Self { core, root }
    }
}

impl Access for MiniMokaBackend {
    type Reader = Buffer;
    type Writer = MiniMokaWriter;
    type Lister = HierarchyLister<MiniMokaLister>;
    type Deleter = oio::OneShotDeleter<MiniMokaDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        let info = AccessorInfo::default();
        info.set_scheme(DEFAULT_SCHEME)
            .set_root(&self.root)
            .set_native_capability(Capability {
                stat: true,
                read: true,
                write: true,
                write_can_empty: true,
                delete: true,
                list: true,

                ..Default::default()
            });

        Arc::new(info)
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        // Check if path exists directly in cache
        match self.core.get(&p) {
            Some(value) => {
                let mut metadata = value.metadata.clone();
                if p.ends_with('/') {
                    metadata.set_mode(EntryMode::DIR);
                } else {
                    metadata.set_mode(EntryMode::FILE);
                }
                Ok(RpStat::new(metadata))
            }
            None => {
                if p.ends_with('/') {
                    let is_prefix = self
                        .core
                        .cache
                        .iter()
                        .any(|entry| entry.key().starts_with(&p) && entry.key() != &p);

                    if is_prefix {
                        let mut metadata = Metadata::default();
                        metadata.set_mode(EntryMode::DIR);
                        return Ok(RpStat::new(metadata));
                    }
                }

                Err(Error::new(ErrorKind::NotFound, "path not found"))
            }
        }
    }

    async fn read(&self, path: &str, op: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);

        match self.core.get(&p) {
            Some(value) => {
                let range = op.range();

                // If range is full, return the content buffer directly
                if range.is_full() {
                    return Ok((RpRead::new(), value.content));
                }

                let offset = range.offset() as usize;
                if offset >= value.content.len() {
                    return Err(Error::new(
                        ErrorKind::RangeNotSatisfied,
                        "range start offset exceeds content length",
                    ));
                }

                let size = range.size().map(|s| s as usize);
                let end = size.map_or(value.content.len(), |s| {
                    (offset + s).min(value.content.len())
                });
                let sliced_content = value.content.slice(offset..end);

                Ok((RpRead::new(), sliced_content))
            }
            None => Err(Error::new(ErrorKind::NotFound, "path not found")),
        }
    }

    async fn write(&self, path: &str, op: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        let writer = MiniMokaWriter::new(self.core.clone(), p, op);
        Ok((RpWrite::new(), writer))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let deleter =
            oio::OneShotDeleter::new(MiniMokaDeleter::new(self.core.clone(), self.root.clone()));
        Ok((RpDelete::default(), deleter))
    }

    async fn list(&self, path: &str, op: OpList) -> Result<(RpList, Self::Lister)> {
        let p = build_abs_path(&self.root, path);

        let mini_moka_lister = MiniMokaLister::new(self.core.clone(), self.root.clone(), p);
        let lister = HierarchyLister::new(mini_moka_lister, path, op.recursive());

        Ok((RpList::default(), lister))
    }
}
