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
use moka::future::Cache;
use moka::future::CacheBuilder;

use super::core::*;
use super::delete::MokaDeleter;
use super::lister::MokaLister;
use super::writer::MokaWriter;
use crate::raw::oio;
use crate::raw::*;
use crate::services::MokaConfig;
use crate::*;

impl Configurator for MokaConfig {
    type Builder = MokaBuilder;
    fn into_builder(self) -> Self::Builder {
        MokaBuilder { config: self }
    }
}

/// [moka](https://github.com/moka-rs/moka) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct MokaBuilder {
    config: MokaConfig,
}

impl MokaBuilder {
    /// Name for this cache instance.
    pub fn name(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.name = Some(v.to_owned());
        }
        self
    }

    /// Sets the max capacity of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::max_capacity`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.max_capacity)
    pub fn max_capacity(mut self, v: u64) -> Self {
        if v != 0 {
            self.config.max_capacity = Some(v);
        }
        self
    }

    /// Sets the time to live of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::time_to_live`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.time_to_live)
    pub fn time_to_live(mut self, v: Duration) -> Self {
        if !v.is_zero() {
            self.config.time_to_live = Some(v);
        }
        self
    }

    /// Sets the time to idle of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::time_to_idle`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.time_to_idle)
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

impl Builder for MokaBuilder {
    const SCHEME: Scheme = Scheme::Moka;
    type Config = MokaConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        let mut builder: CacheBuilder<String, Buffer, _> = Cache::builder();

        // Use entries' bytes as capacity weigher.
        builder = builder.weigher(|k, v| (k.len() + v.len()) as u32);
        if let Some(v) = &self.config.name {
            builder = builder.name(v);
        }
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

        let core = MokaCore {
            cache: builder.build(),
        };

        Ok(MokaAccessor::new(core).with_normalized_root(root))
    }
}

/// MokaAccessor implements Access trait directly
#[derive(Debug, Clone)]
pub struct MokaAccessor {
    core: std::sync::Arc<MokaCore>,
    root: String,
    info: std::sync::Arc<AccessorInfo>,
}

impl MokaAccessor {
    fn new(core: MokaCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(Scheme::Moka);
        info.set_name(core.cache.name().unwrap_or("moka"));
        info.set_root("/");
        info.set_native_capability(Capability {
            read: true,
            write: true,
            delete: true,
            stat: true,
            list: true,
            write_can_empty: true,
            shared: false,
            ..Default::default()
        });

        Self {
            core: std::sync::Arc::new(core),
            root: "/".to_string(),
            info: std::sync::Arc::new(info),
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info.set_root(&root);
        self.root = root;
        self
    }
}

impl Access for MokaAccessor {
    type Reader = Buffer;
    type Writer = MokaWriter;
    type Lister = oio::HierarchyLister<MokaLister>;
    type Deleter = oio::OneShotDeleter<MokaDeleter>;

    fn info(&self) -> std::sync::Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p == build_abs_path(&self.root, "") {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            // Check the exact path first
            match self.core.get(&p).await? {
                Some(bs) => {
                    // If path ends with '/' but we found a file, return DIR
                    // This is because CompleteLayer's create_dir creates empty files with '/' suffix
                    let mode = if p.ends_with('/') {
                        EntryMode::DIR
                    } else {
                        EntryMode::FILE
                    };
                    Ok(RpStat::new(
                        Metadata::new(mode).with_content_length(bs.len() as u64),
                    ))
                }
                None => {
                    // If path ends with '/', check if there are any children
                    if p.ends_with('/') {
                        let has_children = self
                            .core
                            .cache
                            .iter()
                            .any(|kv| kv.0.starts_with(&p) && kv.0.len() > p.len());

                        if has_children {
                            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
                        } else {
                            Err(Error::new(ErrorKind::NotFound, "key not found in moka"))
                        }
                    } else {
                        Err(Error::new(ErrorKind::NotFound, "key not found in moka"))
                    }
                }
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);

        match self.core.get(&p).await? {
            Some(bs) => {
                let buffer = if args.range().is_full() {
                    bs
                } else {
                    let range = args.range();
                    let start = range.offset() as usize;
                    let end = match range.size() {
                        Some(size) => (range.offset() + size) as usize,
                        None => bs.len(),
                    };
                    bs.slice(start..end.min(bs.len()))
                };
                Ok((RpRead::new(), buffer))
            }
            None => Err(Error::new(ErrorKind::NotFound, "key not found in moka")),
        }
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((RpWrite::new(), MokaWriter::new(self.core.clone(), p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(MokaDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        // For moka, we don't distinguish between files and directories
        // Just return the lister to iterate through all matching keys
        let lister = MokaLister::new(self.core.clone(), self.root.clone(), path.to_string());
        let lister = oio::HierarchyLister::new(lister, path, args.recursive());
        Ok((RpList::default(), lister))
    }
}
