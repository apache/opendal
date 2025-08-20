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

use dashmap::DashMap;
use log::debug;

use super::core::DashmapCore;
use super::delete::DashmapDeleter;
use super::lister::DashmapLister;
use super::writer::DashmapWriter;
use super::DEFAULT_SCHEME;
use crate::raw::oio;
use crate::raw::*;
use crate::services::DashmapConfig;
use crate::*;
impl Configurator for DashmapConfig {
    type Builder = DashmapBuilder;
    fn into_builder(self) -> Self::Builder {
        DashmapBuilder { config: self }
    }
}

/// [dashmap](https://github.com/xacrimon/dashmap) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct DashmapBuilder {
    config: DashmapConfig,
}

impl Debug for DashmapBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DashmapBuilder")
            .field("config", &self.config)
            .finish()
    }
}

impl DashmapBuilder {
    /// Set the root for dashmap.
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = if path.is_empty() {
            None
        } else {
            Some(path.to_string())
        };

        self
    }
}

impl Builder for DashmapBuilder {
    type Config = DashmapConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        debug!("backend build finished: {:?}", self.config);

        let core = DashmapCore {
            cache: DashMap::new(),
        };

        Ok(DashmapAccessor::new(core, root))
    }
}

#[derive(Debug, Clone)]
pub struct DashmapAccessor {
    core: Arc<DashmapCore>,
    root: String,
    info: Arc<AccessorInfo>,
}

impl DashmapAccessor {
    fn new(core: DashmapCore, root: String) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(DEFAULT_SCHEME);
        info.set_name("dashmap");
        info.set_root(&root);
        info.set_native_capability(Capability {
            read: true,

            write: true,
            write_can_empty: true,
            write_with_cache_control: true,
            write_with_content_type: true,
            write_with_content_disposition: true,
            write_with_content_encoding: true,

            delete: true,
            stat: true,
            list: true,
            shared: false,
            ..Default::default()
        });

        Self {
            core: Arc::new(core),
            root,
            info: Arc::new(info),
        }
    }
}

impl Access for DashmapAccessor {
    type Reader = Buffer;
    type Writer = DashmapWriter;
    type Lister = oio::HierarchyLister<DashmapLister>;
    type Deleter = oio::OneShotDeleter<DashmapDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        match self.core.get(&p)? {
            Some(value) => {
                let metadata = value.metadata;
                Ok(RpStat::new(metadata))
            }
            None => {
                if p.ends_with('/') {
                    let has_children = self.core.cache.iter().any(|kv| kv.key().starts_with(&p));
                    if has_children {
                        return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
                    }
                }
                Err(Error::new(ErrorKind::NotFound, "key not found in dashmap"))
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);

        match self.core.get(&p)? {
            Some(value) => {
                let buffer = if args.range().is_full() {
                    value.content
                } else {
                    let range = args.range();
                    let start = range.offset() as usize;
                    let end = match range.size() {
                        Some(size) => (range.offset() + size) as usize,
                        None => value.content.len(),
                    };
                    value.content.slice(start..end.min(value.content.len()))
                };
                Ok((RpRead::new(), buffer))
            }
            None => Err(Error::new(ErrorKind::NotFound, "key not found in dashmap")),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((
            RpWrite::new(),
            DashmapWriter::new(self.core.clone(), p, args),
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(DashmapDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let lister = DashmapLister::new(self.core.clone(), self.root.clone(), path.to_string());
        let lister = oio::HierarchyLister::new(lister, path, args.recursive());
        Ok((RpList::default(), lister))
    }
}
