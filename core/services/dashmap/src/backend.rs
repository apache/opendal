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

use std::sync::Arc;

use dashmap::DashMap;
use log::debug;
use opendal_core::raw::*;
use opendal_core::*;

use super::DASHMAP_SCHEME;
use super::config::DashmapConfig;
use super::core::DashmapCore;
use super::deleter::DashmapDeleter;
use super::lister::DashmapLister;
use super::writer::DashmapWriter;

/// [dashmap](https://github.com/xacrimon/dashmap) backend support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct DashmapBuilder {
    pub(super) config: DashmapConfig,
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

    fn build(self) -> Result<impl Service> {
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

        Ok(DashmapBackend::new(core, root))
    }
}

#[derive(Debug, Clone)]
pub struct DashmapBackend {
    core: Arc<DashmapCore>,
    root: String,
    info: ServiceInfo,
    capability: Capability,
}

impl DashmapBackend {
    fn new(core: DashmapCore, root: String) -> Self {
        let info = ServiceInfo::new(DASHMAP_SCHEME, &root, "dashmap");
        let capability = Capability {
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
        };

        Self {
            core: Arc::new(core),
            root,
            info,
            capability,
        }
    }
}

/// Reader returned by this backend.
pub struct DashmapReader {
    backend: DashmapBackend,
    path: String,
}

impl DashmapReader {
    fn new(backend: DashmapBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for DashmapReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let p = build_abs_path(&backend.root, path);

        match backend.core.get(&p)? {
            Some(value) => {
                let total_size = value.content.len() as u64;
                let buffer = value
                    .content
                    .slice(range.to_content_range(value.content.len())?);
                let metadata = Metadata::new(EntryMode::FILE).with_content_length(total_size);
                Ok((
                    RpRead::new(metadata),
                    Box::new(buffer) as Box<dyn oio::ReadStreamDyn>,
                ))
            }
            None => Err(Error::new(ErrorKind::NotFound, "key not found in dashmap")),
        }
    }
}

impl Service for DashmapBackend {
    type Reader = oio::StreamReader<DashmapReader>;
    type Writer = DashmapWriter;
    type Lister = oio::HierarchyLister<DashmapLister>;
    type Deleter = oio::OneShotDeleter<DashmapDeleter>;
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
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
    async fn read(
        &self,
        _ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        let (rp, output): (_, oio::StreamReader<DashmapReader>) = {
            Ok((
                RpRead::default(),
                oio::StreamReader::new(DashmapReader::new(self.clone(), path, args)),
            ))
        }?;

        Ok((rp, output))
    }

    async fn write(
        &self,
        _ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        let (rp, output): (_, DashmapWriter) = {
            let p = build_abs_path(&self.root, path);
            Ok((
                RpWrite::new(),
                DashmapWriter::new(self.core.clone(), p, args),
            ))
        }?;

        Ok((rp, output))
    }

    async fn delete(&self, _ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, output): (_, oio::OneShotDeleter<DashmapDeleter>) = {
            Ok((
                RpDelete::default(),
                oio::OneShotDeleter::new(DashmapDeleter::new(self.core.clone(), self.root.clone())),
            ))
        }?;

        Ok((rp, output))
    }

    async fn list(
        &self,
        _ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        let (rp, output): (_, oio::HierarchyLister<DashmapLister>) = {
            let lister = DashmapLister::new(self.core.clone(), self.root.clone(), path.to_string());
            let lister = oio::HierarchyLister::new(lister, path, args.recursive());
            Ok((RpList::default(), lister))
        }?;

        Ok((rp, output))
    }
}
