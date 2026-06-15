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

use foundationdb::Database;

use super::FOUNDATIONDB_SCHEME;
use super::config::FoundationdbConfig;
use super::core::*;
use super::deleter::FoundationdbDeleter;
use super::writer::FoundationdbWriter;
use opendal_core::raw::*;
use opendal_core::*;

#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct FoundationdbBuilder {
    pub(super) config: FoundationdbConfig,
}

impl FoundationdbBuilder {
    /// Set the root for Foundationdb.
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = Some(path.into());
        self
    }

    /// Set the config path for Foundationdb. If not set, will fallback to use default
    pub fn config_path(mut self, path: &str) -> Self {
        self.config.config_path = Some(path.into());
        self
    }
}

impl Builder for FoundationdbBuilder {
    type Config = FoundationdbConfig;

    fn build(self) -> Result<impl Service> {
        let _network = Arc::new(unsafe { foundationdb::boot() });
        let db;
        if let Some(cfg_path) = &self.config.config_path {
            db = Database::from_path(cfg_path).map_err(|e| {
                Error::new(ErrorKind::ConfigInvalid, "open foundation db")
                    .with_context("service", FOUNDATIONDB_SCHEME)
                    .set_source(e)
            })?;
        } else {
            db = Database::default().map_err(|e| {
                Error::new(ErrorKind::ConfigInvalid, "open foundation db")
                    .with_context("service", FOUNDATIONDB_SCHEME)
                    .set_source(e)
            })?
        }

        let db = Arc::new(db);

        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        Ok(FoundationdbBackend::new(FoundationdbCore { db, _network }).with_normalized_root(root))
    }
}

/// Backend for Foundationdb services.
#[derive(Clone, Debug)]
pub struct FoundationdbBackend {
    core: Arc<FoundationdbCore>,
    root: String,
    info: ServiceInfo,
    capability: Capability,
}

impl FoundationdbBackend {
    pub fn new(core: FoundationdbCore) -> Self {
        let info = ServiceInfo::new(FOUNDATIONDB_SCHEME, "/", "foundationdb");
        let capability = Capability {
            read: true,
            stat: true,
            write: true,
            write_can_empty: true,
            delete: true,
            shared: true,
            ..Default::default()
        };

        Self {
            core: Arc::new(core),
            root: "/".to_string(),
            info,
            capability,
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info = self.info.with_root(&root);
        self.root = root;
        self
    }
}

/// Reader returned by this backend.
pub struct FoundationdbReader {
    backend: FoundationdbBackend,
    path: String,
}

impl FoundationdbReader {
    fn new(backend: FoundationdbBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for FoundationdbReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let p = build_abs_path(&backend.root, path);
        let bs = match backend.core.get(&p).await? {
            Some(bs) => bs,
            None => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "kv not found in foundationdb",
                ));
            }
        };
        let content = bs.slice(range.to_content_range(bs.len())?);
        let metadata = Metadata::new(EntryMode::FILE).with_content_length(bs.len() as u64);
        Ok((
            RpRead::new(metadata),
            Box::new(content) as Box<dyn oio::ReadStreamDyn>,
        ))
    }
}

impl Service for FoundationdbBackend {
    type Reader = oio::StreamReader<FoundationdbReader>;
    type Writer = FoundationdbWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<FoundationdbDeleter>;
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p == build_abs_path(&self.root, "") {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            let bs = self.core.get(&p).await?;
            match bs {
                Some(bs) => Ok(RpStat::new(
                    Metadata::new(EntryMode::FILE).with_content_length(bs.len() as u64),
                )),
                None => Err(Error::new(
                    ErrorKind::NotFound,
                    "kv not found in foundationdb",
                )),
            }
        }
    }
    async fn read(
        &self,
        _ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        let (rp, output): (_, oio::StreamReader<FoundationdbReader>) = {
            Ok((
                RpRead::default(),
                oio::StreamReader::new(FoundationdbReader::new(self.clone(), path, args)),
            ))
        }?;

        Ok((rp, output))
    }

    async fn write(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        let (rp, output): (_, FoundationdbWriter) = {
            let p = build_abs_path(&self.root, path);
            Ok((
                RpWrite::new(),
                FoundationdbWriter::new(self.core.clone(), p),
            ))
        }?;

        Ok((rp, output))
    }

    async fn delete(&self, _ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, output): (_, oio::OneShotDeleter<FoundationdbDeleter>) = {
            Ok((
                RpDelete::default(),
                oio::OneShotDeleter::new(FoundationdbDeleter::new(
                    self.core.clone(),
                    self.root.clone(),
                )),
            ))
        }?;

        Ok((rp, output))
    }
}
