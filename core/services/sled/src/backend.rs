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

use super::SLED_SCHEME;
use super::config::SledConfig;
use super::core::*;
use super::deleter::SledDeleter;
use super::lister::SledLister;
use super::writer::SledWriter;
use opendal_core::raw::*;
use opendal_core::*;

// https://github.com/spacejam/sled/blob/69294e59c718289ab3cb6bd03ac3b9e1e072a1e7/src/db.rs#L5
const DEFAULT_TREE_ID: &str = r#"__sled__default"#;

/// Sled services support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct SledBuilder {
    pub(super) config: SledConfig,
}

impl SledBuilder {
    /// Set the path to the sled data directory. Will create if not exists.
    pub fn datadir(mut self, path: &str) -> Self {
        self.config.datadir = Some(path.into());
        self
    }

    /// Set the tree for sled.
    pub fn tree(mut self, tree: &str) -> Self {
        self.config.tree = Some(tree.into());
        self
    }

    /// Set the root for sled.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }
}

impl Builder for SledBuilder {
    type Config = SledConfig;

    fn build(self) -> Result<impl Service> {
        let datadir_path = self.config.datadir.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "datadir is required but not set")
                .with_context("service", SLED_SCHEME)
        })?;

        let db = sled::open(&datadir_path).map_err(|e| {
            Error::new(ErrorKind::ConfigInvalid, "open db")
                .with_context("service", SLED_SCHEME)
                .with_context("datadir", datadir_path.clone())
                .set_source(e)
        })?;

        // use "default" tree if not set
        let tree_name = self
            .config
            .tree
            .unwrap_or_else(|| DEFAULT_TREE_ID.to_string());

        let tree = db.open_tree(&tree_name).map_err(|e| {
            Error::new(ErrorKind::ConfigInvalid, "open tree")
                .with_context("service", SLED_SCHEME)
                .with_context("datadir", datadir_path.clone())
                .with_context("tree", tree_name.clone())
                .set_source(e)
        })?;

        let root = normalize_root(&self.config.root.unwrap_or_default());

        Ok(SledBackend::new(SledCore {
            datadir: datadir_path,
            tree,
        })
        .with_normalized_root(root))
    }
}

/// Backend for sled services.
#[derive(Clone, Debug)]
pub struct SledBackend {
    core: Arc<SledCore>,
    root: String,
    info: ServiceInfo,
    capability: Capability,
}

impl SledBackend {
    pub fn new(core: SledCore) -> Self {
        let info = ServiceInfo::new(SLED_SCHEME, "/", &core.datadir);
        let capability = Capability {
            read: true,
            stat: true,
            write: true,
            write_can_empty: true,
            delete: true,
            list: true,
            list_with_recursive: true,
            shared: false,
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
pub struct SledReader {
    backend: SledBackend,
    path: String,
}

impl SledReader {
    fn new(backend: SledBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for SledReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let p = build_abs_path(&backend.root, path);
        let bs = match backend.core.get(&p)? {
            Some(bs) => bs,
            None => {
                return Err(Error::new(ErrorKind::NotFound, "kv not found in sled"));
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

impl Service for SledBackend {
    type Reader = oio::StreamReader<SledReader>;
    type Writer = SledWriter;
    type Lister = oio::HierarchyLister<SledLister>;
    type Deleter = oio::OneShotDeleter<SledDeleter>;
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p == build_abs_path(&self.root, "") {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            let bs = self.core.get(&p)?;
            match bs {
                Some(bs) => Ok(RpStat::new(
                    Metadata::new(EntryMode::FILE).with_content_length(bs.len() as u64),
                )),
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in sled")),
            }
        }
    }
    async fn read(
        &self,
        _ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        let (rp, output): (_, oio::StreamReader<SledReader>) = {
            Ok((
                RpRead::default(),
                oio::StreamReader::new(SledReader::new(self.clone(), path, args)),
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
        let (rp, output): (_, SledWriter) = {
            let p = build_abs_path(&self.root, path);
            let writer = SledWriter::new(self.core.clone(), p);
            Ok((RpWrite::new(), writer))
        }?;

        Ok((rp, output))
    }

    async fn delete(&self, _ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, output): (_, oio::OneShotDeleter<SledDeleter>) = {
            let deleter = SledDeleter::new(self.core.clone(), self.root.clone());
            Ok((RpDelete::default(), oio::OneShotDeleter::new(deleter)))
        }?;

        Ok((rp, output))
    }

    async fn list(
        &self,
        _ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        let (rp, output): (_, oio::HierarchyLister<SledLister>) = {
            let p = build_abs_path(&self.root, path);
            let lister = SledLister::new(self.core.clone(), self.root.clone(), p)?;
            Ok((
                RpList::default(),
                oio::HierarchyLister::new(lister, path, args.recursive()),
            ))
        }?;

        Ok((rp, output))
    }

    async fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}
