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
use crate::raw::*;
use crate::*;

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

    fn build(self) -> Result<impl Access> {
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
    info: Arc<AccessorInfo>,
}

impl SledBackend {
    pub fn new(core: SledCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(SLED_SCHEME)
            .set_name(&core.datadir)
            .set_root("/")
            .set_native_capability(Capability {
                read: true,
                stat: true,
                write: true,
                write_can_empty: true,
                delete: true,
                list: true,
                list_with_recursive: true,
                shared: false,
                ..Default::default()
            });

        Self {
            core: Arc::new(core),
            root: "/".to_string(),
            info: Arc::new(info),
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info.set_root(&root);
        self.root = root;
        self
    }
}

impl Access for SledBackend {
    type Reader = Buffer;
    type Writer = SledWriter;
    type Lister = oio::HierarchyLister<SledLister>;
    type Deleter = oio::OneShotDeleter<SledDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
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

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);
        let bs = match self.core.get(&p)? {
            Some(bs) => bs,
            None => {
                return Err(Error::new(ErrorKind::NotFound, "kv not found in sled"));
            }
        };
        Ok((RpRead::new(), bs.slice(args.range().to_range_as_usize())))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        let writer = SledWriter::new(self.core.clone(), p);
        Ok((RpWrite::new(), writer))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let deleter = SledDeleter::new(self.core.clone(), self.root.clone());
        Ok((RpDelete::default(), oio::OneShotDeleter::new(deleter)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let p = build_abs_path(&self.root, path);
        let lister = SledLister::new(self.core.clone(), self.root.clone(), p)?;
        Ok((
            RpList::default(),
            oio::HierarchyLister::new(lister, path, args.recursive()),
        ))
    }
}
