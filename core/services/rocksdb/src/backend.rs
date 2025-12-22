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

use opendal_core::raw::*;
use opendal_core::*;
use rocksdb::DB;

use super::ROCKSDB_SCHEME;
use super::config::RocksdbConfig;
use super::core::*;
use super::deleter::RocksdbDeleter;
use super::lister::RocksdbLister;
use super::writer::RocksdbWriter;

/// RocksDB service support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct RocksdbBuilder {
    pub(super) config: RocksdbConfig,
}

impl RocksdbBuilder {
    /// Set the path to the rocksdb data directory. Creates if not exists.
    pub fn datadir(mut self, path: &str) -> Self {
        self.config.datadir = Some(path.into());
        self
    }

    /// Set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }
}

impl Builder for RocksdbBuilder {
    type Config = RocksdbConfig;

    fn build(self) -> Result<impl Access> {
        let path = self.config.datadir.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "datadir is required but not set")
                .with_context("service", ROCKSDB_SCHEME)
        })?;
        let db = DB::open_default(&path).map_err(|e| {
            Error::new(ErrorKind::ConfigInvalid, "open default transaction db")
                .with_context("service", ROCKSDB_SCHEME)
                .with_context("datadir", path)
                .set_source(e)
        })?;

        let root = normalize_root(&self.config.root.unwrap_or_default());

        Ok(RocksdbBackend::new(RocksdbCore { db: Arc::new(db) }).with_normalized_root(root))
    }
}

/// Backend for rocksdb service.
#[derive(Clone, Debug)]
pub struct RocksdbBackend {
    core: Arc<RocksdbCore>,
    root: String,
    info: Arc<AccessorInfo>,
}

impl RocksdbBackend {
    pub fn new(core: RocksdbCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(ROCKSDB_SCHEME)
            .set_name(&core.db.path().to_string_lossy())
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

impl Access for RocksdbBackend {
    type Reader = Buffer;
    type Writer = RocksdbWriter;
    type Lister = oio::HierarchyLister<RocksdbLister>;
    type Deleter = oio::OneShotDeleter<RocksdbDeleter>;

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
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in rocksdb")),
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);
        let bs = match self.core.get(&p)? {
            Some(bs) => bs,
            None => {
                return Err(Error::new(ErrorKind::NotFound, "kv not found in rocksdb"));
            }
        };
        Ok((RpRead::new(), bs.slice(args.range().to_range_as_usize())))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        let writer = RocksdbWriter::new(self.core.clone(), p);
        Ok((RpWrite::new(), writer))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let deleter = RocksdbDeleter::new(self.core.clone(), self.root.clone());
        Ok((RpDelete::default(), oio::OneShotDeleter::new(deleter)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let p = build_abs_path(&self.root, path);
        let lister = RocksdbLister::new(self.core.clone(), self.root.clone(), p)?;
        Ok((
            RpList::default(),
            oio::HierarchyLister::new(lister, path, args.recursive()),
        ))
    }
}
