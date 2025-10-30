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

use super::config::RedbConfig;
use super::core::*;
use super::deleter::RedbDeleter;
use super::writer::RedbWriter;
use crate::raw::*;
use crate::*;

/// Redb service support.
#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct RedbBuilder {
    pub(super) config: RedbConfig,

    pub(super) database: Option<Arc<redb::Database>>,
}

impl RedbBuilder {
    /// Set the database for Redb.
    ///
    /// This method should be called when you want to
    /// use multiple tables of one database because
    /// Redb doesn't allow opening a database that have been opened.
    ///
    /// <div class="warning">
    ///
    /// `datadir` and `database` should not be set simultaneously.
    /// If both are set, `database` will take precedence.
    ///
    /// </div>
    pub fn database(mut self, db: Arc<redb::Database>) -> Self {
        self.database = Some(db);
        self
    }

    /// Set the path to the redb data directory. Will create if not exists.
    ///
    ///
    /// <div class="warning">
    ///
    /// Opening redb database via `datadir` takes away the ability to access multiple redb tables.
    /// If you need to access multiple redb tables, the correct solution is to
    /// create an `Arc<redb::database>` beforehand and then share it via [`database`]
    /// with multiple builders where every builder will open one redb table.
    ///
    /// </div>
    ///
    /// [`database`]: RedbBuilder::database
    pub fn datadir(mut self, path: &str) -> Self {
        self.config.datadir = Some(path.into());
        self
    }

    /// Set the table name for Redb. Will create if not exists.
    pub fn table(mut self, table: &str) -> Self {
        self.config.table = Some(table.into());
        self
    }

    /// Set the root for Redb.
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = Some(path.into());
        self
    }
}

impl Builder for RedbBuilder {
    type Config = RedbConfig;

    fn build(self) -> Result<impl Access> {
        let table_name = self.config.table.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "table is required but not set")
                .with_context("service", Scheme::Redb)
        })?;

        let (datadir, db) = if let Some(db) = self.database {
            (None, db)
        } else {
            let datadir = self.config.datadir.ok_or_else(|| {
                Error::new(ErrorKind::ConfigInvalid, "datadir is required but not set")
                    .with_context("service", Scheme::Redb)
            })?;

            let db = redb::Database::create(&datadir)
                .map_err(parse_database_error)?
                .into();

            (Some(datadir), db)
        };

        create_table(&db, &table_name)?;

        let root = normalize_root(&self.config.root.unwrap_or_default());

        Ok(RedbBackend::new(RedbCore {
            datadir,
            table: table_name,
            db,
        })
        .with_normalized_root(root))
    }
}

/// Backend for Redb services.
#[derive(Clone, Debug)]
pub struct RedbBackend {
    core: Arc<RedbCore>,
    root: String,
    info: Arc<AccessorInfo>,
}

impl RedbBackend {
    pub fn new(core: RedbCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(Scheme::Redb.into_static());
        info.set_name(&core.table);
        info.set_root("/");
        info.set_native_capability(Capability {
            read: true,
            stat: true,
            write: true,
            write_can_empty: true,
            delete: true,
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

impl Access for RedbBackend {
    type Reader = Buffer;
    type Writer = RedbWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<RedbDeleter>;

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
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in redb")),
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);
        let bs = match self.core.get(&p)? {
            Some(bs) => bs,
            None => {
                return Err(Error::new(ErrorKind::NotFound, "kv not found in redb"));
            }
        };
        Ok((RpRead::new(), bs.slice(args.range().to_range_as_usize())))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((RpWrite::new(), RedbWriter::new(self.core.clone(), p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(RedbDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let _ = build_abs_path(&self.root, path);
        Ok((RpList::default(), ()))
    }
}
