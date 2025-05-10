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

use tokio::task;

use crate::raw::oio::HierarchyLister;
use crate::raw::*;
use crate::services::RedbConfig;
use crate::Builder;
use crate::Error;
use crate::ErrorKind;
use crate::Scheme;
use crate::*;

use super::core::RedbCore;
use super::deleter::RedbDeleter;
use super::error::*;
use super::lister::RedbFilter;
use super::lister::RedbLister;
use super::writer::RedbWriter;

impl Configurator for RedbConfig {
    type Builder = RedbBuilder;
    fn into_builder(self) -> Self::Builder {
        RedbBuilder { config: self }
    }
}

/// Redb service support.
#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct RedbBuilder {
    config: RedbConfig,
}

impl RedbBuilder {
    /// Set the path to the redb data directory. Will create if not exists.
    pub fn datadir(mut self, path: &str) -> Self {
        self.config.datadir = Some(path.into());
        self
    }

    /// Set the table name for Redb.
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
    const SCHEME: Scheme = Scheme::Redb;
    type Config = RedbConfig;

    fn build(self) -> Result<impl Access> {
        let datadir_path = self.config.datadir.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "datadir is required but not set")
                .with_context("service", Scheme::Redb)
        })?;

        let table_name = self.config.table.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "table is required but not set")
                .with_context("service", Scheme::Redb)
        })?;

        let db = redb::Database::create(&datadir_path).map_err(parse_database_error)?;

        Ok(RedbBackend {
            core: RedbCore {
                datadir: datadir_path,
                table: table_name,
                root: self.config.root.unwrap_or_default(),
                db,
            }
            .into(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RedbBackend {
    core: Arc<RedbCore>,
}

impl Access for RedbBackend {
    type Reader = Buffer;
    type Writer = RedbWriter;
    type Lister = HierarchyLister<RedbLister>;
    type Deleter = oio::OneShotDeleter<RedbDeleter>;
    type BlockingReader = Buffer;
    type BlockingWriter = RedbWriter;
    type BlockingLister = HierarchyLister<RedbFilter>;
    type BlockingDeleter = oio::OneShotDeleter<RedbDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        let am = AccessorInfo::default();
        am.set_scheme(Scheme::Redb)
            .set_root(&self.core.root)
            .set_name(&self.core.datadir)
            .set_native_capability(Capability {
                read: true,
                stat: true,

                write: true,
                write_can_empty: true,
                delete: true,

                list: true,
                list_with_recursive: true,

                blocking: true,
                shared: false,
                ..Default::default()
            });

        am.into()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let cloned_self = self.clone();
        let cloned_path = path.to_string();

        task::spawn_blocking(move || cloned_self.blocking_stat(cloned_path.as_str(), args))
            .await
            .map_err(new_task_join_error)
            .and_then(|inner_result| inner_result)
    }

    fn blocking_stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.core.root, path);

        if p == build_abs_path(&self.core.root, "") {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            let bs = self.core.get(&p)?;
            match bs {
                Some(bs) => Ok(RpStat::new(
                    Metadata::new(EntryMode::FILE).with_content_length(bs.len() as u64),
                )),
                None => Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let cloned_self = self.clone();
        let cloned_path = path.to_string();

        task::spawn_blocking(move || cloned_self.blocking_read(cloned_path.as_str(), args))
            .await
            .map_err(new_task_join_error)
            .and_then(|inner_result| inner_result)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let p = build_abs_path(&self.core.root, path);
        let bs = match self.core.get(&p)? {
            Some(bs) => Buffer::from(bs),
            None => return Err(Error::new(ErrorKind::NotFound, "kv doesn't have this path")),
        };
        Ok((RpRead::new(), bs.slice(args.range().to_range_as_usize())))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.blocking_write(path, args)
    }

    fn blocking_write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let p = build_abs_path(&self.core.root, path);

        Ok((RpWrite::new(), RedbWriter::new(self.core.clone(), p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.blocking_delete()
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(RedbDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let pattern = build_abs_path(&self.core.root, path);
        let range = self.core.iter()?;
        let lister = RedbLister::new(RedbFilter::new(range, pattern));
        let lister = HierarchyLister::new(lister, path, args.recursive());

        Ok((RpList::default(), lister))
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let pattern = build_abs_path(&self.core.root, path);
        let range = self.core.iter()?;
        let lister = RedbFilter::new(range, pattern);
        let lister = HierarchyLister::new(lister, path, args.recursive());

        Ok((RpList::default(), lister))
    }
}
