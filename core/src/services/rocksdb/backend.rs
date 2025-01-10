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

use rocksdb::DB;
use tokio::task;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::services::RocksdbConfig;
use crate::Result;
use crate::*;

impl Configurator for RocksdbConfig {
    type Builder = RocksdbBuilder;
    fn into_builder(self) -> Self::Builder {
        RocksdbBuilder { config: self }
    }
}

/// RocksDB service support.
#[doc = include_str!("docs.md")]
#[derive(Clone, Default)]
pub struct RocksdbBuilder {
    config: RocksdbConfig,
}

impl RocksdbBuilder {
    /// Set the path to the rocksdb data directory. Will create if not exists.
    pub fn datadir(mut self, path: &str) -> Self {
        self.config.datadir = Some(path.into());
        self
    }

    /// set the working directory, all operations will be performed under it.
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
    const SCHEME: Scheme = Scheme::Rocksdb;
    type Config = RocksdbConfig;

    fn build(self) -> Result<impl Access> {
        let path = self.config.datadir.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "datadir is required but not set")
                .with_context("service", Scheme::Rocksdb)
        })?;
        let db = DB::open_default(&path).map_err(|e| {
            Error::new(ErrorKind::ConfigInvalid, "open default transaction db")
                .with_context("service", Scheme::Rocksdb)
                .with_context("datadir", path)
                .set_source(e)
        })?;

        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        Ok(RocksdbBackend::new(Adapter { db: Arc::new(db) }).with_normalized_root(root))
    }
}

/// Backend for rocksdb services.
pub type RocksdbBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    db: Arc<DB>,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");
        ds.field("path", &self.db.path());
        ds.finish()
    }
}

impl kv::Adapter for Adapter {
    type Scanner = kv::Scanner;

    fn info(&self) -> kv::Info {
        kv::Info::new(
            Scheme::Rocksdb,
            &self.db.path().to_string_lossy(),
            Capability {
                read: true,
                write: true,
                list: true,
                blocking: true,
                shared: false,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let cloned_self = self.clone();
        let cloned_path = path.to_string();

        task::spawn_blocking(move || cloned_self.blocking_get(cloned_path.as_str()))
            .await
            .map_err(new_task_join_error)?
    }

    fn blocking_get(&self, path: &str) -> Result<Option<Buffer>> {
        let result = self.db.get(path).map_err(parse_rocksdb_error)?;
        Ok(result.map(Buffer::from))
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let cloned_self = self.clone();
        let cloned_path = path.to_string();

        task::spawn_blocking(move || cloned_self.blocking_set(cloned_path.as_str(), value))
            .await
            .map_err(new_task_join_error)?
    }

    fn blocking_set(&self, path: &str, value: Buffer) -> Result<()> {
        self.db
            .put(path, value.to_vec())
            .map_err(parse_rocksdb_error)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let cloned_self = self.clone();
        let cloned_path = path.to_string();

        task::spawn_blocking(move || cloned_self.blocking_delete(cloned_path.as_str()))
            .await
            .map_err(new_task_join_error)?
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        self.db.delete(path).map_err(parse_rocksdb_error)
    }

    async fn scan(&self, path: &str) -> Result<Self::Scanner> {
        let cloned_self = self.clone();
        let cloned_path = path.to_string();

        let res = task::spawn_blocking(move || cloned_self.blocking_scan(cloned_path.as_str()))
            .await
            .map_err(new_task_join_error)??;

        Ok(Box::new(kv::ScanStdIter::new(res.into_iter().map(Ok))))
    }

    /// TODO: we only need key here.
    fn blocking_scan(&self, path: &str) -> Result<Vec<String>> {
        let it = self.db.prefix_iterator(path).map(|r| r.map(|(k, _)| k));
        let mut res = Vec::default();

        for key in it {
            let key = key.map_err(parse_rocksdb_error)?;
            let key = String::from_utf8_lossy(&key);
            if !key.starts_with(path) {
                break;
            }
            res.push(key.to_string());
        }

        Ok(res)
    }
}

fn parse_rocksdb_error(e: rocksdb::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "got rocksdb error").set_source(e)
}
