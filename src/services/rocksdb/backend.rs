// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use rocksdb::TransactionDB;

use crate::adapters::kv;
use crate::error::new_other_backend_error;
use crate::Accessor;
use crate::AccessorCapability;
use crate::Scheme;

/// Rocksdb backend builder
#[derive(Clone, Default, Debug)]
pub struct Builder {
    /// The path to the rocksdb data directory.
    datadir: Option<String>,
    /// the working directory of the Redis service. Can be "/path/to/dir"
    ///
    /// default is "/"
    root: Option<String>,
}

impl Builder {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Self {
        let mut builder = Builder::default();
        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "datadir" => builder.datadir(v),
                _ => continue,
            };
        }
        builder
    }

    /// Set the path to the rocksdb data directory. Will create if not exists.
    pub fn datadir(&mut self, path: &str) -> &mut Self {
        self.datadir = Some(path.into());
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_owned());
        }
        self
    }

    /// Consumes the builder and returns a `Rocksdb` instance.
    pub fn build(&mut self) -> Result<impl Accessor> {
        let path = self.datadir.take().ok_or_else(|| {
            new_other_backend_error(
                HashMap::from([("datadir".into(), "".into())]),
                anyhow!("datadir is required but not set"),
            )
        })?;
        let db = TransactionDB::open_default(&path).map_err(|e| {
            new_other_backend_error(
                HashMap::from([("datadir".into(), path)]),
                anyhow!("failed to open the database: {:?}", e),
            )
        })?;

        Ok(Backend::new(Adapter { db: Arc::new(db) }))
    }
}

/// Backend for rocksdb services.
pub type Backend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    db: Arc<TransactionDB>,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");
        ds.field("path", &self.db.path());
        ds.finish()
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Rocksdb,
            &self.db.path().to_string_lossy(),
            AccessorCapability::Read | AccessorCapability::Write,
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        self.blocking_get(path)
    }

    fn blocking_get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        self.db.get(path).map_err(new_rocksdb_error)
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        self.blocking_set(path, value)
    }

    fn blocking_set(&self, path: &str, value: &[u8]) -> Result<()> {
        self.db.put(path, value).map_err(new_rocksdb_error)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.blocking_delete(path)
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        self.db.delete(path).map_err(new_rocksdb_error)
    }
}

fn new_rocksdb_error(err: impl Debug) -> Error {
    Error::new(ErrorKind::Other, anyhow!("rocksdb: {err:?}"))
}
