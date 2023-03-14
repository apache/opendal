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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use async_trait::async_trait;
use rocksdb::DB;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::Result;
use crate::*;

/// Rocksdb service support.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [ ] ~~list~~
/// - [ ] scan
/// - [ ] ~~presign~~
/// - [x] blocking
///
/// # Note
///
/// OpenDAL will build rocksdb from source by default.
///
/// To link with existing rocksdb lib, please set one of the following:
///
/// - `ROCKSDB_LIB_DIR` to the dir that contains `librocksdb.so`
/// - `ROCKSDB_STATIC` to the dir that contains `librocksdb.a`
///
/// If the version of RocksDB is below 6.0, you may encounter compatibility
/// issues. It is advisable to follow the steps provided in the [`INSTALL`](https://github.com/facebook/rocksdb/blob/main/INSTALL.md)
/// file to build rocksdb, rather than relying on system libraries that
/// may be outdated and incompatible.
///
/// # Configuration
///
/// - `root`: Set the working directory of `OpenDAL`
/// - `datadir`: Set the path to the rocksdb data directory
///
/// You can refer to [`RocksdbBuilder`]'s docs for more information
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::services::Rocksdb;
/// use opendal::Object;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = Rocksdb::default();
///     builder.datadir("/tmp/opendal/rocksdb");
///
///     let op: Operator = Operator::new(builder)?.finish();
///     let _: Object = op.object("test_file");
///     Ok(())
/// }
/// ```
#[derive(Clone, Default, Debug)]
pub struct RocksdbBuilder {
    /// The path to the rocksdb data directory.
    datadir: Option<String>,
    /// the working directory of the service. Can be "/path/to/dir"
    ///
    /// default is "/"
    root: Option<String>,
}

impl RocksdbBuilder {
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
}

impl Builder for RocksdbBuilder {
    const SCHEME: Scheme = Scheme::Rocksdb;
    type Accessor = RocksdbBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = RocksdbBuilder::default();

        map.get("datadir").map(|v| builder.datadir(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let path = self.datadir.take().ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "datadir is required but not set")
                .with_context("service", Scheme::Rocksdb)
        })?;
        let db = DB::open_default(&path).map_err(|e| {
            Error::new(ErrorKind::ConfigInvalid, "open default transaction db")
                .with_context("service", Scheme::Rocksdb)
                .with_context("datadir", path)
                .set_source(e)
        })?;

        Ok(RocksdbBackend::new(Adapter { db: Arc::new(db) }))
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
        Ok(self.db.get(path)?)
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        self.blocking_set(path, value)
    }

    fn blocking_set(&self, path: &str, value: &[u8]) -> Result<()> {
        Ok(self.db.put(path, value)?)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.blocking_delete(path)
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        Ok(self.db.delete(path)?)
    }
}

impl From<rocksdb::Error> for Error {
    fn from(e: rocksdb::Error) -> Self {
        Error::new(ErrorKind::Unexpected, "got rocksdb error").set_source(e)
    }
}
