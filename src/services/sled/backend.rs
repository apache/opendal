// Copyright 2023 Datafuse Labs.
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

use async_trait::async_trait;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::Builder;
use crate::Error;
use crate::ErrorKind;
use crate::Scheme;
use crate::*;

/// Sled service support.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [ ] ~~list~~
/// - [ ] ~~presign~~
/// - [ ] ~~multipart~~
/// - [x] blocking
///
/// # Note
///
/// The storage format for this service is not **stable** yet.
///
/// PLEASE DON'T USE THIS SERVICE FOR PERSIST DATA.
///
/// # Configuration
///
/// - `datadir`: Set the path to the sled data directory
///
/// You can refer to [`SledBuilder`]'s docs for more information
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::services::Sled;
/// use opendal::Object;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = Sled::default();
///     builder.datadir("/tmp/opendal/sled");
///
///     let op: Operator = Operator::create(builder)?.finish();
///     let _: Object = op.object("test_file");
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct SledBuilder {
    /// That path to the sled data directory.
    datadir: Option<String>,
}

impl SledBuilder {
    /// Set the path to the sled data directory. Will create if not exists.
    pub fn datadir(&mut self, path: &str) -> &mut Self {
        self.datadir = Some(path.into());
        self
    }
}

impl Builder for SledBuilder {
    const SCHEME: Scheme = Scheme::Sled;
    type Accessor = SledBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = SledBuilder::default();

        map.get("datadir").map(|v| builder.datadir(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let datadir_path = self.datadir.take().ok_or_else(|| {
            Error::new(
                ErrorKind::BackendConfigInvalid,
                "datadir is required but not set",
            )
            .with_context("service", Scheme::Sled)
        })?;

        let db = sled::open(&datadir_path).map_err(|e| {
            Error::new(ErrorKind::BackendConfigInvalid, "open db")
                .with_context("service", Scheme::Sled)
                .with_context("datadir", datadir_path.clone())
                .set_source(e)
        })?;

        Ok(SledBackend::new(Adapter {
            datadir: datadir_path,
            db,
        }))
    }
}

/// Backend for sled services.
pub type SledBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    datadir: String,
    db: sled::Db,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");
        ds.field("path", &self.datadir);
        ds.finish()
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Sled,
            &self.datadir,
            AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::Blocking,
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        self.blocking_get(path)
    }

    fn blocking_get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let parent = get_parent(path);
        let basename = get_basename(path);

        let tree = self.db.open_tree(parent).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Unable to open tree")
                .with_context("input", path)
                .set_source(e)
        })?;

        Ok(tree
            .get(basename)
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Unable to get")
                    .with_context("input", basename)
                    .set_source(e)
            })?
            .map(|v| v.to_vec()))
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        self.blocking_set(path, value)
    }

    fn blocking_set(&self, path: &str, value: &[u8]) -> Result<()> {
        let parent = get_parent(path);
        let basename = get_basename(path);

        let tree = self.db.open_tree(parent).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Unable to open tree")
                .with_context("input", path)
                .set_source(e)
        })?;

        tree.insert(basename, value).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Unable to set")
                .with_context("input", basename)
                .set_source(e)
        })?;

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.blocking_delete(path)
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        let parent = get_parent(path);
        let basename = get_basename(path);

        let tree = self.db.open_tree(parent).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Unable to open tree")
                .with_context("input", path)
                .set_source(e)
        })?;

        tree.remove(basename).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Unable to delete")
                .with_context("input", basename)
                .set_source(e)
        })?;

        Ok(())
    }
}
