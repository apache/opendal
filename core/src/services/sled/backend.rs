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
use std::str;

use tokio::task;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::services::SledConfig;
use crate::Builder;
use crate::Error;
use crate::ErrorKind;
use crate::Scheme;
use crate::*;

// https://github.com/spacejam/sled/blob/69294e59c718289ab3cb6bd03ac3b9e1e072a1e7/src/db.rs#L5
const DEFAULT_TREE_ID: &str = r#"__sled__default"#;

impl Configurator for SledConfig {
    type Builder = SledBuilder;
    fn into_builder(self) -> Self::Builder {
        SledBuilder { config: self }
    }
}

/// Sled services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct SledBuilder {
    config: SledConfig,
}

impl Debug for SledBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SledBuilder")
            .field("config", &self.config)
            .finish()
    }
}

impl SledBuilder {
    /// Set the path to the sled data directory. Will create if not exists.
    pub fn datadir(mut self, path: &str) -> Self {
        self.config.datadir = Some(path.into());
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

    /// Set the tree for sled.
    pub fn tree(mut self, tree: &str) -> Self {
        self.config.tree = Some(tree.into());
        self
    }
}

impl Builder for SledBuilder {
    const SCHEME: Scheme = Scheme::Sled;
    type Config = SledConfig;

    fn build(self) -> Result<impl Access> {
        let datadir_path = self.config.datadir.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "datadir is required but not set")
                .with_context("service", Scheme::Sled)
        })?;

        let db = sled::open(&datadir_path).map_err(|e| {
            Error::new(ErrorKind::ConfigInvalid, "open db")
                .with_context("service", Scheme::Sled)
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
                .with_context("service", Scheme::Sled)
                .with_context("datadir", datadir_path.clone())
                .with_context("tree", tree_name.clone())
                .set_source(e)
        })?;

        Ok(SledBackend::new(Adapter {
            datadir: datadir_path,
            tree,
        })
        .with_root(self.config.root.as_deref().unwrap_or("/")))
    }
}

/// Backend for sled services.
pub type SledBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    datadir: String,
    tree: sled::Tree,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");
        ds.field("path", &self.datadir);
        ds.finish()
    }
}

impl kv::Adapter for Adapter {
    type Scanner = kv::Scanner;

    fn info(&self) -> kv::Info {
        kv::Info::new(
            Scheme::Sled,
            &self.datadir,
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
        Ok(self
            .tree
            .get(path)
            .map_err(parse_error)?
            .map(|v| Buffer::from(v.to_vec())))
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let cloned_self = self.clone();
        let cloned_path = path.to_string();

        task::spawn_blocking(move || cloned_self.blocking_set(cloned_path.as_str(), value))
            .await
            .map_err(new_task_join_error)?
    }

    fn blocking_set(&self, path: &str, value: Buffer) -> Result<()> {
        self.tree
            .insert(path, value.to_vec())
            .map_err(parse_error)?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let cloned_self = self.clone();
        let cloned_path = path.to_string();

        task::spawn_blocking(move || cloned_self.blocking_delete(cloned_path.as_str()))
            .await
            .map_err(new_task_join_error)?
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        self.tree.remove(path).map_err(parse_error)?;

        Ok(())
    }

    async fn scan(&self, path: &str) -> Result<Self::Scanner> {
        let cloned_self = self.clone();
        let cloned_path = path.to_string();

        let res = task::spawn_blocking(move || cloned_self.blocking_scan(cloned_path.as_str()))
            .await
            .map_err(new_task_join_error)??;

        Ok(Box::new(kv::ScanStdIter::new(res.into_iter().map(Ok))))
    }

    fn blocking_scan(&self, path: &str) -> Result<Vec<String>> {
        let it = self.tree.scan_prefix(path).keys();
        let mut res = Vec::default();

        for i in it {
            let bs = i.map_err(parse_error)?.to_vec();
            let v = String::from_utf8(bs).map_err(|err| {
                Error::new(ErrorKind::Unexpected, "store key is not valid utf-8 string")
                    .set_source(err)
            })?;

            res.push(v);
        }

        Ok(res)
    }
}

fn parse_error(err: sled::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "error from sled").set_source(err)
}
