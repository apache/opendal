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
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use log::debug;
use serde::Deserialize;
use uuid::Uuid;

use super::lister::NativeHdfsLister;
use super::reader::NativeHdfsReader;
use super::writer::NativeHdfsWriter;
use crate::raw::*;
use crate::*;

/// [Hadoop Distributed File System (HDFS™)](https://hadoop.apache.org/) support.
/// Using [Native Rust HDFS client](https://github.com/Kimahriman/hdfs-native).

/// Config for NativeHdfs services support.
#[derive(Default, Deserialize, Clone)]
#[serde(default)]
#[non_exhaustive]
pub struct NativeHdfsConfig {
    /// work dir of this backend
    pub root: Option<String>,
    /// name node of this backend
    pub name_node: Option<String>,
    /// enable the append capacity
    pub enable_append: bool,
}

impl Debug for NativeHdfsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeHdfsConfig")
            .field("root", &self.root)
            .field("name_node", &self.name_node)
            .field("enable_append", &self.enable_append)
            .finish_non_exhaustive()
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct NativeHdfsBuilder {
    config: NativeHdfsConfig,
}

impl Debug for NativeHdfsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeHdfsBuilder")
            .field("config", &self.config)
            .finish()
    }
}

impl NativeHdfsBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set name_node of this backend.
    ///
    /// Valid format including:
    ///
    /// - `default`: using the default setting based on hadoop config.
    /// - `hdfs://127.0.0.1:9000`: connect to hdfs cluster.
    pub fn name_node(&mut self, name_node: &str) -> &mut Self {
        if !name_node.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.config.name_node = Some(name_node.trim_end_matches('/').to_string())
        }

        self
    }

    /// Enable append capacity of this backend.
    ///
    /// This should be disabled when HDFS runs in non-distributed mode.
    pub fn enable_append(&mut self, enable_append: bool) -> &mut Self {
        self.config.enable_append = enable_append;
        self
    }
}

impl Builder for NativeHdfsBuilder {
    const SCHEME: Scheme = Scheme::NativeHdfs;
    type Accessor = NativeHdfsBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        // Deserialize the configuration from the HashMap.
        let config = NativeHdfsConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");

        // Create an NativeHdfsBuilder instance with the deserialized config.
        NativeHdfsBuilder { config }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let name_node = match &self.config.name_node {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "name node is empty")
                    .with_context("service", Scheme::NativeHdfs))
            }
        };

        let root = normalize_root(&self.config.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let client = hdfs_native::Client::new(name_node).map_err(new_std_io_error)?;

        // need to check if root dir exists, create if not

        debug!("backend build finished: {:?}", &self);
        Ok(NativeHdfsBackend {
            root,
            client: Arc::new(client),
            enable_append: self.config.enable_append,
        })
    }
}

#[inline]
fn tmp_file_of(path: &str) -> String {
    let name = get_basename(path);
    let uuid = Uuid::new_v4().to_string();

    format!("{name}.{uuid}")
}

/// Backend for hdfs-native services.
#[derive(Debug, Clone)]
pub struct NativeHdfsBackend {
    root: String,
    client: Arc<hdfs_native::Client>,
    enable_append: bool,
}

/// hdfs_native::Client is thread-safe.
unsafe impl Send for NativeHdfsBackend {}
unsafe impl Sync for NativeHdfsBackend {}

#[async_trait]
impl Accessor for NativeHdfsBackend {
    type Reader = NativeHdfsReader;
    type BlockingReader = ();
    type Writer = NativeHdfsWriter;
    type BlockingWriter = ();
    type Lister = Option<NativeHdfsLister>;
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        todo!()
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let p = build_rooted_abs_path(&self.root, path);

        self.client
            .mkdirs(&p, 0o777, true)
            .await
            .map_err(new_std_io_error)?;
        Ok(RpCreateDir::default())
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = self.client.read(&p).await.map_err(new_std_io_error)?;

        let r = NativeHdfsReader::new(f);

        Ok((RpRead::new(), r))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        todo!()
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        todo!()
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        todo!()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        todo!()
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        todo!()
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let p = build_rooted_abs_path(&self.root, path);
        todo!()
    }
}
