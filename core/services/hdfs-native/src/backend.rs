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

use log::debug;

use super::HDFS_NATIVE_SCHEME;
use super::config::HdfsNativeConfig;
use super::core::HdfsNativeCore;
use super::deleter::HdfsNativeDeleter;
use super::error::parse_hdfs_error;
use super::lister::HdfsNativeLister;
use super::reader::HdfsNativeReader;
use super::writer::HdfsNativeWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// [Hadoop Distributed File System (HDFS™)](https://hadoop.apache.org/) support.
/// Using [Native Rust HDFS client](https://github.com/Kimahriman/hdfs-native).
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct HdfsNativeBuilder {
    pub(super) config: HdfsNativeConfig,
}

impl HdfsNativeBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
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
    pub fn name_node(mut self, name_node: &str) -> Self {
        if !name_node.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.config.name_node = Some(name_node.trim_end_matches('/').to_string())
        }

        self
    }

    /// Enable append capacity of this backend.
    ///
    /// This should be disabled when HDFS runs in non-distributed mode.
    pub fn enable_append(mut self, enable_append: bool) -> Self {
        self.config.enable_append = enable_append;
        self
    }
}

impl Builder for HdfsNativeBuilder {
    type Config = HdfsNativeConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let name_node = match &self.config.name_node {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "name_node is empty")
                    .with_context("service", HDFS_NATIVE_SCHEME));
            }
        };

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        let client = hdfs_native::ClientBuilder::new()
            .with_url(name_node)
            .build()
            .map_err(parse_hdfs_error)?;

        // need to check if root dir exists, create if not
        Ok(HdfsNativeBackend {
            core: Arc::new(HdfsNativeCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(HDFS_NATIVE_SCHEME)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,

                            read: true,

                            write: true,
                            write_can_append: self.config.enable_append,

                            create_dir: true,
                            delete: true,

                            list: true,

                            rename: true,

                            shared: true,

                            ..Default::default()
                        });

                    am.into()
                },
                root,
                client: Arc::new(client),
                enable_append: self.config.enable_append,
            }),
        })
    }
}

// #[inline]
// fn tmp_file_of(path: &str) -> String {
//     let name = get_basename(path);
//     let uuid = Uuid::new_v4().to_string();

//     format!("{name}.{uuid}")
// }

/// Backend for hdfs-native services.
#[derive(Debug, Clone)]
pub struct HdfsNativeBackend {
    core: Arc<HdfsNativeCore>,
}

impl Access for HdfsNativeBackend {
    type Reader = HdfsNativeReader;
    type Writer = HdfsNativeWriter;
    type Lister = Option<HdfsNativeLister>;
    type Deleter = oio::OneShotDeleter<HdfsNativeDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _args: OpCreateDir) -> Result<RpCreateDir> {
        self.core.hdfs_create_dir(path).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let m = self.core.hdfs_stat(path).await?;
        Ok(RpStat::new(m))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let (f, offset, size) = self.core.hdfs_read(path, &args).await?;

        let r = HdfsNativeReader::new(f, offset as _, size as _);

        Ok((RpRead::new(), r))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let (f, initial_size) = self.core.hdfs_write(path, &args).await?;

        Ok((RpWrite::new(), HdfsNativeWriter::new(f, initial_size)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(HdfsNativeDeleter::new(Arc::clone(&self.core))),
        ))
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        match self.core.hdfs_list(path).await? {
            Some((p, current_path)) => Ok((
                RpList::default(),
                Some(HdfsNativeLister::new(
                    &self.core.root,
                    &self.core.client,
                    &p,
                    current_path,
                )),
            )),
            None => Ok((RpList::default(), None)),
        }
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        self.core.hdfs_rename(from, to).await?;
        Ok(RpRename::default())
    }
}
