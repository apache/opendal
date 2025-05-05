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

use super::delete::HdfsNativeDeleter;
use super::error::parse_hdfs_error;
use super::lister::HdfsNativeLister;
use super::reader::HdfsNativeReader;
use super::writer::HdfsNativeWriter;
use crate::raw::*;
use crate::services::HdfsNativeConfig;
use crate::*;
use hdfs_native::HdfsError;
use hdfs_native::WriteOptions;
use log::debug;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

/// [Hadoop Distributed File System (HDFS™)](https://hadoop.apache.org/) support.
/// Using [Native Rust HDFS client](https://github.com/Kimahriman/hdfs-native).
impl Configurator for HdfsNativeConfig {
    type Builder = HdfsNativeBuilder;
    fn into_builder(self) -> Self::Builder {
        HdfsNativeBuilder { config: self }
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct HdfsNativeBuilder {
    config: HdfsNativeConfig,
}

impl Debug for HdfsNativeBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsNativeBuilder")
            .field("config", &self.config)
            .finish()
    }
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
    const SCHEME: Scheme = Scheme::HdfsNative;
    type Config = HdfsNativeConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let name_node = match &self.config.name_node {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "name_node is empty")
                    .with_context("service", Scheme::HdfsNative));
            }
        };

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", root);

        let client = hdfs_native::Client::new(name_node).map_err(parse_hdfs_error)?;

        // need to check if root dir exists, create if not
        Ok(HdfsNativeBackend {
            root,
            client: Arc::new(client),
            enable_append: self.config.enable_append,
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
    pub root: String,
    pub client: Arc<hdfs_native::Client>,
    enable_append: bool,
}

/// hdfs_native::Client is thread-safe.
unsafe impl Send for HdfsNativeBackend {}
unsafe impl Sync for HdfsNativeBackend {}

impl Access for HdfsNativeBackend {
    type Reader = HdfsNativeReader;
    type Writer = HdfsNativeWriter;
    type Lister = Option<HdfsNativeLister>;
    type Deleter = oio::OneShotDeleter<HdfsNativeDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let am = AccessorInfo::default();
        am.set_scheme(Scheme::HdfsNative)
            .set_root(&self.root)
            .set_native_capability(Capability {
                stat: true,
                stat_has_last_modified: true,
                stat_has_content_length: true,

                read: true,

                write: true,
                write_can_append: self.enable_append,

                create_dir: true,
                delete: true,

                list: true,
                list_has_content_length: true,
                list_has_last_modified: true,

                rename: true,

                shared: true,

                ..Default::default()
            });

        am.into()
    }

    async fn create_dir(&self, path: &str, _args: OpCreateDir) -> Result<RpCreateDir> {
        let p = build_rooted_abs_path(&self.root, path);

        self.client
            .mkdirs(&p, 0o777, true)
            .await
            .map_err(parse_hdfs_error)?;

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let p = build_rooted_abs_path(&self.root, path);

        let status: hdfs_native::client::FileStatus = self
            .client
            .get_file_info(&p)
            .await
            .map_err(parse_hdfs_error)?;

        let mode = if status.isdir {
            EntryMode::DIR
        } else {
            EntryMode::FILE
        };

        let mut metadata = Metadata::new(mode);
        metadata
            .set_last_modified(parse_datetime_from_from_timestamp_millis(
                status.modification_time as i64,
            )?)
            .set_content_length(status.length as u64);

        Ok(RpStat::new(metadata))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = self.client.read(&p).await.map_err(parse_hdfs_error)?;

        let r = HdfsNativeReader::new(
            f,
            args.range().offset() as _,
            args.range().size().unwrap_or(u64::MAX) as _,
        );

        Ok((RpRead::new(), r))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let target_path = build_rooted_abs_path(&self.root, path);
        let mut initial_size = 0;

        let target_exists = match self.client.get_file_info(&target_path).await {
            Ok(status) => {
                initial_size = status.length as u64;
                true
            }
            Err(err) => match &err {
                HdfsError::FileNotFound(_) => false,
                _ => return Err(parse_hdfs_error(err)),
            },
        };

        let f = if target_exists {
            if args.append() {
                assert!(self.enable_append, "append is not enabled");
                self.client
                    .append(&target_path)
                    .await
                    .map_err(parse_hdfs_error)?
            } else {
                initial_size = 0;
                self.client
                    .create(&target_path, WriteOptions::default().overwrite(true))
                    .await
                    .map_err(parse_hdfs_error)?
            }
        } else {
            initial_size = 0;
            self.client
                .create(&target_path, WriteOptions::default())
                .await
                .map_err(parse_hdfs_error)?
        };

        Ok((RpWrite::new(), HdfsNativeWriter::new(f, initial_size)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(HdfsNativeDeleter::new(Arc::new(self.clone()))),
        ))
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let p: String = build_rooted_abs_path(&self.root, path);

        let isdir = match self.client.get_file_info(&p).await {
            Ok(status) => status.isdir,
            Err(err) => {
                return match &err {
                    HdfsError::FileNotFound(_) => Ok((RpList::default(), None)),
                    _ => Err(parse_hdfs_error(err)),
                };
            }
        };
        let current_path = if isdir {
            if !path.ends_with("/") {
                Some(path.to_string() + "/")
            } else {
                Some(path.to_string())
            }
        } else {
            None
        };

        Ok((
            RpList::default(),
            Some(HdfsNativeLister::new(
                &self.root,
                &self.client,
                &p,
                current_path,
            )),
        ))
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let from_path = build_rooted_abs_path(&self.root, from);
        let to_path = build_rooted_abs_path(&self.root, to);
        match self.client.get_file_info(&to_path).await {
            Ok(status) => {
                if status.isdir {
                    return Err(Error::new(ErrorKind::IsADirectory, "path should be a file")
                        .with_context("input", &to_path));
                } else {
                    self.client
                        .delete(&to_path, true)
                        .await
                        .map_err(parse_hdfs_error)?;
                }
            }
            Err(err) => match &err {
                HdfsError::FileNotFound(_) => {
                    self.client
                        .create(&to_path, WriteOptions::default().create_parent(true))
                        .await
                        .map_err(parse_hdfs_error)?;
                }
                _ => return Err(parse_hdfs_error(err)),
            },
        };

        self.client
            .rename(&from_path, &to_path, true)
            .await
            .map_err(parse_hdfs_error)?;

        Ok(RpRename::default())
    }
}
