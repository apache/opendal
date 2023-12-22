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
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::AsyncWriteExt;
use log::debug;
use serde::Deserialize;

use super::lister::HdfsLister;
use super::writer::HdfsWriter;
use crate::raw::*;
use crate::*;

/// [Hadoop Distributed File System (HDFS™)](https://hadoop.apache.org/) support.

/// Config for Hdfs services support.
#[derive(Default, Deserialize, Clone)]
#[serde(default)]
#[non_exhaustive]
pub struct HdfsConfig {
    /// work dir of this backend
    pub root: Option<String>,
    /// name node of this backend
    pub name_node: Option<String>,
    /// kerberos_ticket_cache_path of this backend
    pub kerberos_ticket_cache_path: Option<String>,
    /// user of this backend
    pub user: Option<String>,
    /// enable the append capacity
    pub enable_append: bool,
}

impl Debug for HdfsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsConfig")
            .field("root", &self.root)
            .field("name_node", &self.name_node)
            .field(
                "kerberos_ticket_cache_path",
                &self.kerberos_ticket_cache_path,
            )
            .field("user", &self.user)
            .field("enable_append", &self.enable_append)
            .finish_non_exhaustive()
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct HdfsBuilder {
    config: HdfsConfig,
}

impl Debug for HdfsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsBuilder")
            .field("config", &self.config)
            .finish()
    }
}

impl HdfsBuilder {
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

    /// Set kerberos_ticket_cache_path of this backend
    ///
    /// This should be configured when kerberos is enabled.
    pub fn kerberos_ticket_cache_path(&mut self, kerberos_ticket_cache_path: &str) -> &mut Self {
        if !kerberos_ticket_cache_path.is_empty() {
            self.config.kerberos_ticket_cache_path = Some(kerberos_ticket_cache_path.to_string())
        }
        self
    }

    /// Set user of this backend
    pub fn user(&mut self, user: &str) -> &mut Self {
        if !user.is_empty() {
            self.config.user = Some(user.to_string())
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

impl Builder for HdfsBuilder {
    const SCHEME: Scheme = Scheme::Hdfs;
    type Accessor = HdfsBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        // Deserialize the configuration from the HashMap.
        let config = HdfsConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");

        // Create an HdfsBuilder instance with the deserialized config.
        HdfsBuilder { config }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let name_node = match &self.config.name_node {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "name node is empty")
                    .with_context("service", Scheme::Hdfs))
            }
        };

        let root = normalize_root(&self.config.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let mut builder = hdrs::ClientBuilder::new(name_node);
        if let Some(ticket_cache_path) = &self.config.kerberos_ticket_cache_path {
            builder = builder.with_kerberos_ticket_cache_path(ticket_cache_path.as_str());
        }
        if let Some(user) = &self.config.user {
            builder = builder.with_user(user.as_str());
        }

        let client = builder.connect().map_err(new_std_io_error)?;

        // Create root dir if not exist.
        if let Err(e) = client.metadata(&root) {
            if e.kind() == io::ErrorKind::NotFound {
                debug!("root {} is not exist, creating now", root);

                client.create_dir(&root).map_err(new_std_io_error)?
            }
        }

        debug!("backend build finished: {:?}", &self);
        Ok(HdfsBackend {
            root,
            client: Arc::new(client),
            enable_append: self.config.enable_append,
        })
    }
}

/// Backend for hdfs services.
#[derive(Debug, Clone)]
pub struct HdfsBackend {
    root: String,
    client: Arc<hdrs::Client>,
    enable_append: bool,
}

/// hdrs::Client is thread-safe.
unsafe impl Send for HdfsBackend {}
unsafe impl Sync for HdfsBackend {}

#[async_trait]
impl Accessor for HdfsBackend {
    type Reader = oio::FuturesReader<hdrs::AsyncFile>;
    type BlockingReader = oio::StdReader<hdrs::File>;
    type Writer = HdfsWriter<hdrs::AsyncFile>;
    type BlockingWriter = HdfsWriter<hdrs::File>;
    type Lister = Option<HdfsLister>;
    type BlockingLister = Option<HdfsLister>;

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Hdfs)
            .set_root(&self.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,
                read_can_seek: true,

                write: true,
                write_can_append: self.enable_append,

                create_dir: true,
                delete: true,

                list: true,

                rename: true,
                blocking: true,

                ..Default::default()
            });

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let p = build_rooted_abs_path(&self.root, path);

        self.client.create_dir(&p).map_err(new_std_io_error)?;

        Ok(RpCreateDir::default())
    }

    async fn read(&self, path: &str, _: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = self
            .client
            .open_file()
            .read(true)
            .async_open(&p)
            .await
            .map_err(new_std_io_error)?;

        let r = oio::FuturesReader::new(f);

        Ok((RpRead::new(), r))
    }

    async fn write(&self, path: &str, op: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_rooted_abs_path(&self.root, path);

        if let Err(err) = self.client.metadata(&p) {
            // Early return if other error happened.
            if err.kind() != io::ErrorKind::NotFound {
                return Err(new_std_io_error(err));
            }

            let parent = get_parent(&p);

            self.client.create_dir(parent).map_err(new_std_io_error)?;

            let mut f = self
                .client
                .open_file()
                .create(true)
                .write(true)
                .async_open(&p)
                .await
                .map_err(new_std_io_error)?;
            f.close().await.map_err(new_std_io_error)?;
        }

        let mut open_options = self.client.open_file();
        open_options.create(true);
        if op.append() {
            open_options.append(true);
        } else {
            open_options.write(true);
        }

        let f = open_options
            .async_open(&p)
            .await
            .map_err(new_std_io_error)?;

        Ok((RpWrite::new(), HdfsWriter::new(f)))
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let from_path = build_rooted_abs_path(&self.root, from);
        self.client.metadata(&from_path).map_err(new_std_io_error)?;

        let to_path = build_rooted_abs_path(&self.root, to);
        let result = self.client.metadata(&to_path);
        match result {
            Err(err) => {
                // Early return if other error happened.
                if err.kind() != io::ErrorKind::NotFound {
                    return Err(new_std_io_error(err));
                }

                let parent = PathBuf::from(&to_path)
                    .parent()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "path should have parent but not, it must be malformed",
                        )
                        .with_context("input", &to_path)
                    })?
                    .to_path_buf();

                self.client
                    .create_dir(&parent.to_string_lossy())
                    .map_err(new_std_io_error)?;
            }
            Ok(metadata) => {
                if metadata.is_file() {
                    self.client
                        .remove_file(&to_path)
                        .map_err(new_std_io_error)?;
                } else {
                    return Err(Error::new(ErrorKind::IsADirectory, "path should be a file")
                        .with_context("input", &to_path));
                }
            }
        }

        self.client
            .rename_file(&from_path, &to_path)
            .map_err(new_std_io_error)?;

        Ok(RpRename::new())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = self.client.metadata(&p).map_err(new_std_io_error)?;

        let mode = if meta.is_dir() {
            EntryMode::DIR
        } else if meta.is_file() {
            EntryMode::FILE
        } else {
            EntryMode::Unknown
        };
        let mut m = Metadata::new(mode);
        m.set_content_length(meta.len());
        m.set_last_modified(meta.modified().into());

        Ok(RpStat::new(m))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = self.client.metadata(&p);

        if let Err(err) = meta {
            return if err.kind() == io::ErrorKind::NotFound {
                Ok(RpDelete::default())
            } else {
                Err(new_std_io_error(err))
            };
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let result = if meta.is_dir() {
            self.client.remove_dir(&p)
        } else {
            self.client.remove_file(&p)
        };

        result.map_err(new_std_io_error)?;

        Ok(RpDelete::default())
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = match self.client.read_dir(&p) {
            Ok(f) => f,
            Err(e) => {
                return if e.kind() == io::ErrorKind::NotFound {
                    Ok((RpList::default(), None))
                } else {
                    Err(new_std_io_error(e))
                }
            }
        };

        let rd = HdfsLister::new(&self.root, f);

        Ok((RpList::default(), Some(rd)))
    }

    fn blocking_create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let p = build_rooted_abs_path(&self.root, path);

        self.client.create_dir(&p).map_err(new_std_io_error)?;

        Ok(RpCreateDir::default())
    }

    fn blocking_read(&self, path: &str, _: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = self
            .client
            .open_file()
            .read(true)
            .open(&p)
            .map_err(new_std_io_error)?;

        let r = oio::StdReader::new(f);

        Ok((RpRead::new(), r))
    }

    fn blocking_write(&self, path: &str, op: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let p = build_rooted_abs_path(&self.root, path);

        if let Err(err) = self.client.metadata(&p) {
            // Early return if other error happened.
            if err.kind() != io::ErrorKind::NotFound {
                return Err(new_std_io_error(err));
            }

            let parent = get_parent(&p);

            self.client.create_dir(parent).map_err(new_std_io_error)?;

            self.client
                .open_file()
                .create(true)
                .write(true)
                .open(&p)
                .map_err(new_std_io_error)?;
        }

        let mut open_options = self.client.open_file();
        open_options.create(true);
        if op.append() {
            open_options.append(true);
        } else {
            open_options.write(true);
        }

        let f = open_options.open(&p).map_err(new_std_io_error)?;

        Ok((RpWrite::new(), HdfsWriter::new(f)))
    }

    fn blocking_rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let from_path = build_rooted_abs_path(&self.root, from);
        self.client.metadata(&from_path).map_err(new_std_io_error)?;

        let to_path = build_rooted_abs_path(&self.root, to);
        let result = self.client.metadata(&to_path);
        match result {
            Err(err) => {
                // Early return if other error happened.
                if err.kind() != io::ErrorKind::NotFound {
                    return Err(new_std_io_error(err));
                }

                let parent = PathBuf::from(&to_path)
                    .parent()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "path should have parent but not, it must be malformed",
                        )
                        .with_context("input", &to_path)
                    })?
                    .to_path_buf();

                self.client
                    .create_dir(&parent.to_string_lossy())
                    .map_err(new_std_io_error)?;
            }
            Ok(metadata) => {
                if metadata.is_file() {
                    self.client
                        .remove_file(&to_path)
                        .map_err(new_std_io_error)?;
                } else {
                    return Err(Error::new(ErrorKind::IsADirectory, "path should be a file")
                        .with_context("input", &to_path));
                }
            }
        }

        self.client
            .rename_file(&from_path, &to_path)
            .map_err(new_std_io_error)?;

        Ok(RpRename::new())
    }

    fn blocking_stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = self.client.metadata(&p).map_err(new_std_io_error)?;

        let mode = if meta.is_dir() {
            EntryMode::DIR
        } else if meta.is_file() {
            EntryMode::FILE
        } else {
            EntryMode::Unknown
        };
        let mut m = Metadata::new(mode);
        m.set_content_length(meta.len());
        m.set_last_modified(meta.modified().into());

        Ok(RpStat::new(m))
    }

    fn blocking_delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = self.client.metadata(&p);

        if let Err(err) = meta {
            return if err.kind() == io::ErrorKind::NotFound {
                Ok(RpDelete::default())
            } else {
                Err(new_std_io_error(err))
            };
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let result = if meta.is_dir() {
            self.client.remove_dir(&p)
        } else {
            self.client.remove_file(&p)
        };

        result.map_err(new_std_io_error)?;

        Ok(RpDelete::default())
    }

    fn blocking_list(&self, path: &str, _: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = match self.client.read_dir(&p) {
            Ok(f) => f,
            Err(e) => {
                return if e.kind() == io::ErrorKind::NotFound {
                    Ok((RpList::default(), None))
                } else {
                    Err(new_std_io_error(e))
                }
            }
        };

        let rd = HdfsLister::new(&self.root, f);

        Ok((RpList::default(), Some(rd)))
    }
}
