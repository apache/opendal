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

use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use log::debug;

use super::error::parse_io_error;
use super::pager::HdfsPager;
use super::writer::HdfsWriter;
use crate::raw::*;
use crate::*;

/// [Hadoop Distributed File System (HDFS™)](https://hadoop.apache.org/) support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct HdfsBuilder {
    root: Option<String>,
    name_node: Option<String>,
    kerberos_ticket_cache_path: Option<String>,
    user: Option<String>,
}

impl HdfsBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
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
            self.name_node = Some(name_node.trim_end_matches('/').to_string())
        }

        self
    }

    /// Set kerberos_ticket_cache_path of this backend
    ///
    /// This should be configured when kerberos is enabled.
    pub fn kerberos_ticket_cache_path(&mut self, kerberos_ticket_cache_path: &str) -> &mut Self {
        if !kerberos_ticket_cache_path.is_empty() {
            self.kerberos_ticket_cache_path = Some(kerberos_ticket_cache_path.to_string())
        }
        self
    }

    /// Set user of this backend
    pub fn user(&mut self, user: &str) -> &mut Self {
        if !user.is_empty() {
            self.user = Some(user.to_string())
        }
        self
    }
}

impl Builder for HdfsBuilder {
    const SCHEME: Scheme = Scheme::Hdfs;
    type Accessor = HdfsBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = HdfsBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("name_node").map(|v| builder.name_node(v));
        map.get("kerberos_ticket_cache_path")
            .map(|v| builder.kerberos_ticket_cache_path(v));
        map.get("user").map(|v| builder.user(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let name_node = match &self.name_node {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "name node is empty")
                    .with_context("service", Scheme::Hdfs))
            }
        };

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let mut builder = hdrs::ClientBuilder::new(name_node);
        if let Some(ticket_cache_path) = &self.kerberos_ticket_cache_path {
            builder = builder.with_kerberos_ticket_cache_path(ticket_cache_path.as_str());
        }
        if let Some(user) = &self.user {
            builder = builder.with_user(user.as_str());
        }

        let client = builder.connect().map_err(parse_io_error)?;

        // Create root dir if not exist.
        if let Err(e) = client.metadata(&root) {
            if e.kind() == io::ErrorKind::NotFound {
                debug!("root {} is not exist, creating now", root);

                client.create_dir(&root).map_err(parse_io_error)?
            }
        }

        debug!("backend build finished: {:?}", &self);
        Ok(HdfsBackend {
            root,
            client: Arc::new(client),
        })
    }
}

/// Backend for hdfs services.
#[derive(Debug, Clone)]
pub struct HdfsBackend {
    root: String,
    client: Arc<hdrs::Client>,
}

/// hdrs::Client is thread-safe.
unsafe impl Send for HdfsBackend {}
unsafe impl Sync for HdfsBackend {}

#[async_trait]
impl Accessor for HdfsBackend {
    type Reader = oio::FromFileReader<hdrs::AsyncFile>;
    type BlockingReader = oio::FromFileReader<hdrs::File>;
    type Writer = HdfsWriter<hdrs::AsyncFile>;
    type BlockingWriter = HdfsWriter<hdrs::File>;
    type Pager = Option<HdfsPager>;
    type BlockingPager = Option<HdfsPager>;

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Hdfs)
            .set_root(&self.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,
                read_can_seek: true,
                read_with_range: true,

                write: true,
                // TODO: wait for https://github.com/apache/incubator-opendal/pull/2715
                write_can_append: false,

                create_dir: true,
                delete: true,

                list: true,
                list_with_delimiter_slash: true,

                blocking: true,

                ..Default::default()
            });

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let p = build_rooted_abs_path(&self.root, path);

        self.client.create_dir(&p).map_err(parse_io_error)?;

        Ok(RpCreateDir::default())
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        use oio::ReadExt;

        let p = build_rooted_abs_path(&self.root, path);

        // This will be addressed by https://github.com/apache/incubator-opendal/issues/506
        let meta = self.client.metadata(&p).map_err(parse_io_error)?;

        let f = self
            .client
            .open_file()
            .read(true)
            .async_open(&p)
            .await
            .map_err(parse_io_error)?;

        let br = args.range();
        let (start, end) = match (br.offset(), br.size()) {
            // Read a specific range.
            (Some(offset), Some(size)) => (offset, min(offset + size, meta.len())),
            // Read from offset.
            (Some(offset), None) => (offset, meta.len()),
            // Read the last size bytes.
            (None, Some(size)) => (meta.len() - size, meta.len()),
            // Read the whole file.
            (None, None) => (0, meta.len()),
        };

        let mut r = oio::into_read_from_file(f, start, end);
        // Rewind to make sure we are on the correct offset.
        r.seek(SeekFrom::Start(0)).await?;

        Ok((RpRead::new(end - start), r))
    }

    async fn write(&self, path: &str, op: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_rooted_abs_path(&self.root, path);

        let parent = PathBuf::from(&p)
            .parent()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "path should have parent but not, it must be malformed",
                )
                .with_context("input", &p)
            })?
            .to_path_buf();

        self.client
            .create_dir(&parent.to_string_lossy())
            .map_err(parse_io_error)?;

        let mut open_options = self.client.open_file();
        open_options.create(true);
        if op.append() {
            open_options.append(true);
        } else {
            open_options.write(true);
        }

        let f = open_options.async_open(&p).await.map_err(parse_io_error)?;

        Ok((RpWrite::new(), HdfsWriter::new(f)))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = self.client.metadata(&p).map_err(parse_io_error)?;

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
                Err(parse_io_error(err))
            };
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let result = if meta.is_dir() {
            self.client.remove_dir(&p)
        } else {
            self.client.remove_file(&p)
        };

        result.map_err(parse_io_error)?;

        Ok(RpDelete::default())
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = match self.client.read_dir(&p) {
            Ok(f) => f,
            Err(e) => {
                return if e.kind() == io::ErrorKind::NotFound {
                    Ok((RpList::default(), None))
                } else {
                    Err(parse_io_error(e))
                }
            }
        };

        let rd = HdfsPager::new(&self.root, f, args.limit());

        Ok((RpList::default(), Some(rd)))
    }

    fn blocking_create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let p = build_rooted_abs_path(&self.root, path);

        self.client.create_dir(&p).map_err(parse_io_error)?;

        Ok(RpCreateDir::default())
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        use oio::BlockingRead;

        let p = build_rooted_abs_path(&self.root, path);

        // This will be addressed by https://github.com/apache/incubator-opendal/issues/506
        let meta = self.client.metadata(&p).map_err(parse_io_error)?;

        let f = self
            .client
            .open_file()
            .read(true)
            .open(&p)
            .map_err(parse_io_error)?;

        let br = args.range();
        let (start, end) = match (br.offset(), br.size()) {
            // Read a specific range.
            (Some(offset), Some(size)) => (offset, min(offset + size, meta.len())),
            // Read from offset.
            (Some(offset), None) => (offset, meta.len()),
            // Read the last size bytes.
            (None, Some(size)) => (meta.len() - size, meta.len()),
            // Read the whole file.
            (None, None) => (0, meta.len()),
        };

        let mut r = oio::into_read_from_file(f, start, end);
        // Rewind to make sure we are on the correct offset.
        r.seek(SeekFrom::Start(0))?;

        Ok((RpRead::new(end - start), r))
    }

    fn blocking_write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let p = build_rooted_abs_path(&self.root, path);

        let parent = PathBuf::from(&p)
            .parent()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "path should have parent but not, it must be malformed",
                )
                .with_context("input", &p)
            })?
            .to_path_buf();

        self.client
            .create_dir(&parent.to_string_lossy())
            .map_err(parse_io_error)?;

        let f = self
            .client
            .open_file()
            .create(true)
            .write(true)
            .open(&p)
            .map_err(parse_io_error)?;

        Ok((RpWrite::new(), HdfsWriter::new(f)))
    }

    fn blocking_stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = self.client.metadata(&p).map_err(parse_io_error)?;

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
                Err(parse_io_error(err))
            };
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let result = if meta.is_dir() {
            self.client.remove_dir(&p)
        } else {
            self.client.remove_file(&p)
        };

        result.map_err(parse_io_error)?;

        Ok(RpDelete::default())
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = match self.client.read_dir(&p) {
            Ok(f) => f,
            Err(e) => {
                return if e.kind() == io::ErrorKind::NotFound {
                    Ok((RpList::default(), None))
                } else {
                    Err(parse_io_error(e))
                }
            }
        };

        let rd = HdfsPager::new(&self.root, f, args.limit());

        Ok((RpList::default(), Some(rd)))
    }
}
