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
use time::OffsetDateTime;

use super::error::parse_io_error;
use super::pager::HdfsPager;
use super::writer::HdfsWriter;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// [Hadoop Distributed File System (HDFS™)](https://hadoop.apache.org/) support.
///
/// A distributed file system that provides high-throughput access to application data.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] list
/// - [ ] ~~scan~~
/// - [ ] ~~presign~~
/// - [x] blocking
///
/// # Differences with webhdfs
///
/// [Webhdfs][crate::services::Webhdfs] is powered by hdfs's RESTful HTTP API.
///
/// # Features
///
/// HDFS support needs to enable feature `services-hdfs`.
///
/// # Configuration
///
/// - `root`: Set the work dir for backend.
/// - `name_node`: Set the name node for backend.
///
/// Refer to [`HdfsBuilder`]'s public API docs for more information.
///
/// # Environment
///
/// HDFS needs some environment set correctly.
///
/// - `JAVA_HOME`: the path to java home, could be found via `java -XshowSettings:properties -version`
/// - `HADOOP_HOME`: the path to hadoop home, opendal relays on this env to discover hadoop jars and set `CLASSPATH` automatically.
///
/// Most of the time, setting `JAVA_HOME` and `HADOOP_HOME` is enough. But there are some edge cases:
///
/// - If meeting errors like the following:
///
/// ```shell
/// error while loading shared libraries: libjvm.so: cannot open shared object file: No such file or directory
/// ```
///
/// Java's lib are not including in pkg-config find path, please set `LD_LIBRARY_PATH`:
///
/// ```shell
/// export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH}
/// ```
///
/// The path of `libjvm.so` could be different, please keep an eye on it.
///
/// - If meeting errors like the following:
///
/// ```shell
/// (unable to get stack trace for java.lang.NoClassDefFoundError exception: ExceptionUtils::getStackTrace error.)
/// ```
///
/// `CLASSPATH` is not set correctly or your hadoop installation is incorrect.
///
/// # Example
///
/// ### Via Builder
///
/// ```no_run
/// use std::sync::Arc;
///
/// use anyhow::Result;
/// use opendal::services::Hdfs;
/// use opendal::Object;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Create fs backend builder.
///     let mut builder = Hdfs::default();
///     // Set the name node for hdfs.
///     builder.name_node("hdfs://127.0.0.1:9000");
///     // Set the root for hdfs, all operations will happen under this root.
///     //
///     // NOTE: the root must be absolute path.
///     builder.root("/tmp");
///
///     // `Accessor` provides the low level APIs, we will use `Operator` normally.
///     let op: Operator = Operator::new(builder)?.finish();
///
///     // Create an object handle to start operation on object.
///     let _: Object = op.object("test_file");
///
///     Ok(())
/// }
/// ```
#[derive(Debug, Default)]
pub struct HdfsBuilder {
    root: Option<String>,
    name_node: Option<String>,
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
}

impl Builder for HdfsBuilder {
    const SCHEME: Scheme = Scheme::Hdfs;
    type Accessor = HdfsBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = HdfsBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("name_node").map(|v| builder.name_node(v));

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

        let client = hdrs::Client::connect(name_node).map_err(parse_io_error)?;

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
    type Reader = oio::into_reader::FdReader<hdrs::AsyncFile>;
    type BlockingReader = oio::into_blocking_reader::FdReader<hdrs::File>;
    type Writer = HdfsWriter<hdrs::AsyncFile>;
    type BlockingWriter = HdfsWriter<hdrs::File>;
    type Pager = Option<HdfsPager>;
    type BlockingPager = Option<HdfsPager>;

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Hdfs)
            .set_root(&self.root)
            .set_capabilities(
                AccessorCapability::Read
                    | AccessorCapability::Write
                    | AccessorCapability::List
                    | AccessorCapability::Blocking,
            )
            .set_hints(AccessorHint::ReadSeekable);

        am
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let p = build_rooted_abs_path(&self.root, path);

        match args.mode() {
            EntryMode::FILE => {
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

                self.client
                    .open_file()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&p)
                    .map_err(parse_io_error)?;

                Ok(RpCreate::default())
            }
            EntryMode::DIR => {
                self.client.create_dir(&p).map_err(parse_io_error)?;

                Ok(RpCreate::default())
            }
            EntryMode::Unknown => unreachable!(),
        }
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

        let mut r = oio::into_reader::from_fd(f, start, end);
        // Rewind to make sure we are on the correct offset.
        r.seek(SeekFrom::Start(0)).await?;

        Ok((RpRead::new(end - start), r))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
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
            .async_open(&p)
            .await
            .map_err(parse_io_error)?;

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
        m.set_last_modified(OffsetDateTime::from(meta.modified()));

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

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let p = build_rooted_abs_path(&self.root, path);

        match args.mode() {
            EntryMode::FILE => {
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

                self.client
                    .open_file()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&p)
                    .map_err(parse_io_error)?;

                Ok(RpCreate::default())
            }
            EntryMode::DIR => {
                self.client.create_dir(&p).map_err(parse_io_error)?;

                Ok(RpCreate::default())
            }
            EntryMode::Unknown => unreachable!(),
        }
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

        let mut r = oio::into_blocking_reader::from_fd(f, start, end);
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
        m.set_last_modified(OffsetDateTime::from(meta.modified()));

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
