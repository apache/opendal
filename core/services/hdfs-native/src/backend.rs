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
use std::sync::Arc;

use log::debug;

use super::HDFS_NATIVE_SCHEME;
use super::config::HDFS_DEFAULT_AUTHORITY;
use super::config::HDFS_SCHEME_PREFIX;
use super::config::HdfsNativeConfig;
use super::config::init_hdfs_config;
use super::core::HdfsNativeCore;
use super::deleter::HdfsNativeDeleter;
use super::error::parse_hdfs_error;
use super::lister::HdfsNativeLister;
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
    /// - `hdfs://namenode1:9000,namenode2:9000`: connect to hdfs ha cluster.
    pub fn name_node(mut self, name_node: &str) -> Self {
        if !name_node.is_empty() {
            // Trim trailing `/` so that we can accept `hdfs://127.0.0.1:9000/`
            self.config.name_node = Some(name_node.trim_end_matches('/').to_string())
        }

        self
    }

    /// Deprecated: HDFS Native append capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "HDFS Native append capability is enabled by default and this option is no longer needed."
    )]
    pub fn enable_append(self, _enable_append: bool) -> Self {
        self
    }

    /// Set other hdfs-native client options of this backend.
    ///
    /// Currently the supported configs refer to (https://github.com/Kimahriman/hdfs-native)
    pub fn options(mut self, options: HashMap<String, String>) -> Self {
        self.config.options = Some(options);
        self
    }
}

impl Builder for HdfsNativeBuilder {
    type Config = HdfsNativeConfig;

    fn build(self) -> Result<impl Service> {
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

        let mut hdfs_config = init_hdfs_config(name_node);
        if let Some(options) = &self.config.options {
            hdfs_config.extend(options.iter().map(|(k, v)| (k.clone(), v.clone())));
        }

        let client = hdfs_native::ClientBuilder::new()
            .with_url(format!("{}{}", HDFS_SCHEME_PREFIX, HDFS_DEFAULT_AUTHORITY))
            .with_config(hdfs_config)
            .build()
            .map_err(parse_hdfs_error)?;

        // need to check if root dir exists, create if not
        Ok(HdfsNativeBackend {
            core: Arc::new(HdfsNativeCore {
                info: ServiceInfo::new(HDFS_NATIVE_SCHEME, &root, ""),
                capability: Capability {
                    stat: true,

                    read: true,

                    write: true,
                    write_can_append: true,

                    create_dir: true,
                    delete: true,

                    list: true,

                    rename: true,

                    shared: true,

                    ..Default::default()
                },
                root,
                client: Arc::new(client),
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

/// Reader returned by this backend.
pub struct HdfsNativeReader {
    core: Arc<HdfsNativeCore>,
    path: String,
}

impl HdfsNativeReader {
    fn new(core: Arc<HdfsNativeCore>, path: &str) -> Self {
        Self {
            core,
            path: path.to_string(),
        }
    }
}

pub struct HdfsNativeReaderHandle {
    file: hdfs_native::file::FileReader,
}

impl HdfsNativeReaderHandle {
    fn new(file: hdfs_native::file::FileReader) -> Self {
        Self { file }
    }
}

impl oio::PositionRead for HdfsNativeReader {
    type Handle = HdfsNativeReaderHandle;

    async fn open(&self) -> Result<Self::Handle> {
        let file = self.core.hdfs_open(&self.path).await?;
        Ok(HdfsNativeReaderHandle::new(file))
    }

    async fn read_at(handle: &Self::Handle, offset: u64, size: usize) -> Result<Buffer> {
        if size == 0 {
            return Ok(Buffer::new());
        }

        let Ok(offset) = usize::try_from(offset) else {
            return Ok(Buffer::new());
        };

        let file_length = handle.file.file_length();
        if offset >= file_length {
            return Ok(Buffer::new());
        }

        let size = size.min(file_length - offset);
        let bytes = handle
            .file
            .read_range(offset, size)
            .await
            .map_err(parse_hdfs_error)?;

        Ok(Buffer::from(bytes))
    }
}

pub struct HdfsNativeLazyWriter {
    core: Arc<HdfsNativeCore>,
    path: String,
    args: OpWrite,
    inner: Option<HdfsNativeWriter>,
}

impl HdfsNativeLazyWriter {
    fn new(core: Arc<HdfsNativeCore>, path: &str, args: OpWrite) -> Self {
        Self {
            core,
            path: path.to_string(),
            args,
            inner: None,
        }
    }

    async fn inner(&mut self) -> Result<&mut HdfsNativeWriter> {
        if self.inner.is_none() {
            let (f, initial_size) = self.core.hdfs_write(&self.path, &self.args).await?;
            self.inner = Some(HdfsNativeWriter::new(f, initial_size));
        }

        Ok(self.inner.as_mut().expect("writer must be initialized"))
    }
}

impl oio::Write for HdfsNativeLazyWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner().await?.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner().await?.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner().await?.abort().await
    }
}

pub struct HdfsNativeLazyLister {
    core: Arc<HdfsNativeCore>,
    path: String,
    inner: Option<Option<HdfsNativeLister>>,
}

impl HdfsNativeLazyLister {
    fn new(core: Arc<HdfsNativeCore>, path: &str) -> Self {
        Self {
            core,
            path: path.to_string(),
            inner: None,
        }
    }
}

impl oio::List for HdfsNativeLazyLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if self.inner.is_none() {
            self.inner = Some(match self.core.hdfs_list(&self.path).await? {
                Some((p, current_path)) => Some(HdfsNativeLister::new(
                    &self.core.root,
                    &self.core.client,
                    &p,
                    current_path,
                )),
                None => None,
            });
        }

        match self.inner.as_mut().expect("lister must be initialized") {
            Some(lister) => lister.next().await,
            None => Ok(None),
        }
    }
}

impl Service for HdfsNativeBackend {
    type Reader = oio::PositionReader<HdfsNativeReader>;
    type Writer = HdfsNativeLazyWriter;
    type Lister = HdfsNativeLazyLister;
    type Deleter = oio::OneShotDeleter<HdfsNativeDeleter>;
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.core.hdfs_create_dir(path).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _args: OpStat) -> Result<RpStat> {
        let m = self.core.hdfs_stat(path).await?;
        Ok(RpStat::new(m))
    }
    fn read(&self, _ctx: &OperationContext, path: &str, _: OpRead) -> Result<Self::Reader> {
        Ok(oio::PositionReader::new(HdfsNativeReader::new(
            self.core.clone(),
            path,
        )))
    }

    fn write(&self, _ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(HdfsNativeLazyWriter::new(self.core.clone(), path, args))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<HdfsNativeDeleter> = {
            Ok(oio::OneShotDeleter::new(HdfsNativeDeleter::new(
                Arc::clone(&self.core),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, _ctx: &OperationContext, path: &str, _args: OpList) -> Result<Self::Lister> {
        Ok(HdfsNativeLazyLister::new(self.core.clone(), path))
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        self.core.hdfs_rename(from, to).await?;
        Ok(RpRename::default())
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}
