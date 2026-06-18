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

use std::io;
use std::sync::Arc;

use log::debug;

use super::HDFS_SCHEME;
use super::config::HdfsConfig;
use super::core::HdfsCore;
use super::deleter::HdfsDeleter;
use super::lister::HdfsLister;
use super::writer::HdfsWriter;
use opendal_core::raw::*;
use opendal_core::*;

#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct HdfsBuilder {
    pub(super) config: HdfsConfig,
}

impl HdfsBuilder {
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
            self.config.name_node = Some(name_node.to_string())
        }

        self
    }

    /// Set kerberos_ticket_cache_path of this backend
    ///
    /// This should be configured when kerberos is enabled.
    pub fn kerberos_ticket_cache_path(mut self, kerberos_ticket_cache_path: &str) -> Self {
        if !kerberos_ticket_cache_path.is_empty() {
            self.config.kerberos_ticket_cache_path = Some(kerberos_ticket_cache_path.to_string())
        }
        self
    }

    /// Set user of this backend
    pub fn user(mut self, user: &str) -> Self {
        if !user.is_empty() {
            self.config.user = Some(user.to_string())
        }
        self
    }

    /// Deprecated: HDFS append capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "HDFS append capability is enabled by default and this option is no longer needed."
    )]
    pub fn enable_append(self, _enable_append: bool) -> Self {
        self
    }

    /// Set temp dir for atomic write.
    ///
    /// # Notes
    ///
    /// - When append is enabled, we will not use atomic write
    ///   to avoid data loss and performance issue.
    pub fn atomic_write_dir(mut self, dir: &str) -> Self {
        self.config.atomic_write_dir = if dir.is_empty() {
            None
        } else {
            Some(String::from(dir))
        };
        self
    }
}

impl Builder for HdfsBuilder {
    type Config = HdfsConfig;

    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", &self);

        let name_node = match &self.config.name_node {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "name node is empty")
                    .with_context("service", HDFS_SCHEME));
            }
        };

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

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
                debug!("root {root} is not exist, creating now");

                client.create_dir(&root).map_err(new_std_io_error)?
            }
        }

        let atomic_write_dir = self.config.atomic_write_dir;

        // If atomic write dir is not exist, we must create it.
        if let Some(d) = &atomic_write_dir {
            if let Err(e) = client.metadata(d) {
                if e.kind() == io::ErrorKind::NotFound {
                    client.create_dir(d).map_err(new_std_io_error)?
                }
            }
        }

        Ok(HdfsBackend {
            core: Arc::new(HdfsCore {
                info: ServiceInfo::new(HDFS_SCHEME, &root, ""),
                capability: Capability {
                    stat: true,

                    read: true,

                    write: true,
                    write_can_append: true,

                    create_dir: true,
                    delete: true,
                    delete_with_recursive: true,

                    list: true,

                    rename: true,

                    shared: true,

                    ..Default::default()
                },
                root,
                atomic_write_dir,
                client: Arc::new(client),
            }),
        })
    }
}

/// Backend for hdfs services.
#[derive(Debug, Clone)]
pub struct HdfsBackend {
    core: Arc<HdfsCore>,
}

/// Reader returned by this backend.
pub struct HdfsReader {
    core: Arc<HdfsCore>,
    path: String,
}

impl HdfsReader {
    fn new(core: Arc<HdfsCore>, path: &str) -> Self {
        Self {
            core,
            path: path.to_string(),
        }
    }
}

pub struct HdfsReaderHandle {
    file: Arc<hdrs::File>,
}

impl HdfsReaderHandle {
    fn new(file: hdrs::File) -> Self {
        Self { file: file.into() }
    }
}

impl oio::PositionRead for HdfsReader {
    type Handle = HdfsReaderHandle;

    async fn open(&self) -> Result<Self::Handle> {
        let file = self.core.hdfs_open(&self.path).await?;
        Ok(HdfsReaderHandle::new(file))
    }

    async fn read_at(handle: &Self::Handle, offset: u64, size: usize) -> Result<Buffer> {
        if size == 0 {
            return Ok(Buffer::new());
        }

        let file = handle.file.clone();
        let buf = tokio::task::spawn_blocking(move || {
            let mut buf = vec![0; size];
            let n = file.read_at(&mut buf, offset).map_err(new_std_io_error)?;
            buf.truncate(n);
            Ok::<_, Error>(Buffer::from(buf))
        })
        .await
        .map_err(|e| Error::new(ErrorKind::Unexpected, "tokio task join failed").set_source(e))??;

        Ok(buf)
    }
}

pub struct HdfsLazyWriter {
    core: Arc<HdfsCore>,
    path: String,
    op: OpWrite,
    inner: Option<HdfsWriter<hdrs::AsyncFile>>,
}

impl HdfsLazyWriter {
    fn new(core: Arc<HdfsCore>, path: &str, op: OpWrite) -> Self {
        Self {
            core,
            path: path.to_string(),
            op,
            inner: None,
        }
    }

    async fn inner(&mut self) -> Result<&mut HdfsWriter<hdrs::AsyncFile>> {
        if self.inner.is_none() {
            let (target_path, tmp_path, f, target_exists, initial_size) =
                self.core.hdfs_write(&self.path, &self.op).await?;

            self.inner = Some(HdfsWriter::new(
                target_path,
                tmp_path,
                f,
                Arc::clone(&self.core.client),
                target_exists,
                initial_size,
            ));
        }

        Ok(self.inner.as_mut().expect("writer must be initialized"))
    }
}

impl oio::Write for HdfsLazyWriter {
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

impl Service for HdfsBackend {
    type Reader = oio::PositionReader<HdfsReader>;
    type Writer = HdfsLazyWriter;
    type Lister = Option<HdfsLister>;
    type Deleter = oio::OneShotDeleter<HdfsDeleter>;
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
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.core.hdfs_create_dir(path)?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let m = self.core.hdfs_stat(path)?;
        Ok(RpStat::new(m))
    }
    fn read(&self, _ctx: &OperationContext, path: &str, _: OpRead) -> Result<Self::Reader> {
        Ok(oio::PositionReader::new(HdfsReader::new(
            self.core.clone(),
            path,
        )))
    }

    fn write(&self, _ctx: &OperationContext, path: &str, op: OpWrite) -> Result<Self::Writer> {
        Ok(HdfsLazyWriter::new(self.core.clone(), path, op))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<HdfsDeleter> = {
            Ok(oio::OneShotDeleter::new(HdfsDeleter::new(Arc::clone(
                &self.core,
            ))))
        }?;

        Ok(output)
    }

    fn list(&self, _ctx: &OperationContext, path: &str, _: OpList) -> Result<Self::Lister> {
        let output: Option<HdfsLister> = {
            match self.core.hdfs_list(path)? {
                Some(f) => {
                    let rd = HdfsLister::new(&self.core.root, f, path);
                    Ok(Some(rd))
                }
                None => Ok(None),
            }
        }?;

        Ok(output)
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
        self.core.hdfs_rename(from, to)?;
        Ok(RpRename::new())
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
