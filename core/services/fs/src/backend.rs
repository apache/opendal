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

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use log::debug;

use super::FS_SCHEME;
use super::config::FsConfig;
use super::core::*;
use super::deleter::FsDeleter;
use super::lister::FsLister;
use super::reader::FsReader;
use super::writer::FsLazyWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// POSIX file system support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct FsBuilder {
    pub(super) config: FsConfig,
}

impl FsBuilder {
    /// Set root for backend.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set temp dir for atomic write.
    ///
    /// # Notes
    ///
    /// - Unconditional append writes directly to the target file without
    ///   staging.
    /// - Staged writes with `if_not_exists` publish without replacing an
    ///   existing destination, through `RENAME_NOREPLACE` on Linux and through
    ///   a hard link elsewhere. Both need the staging directory and the
    ///   destination on the same filesystem.
    pub fn atomic_write_dir(mut self, dir: &str) -> Self {
        if !dir.is_empty() {
            self.config.atomic_write_dir = Some(dir.to_string());
        }

        self
    }

    /// Bypass the page cache for file reads and writes on Linux. Defaults to `false`.
    pub fn direct_io(mut self, enabled: bool) -> Self {
        self.config.direct_io = enabled;
        self
    }
}

impl Builder for FsBuilder {
    type Config = FsConfig;

    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", self);

        let direct_io = self.config.direct_io;
        if direct_io && !cfg!(target_os = "linux") {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "direct_io is only supported on Linux",
            ));
        }

        let root = match self.config.root.map(PathBuf::from) {
            Some(root) => Ok(root),
            None => Err(Error::new(
                ErrorKind::ConfigInvalid,
                "root is not specified",
            )),
        }?;
        debug!("backend use root {}", root.to_string_lossy());

        // If root dir is not exist, we must create it.
        if let Err(e) = std::fs::metadata(&root)
            && e.kind() == std::io::ErrorKind::NotFound
        {
            std::fs::create_dir_all(&root).map_err(|e| {
                Error::new(ErrorKind::Unexpected, "create root dir failed")
                    .with_operation("Builder::build")
                    .with_context("root", root.to_string_lossy())
                    .set_source(e)
            })?;
        }

        let atomic_write_dir = self.config.atomic_write_dir.map(PathBuf::from);

        // If atomic write dir is not exist, we must create it.
        if let Some(d) = &atomic_write_dir
            && let Err(e) = std::fs::metadata(d)
            && e.kind() == std::io::ErrorKind::NotFound
        {
            std::fs::create_dir_all(d).map_err(|e| {
                Error::new(ErrorKind::Unexpected, "create atomic write dir failed")
                    .with_operation("Builder::build")
                    .with_context("atomic_write_dir", d.to_string_lossy())
                    .set_source(e)
            })?;
        }

        // Canonicalize the root directory. This should work since we already know that we can
        // get the metadata of the path.
        let root = root.canonicalize().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                "canonicalize of root directory failed",
            )
            .set_source(e)
        })?;

        // Canonicalize the atomic_write_dir directory. This should work since we already know that
        // we can get the metadata of the path.
        let atomic_write_dir = atomic_write_dir
            .map(|p| {
                p.canonicalize().map(Some).map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "canonicalize of atomic_write_dir directory failed",
                    )
                    .with_operation("Builder::build")
                    .with_context("root", root.to_string_lossy())
                    .set_source(e)
                })
            })
            .unwrap_or(Ok(None))?;

        Ok(FsBackend {
            core: Arc::new(FsCore {
                info: ServiceInfo::new(FS_SCHEME, root.to_string_lossy(), ""),
                capability: Capability {
                    stat: true,

                    read: true,
                    read_with_if_modified_since: true,
                    read_with_if_unmodified_since: true,

                    write: true,
                    write_can_empty: true,
                    write_can_append: !direct_io,
                    write_can_multi: true,
                    write_with_if_not_exists: true,
                    #[cfg(unix)]
                    write_with_user_metadata: true,

                    create_dir: true,
                    delete: true,
                    delete_with_recursive: true,

                    list: true,
                    list_with_recursive: true,
                    list_with_start_after: true,

                    copy: true,
                    rename: true,
                    #[cfg(target_os = "linux")]
                    rename_with_if_not_exists: true,

                    shared: true,

                    ..Default::default()
                },
                root,
                atomic_write_dir,
                direct_io,
                buf_pool: oio::PooledBuf::new(16).with_initial_capacity(256 * 1024),
            }),
        })
    }
}

/// FsBackend implements [`Service`] for POSIX-like file systems.
#[derive(Debug, Clone)]
pub struct FsBackend {
    pub(crate) core: Arc<FsCore>,
}

impl Service for FsBackend {
    type Reader = oio::PositionReader<FsReader>;
    type Writer = FsLazyWriter;
    type Lister = FsLister;
    type Deleter = oio::OneShotDeleter<FsDeleter>;
    type Copier = oio::OneShotCopier;
    type Composer = ();

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
        self.core.fs_create_dir(path).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let m = self.core.fs_stat(path).await?;
        Ok(RpStat::new(m))
    }

    fn read(&self, _ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        Ok(oio::PositionReader::new(FsReader::new(
            self.core.clone(),
            path,
            args,
        )))
    }

    fn write(&self, ctx: &OperationContext, path: &str, op: OpWrite) -> Result<Self::Writer> {
        if self.core.direct_io && op.append() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "append is not supported with direct_io",
            ));
        }

        Ok(FsLazyWriter::new(
            self.core.clone(),
            ctx.executor().clone(),
            path,
            op,
        ))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        Ok(oio::OneShotDeleter::new(FsDeleter::new(self.core.clone())))
    }

    fn list(&self, _ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        Ok(FsLister::new(self.core.clone(), path, args))
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        let core = self.core.clone();
        let from = from.to_string();
        let to = to.to_string();
        Ok(oio::OneShotCopier::new(async move {
            let size = core.fs_copy(&from, &to).await?;
            let metadata = MetadataBuilder::file(size);
            Ok(metadata.build())
        }))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.core.fs_rename(from, to, args.if_not_exists()).await?;
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

#[cfg(windows)]
pub(crate) fn read_at(f: &File, buf: &mut [u8], offset: u64) -> Result<usize> {
    use std::os::windows::fs::FileExt;
    f.seek_read(buf, offset).map_err(new_std_io_error)
}

#[cfg(unix)]
pub(crate) fn read_at(f: &File, buf: &mut [u8], offset: u64) -> Result<usize> {
    use std::os::unix::fs::FileExt;
    f.read_at(buf, offset).map_err(new_std_io_error)
}
