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
use super::writer::FsWriter;
use super::writer::FsWriters;
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
    /// - When append is enabled, we will not use atomic write
    ///   to avoid data loss and performance issue.
    pub fn atomic_write_dir(mut self, dir: &str) -> Self {
        if !dir.is_empty() {
            self.config.atomic_write_dir = Some(dir.to_string());
        }

        self
    }
}

impl Builder for FsBuilder {
    type Config = FsConfig;

    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", &self);

        let root = match self.config.root.map(PathBuf::from) {
            Some(root) => Ok(root),
            None => Err(Error::new(
                ErrorKind::ConfigInvalid,
                "root is not specified",
            )),
        }?;
        debug!("backend use root {}", root.to_string_lossy());

        // If root dir is not exist, we must create it.
        if let Err(e) = std::fs::metadata(&root) {
            if e.kind() == std::io::ErrorKind::NotFound {
                std::fs::create_dir_all(&root).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "create root dir failed")
                        .with_operation("Builder::build")
                        .with_context("root", root.to_string_lossy())
                        .set_source(e)
                })?;
            }
        }

        let atomic_write_dir = self.config.atomic_write_dir.map(PathBuf::from);

        // If atomic write dir is not exist, we must create it.
        if let Some(d) = &atomic_write_dir {
            if let Err(e) = std::fs::metadata(d) {
                if e.kind() == std::io::ErrorKind::NotFound {
                    std::fs::create_dir_all(d).map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "create atomic write dir failed")
                            .with_operation("Builder::build")
                            .with_context("atomic_write_dir", d.to_string_lossy())
                            .set_source(e)
                    })?;
                }
            }
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

                    write: true,
                    write_can_empty: true,
                    write_can_append: true,
                    write_can_multi: true,
                    write_with_if_not_exists: true,
                    #[cfg(unix)]
                    write_with_user_metadata: true,

                    create_dir: true,
                    delete: true,
                    delete_with_recursive: true,

                    list: true,

                    copy: true,
                    rename: true,

                    shared: true,

                    ..Default::default()
                },
                root,
                atomic_write_dir,
                buf_pool: oio::PooledBuf::new(16).with_initial_capacity(256 * 1024),
            }),
        })
    }
}

/// FsBackend implements [`Service`] for POSIX-like file systems.
#[derive(Debug, Clone)]
pub struct FsBackend {
    core: Arc<FsCore>,
}

/// Reader returned by this backend.
pub struct FsReader {
    core: Arc<FsCore>,
    file: Arc<File>,
}

impl FsReader {
    fn new(core: Arc<FsCore>, file: File) -> Self {
        Self {
            core,
            file: file.into(),
        }
    }
}

impl oio::PositionRead for FsReader {
    async fn read_at(&self, offset: u64, size: usize) -> Result<Buffer> {
        if size == 0 {
            return Ok(Buffer::new());
        }

        let mut bs = self.core.buf_pool.get();
        bs.resize(size, 0);

        let f = self.file.clone();
        let (n, mut bs) = tokio::task::spawn_blocking(move || {
            let n = read_at(&f, &mut bs, offset)?;
            Ok::<_, Error>((n, bs))
        })
        .await
        .map_err(new_task_join_error)??;

        let frozen = bs.split_to(n).freeze();
        self.core.buf_pool.put(bs);

        Ok(Buffer::from(frozen))
    }
}

impl Service for FsBackend {
    type Reader = oio::PositionReader<FsReader>;
    type Writer = FsWriters;
    type Lister = Option<FsLister<tokio::fs::ReadDir>>;
    type Deleter = oio::OneShotDeleter<FsDeleter>;
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
        self.core.fs_create_dir(path).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let m = self.core.fs_stat(path).await?;
        Ok(RpStat::new(m))
    }

    async fn read(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        let (rp, output): (_, oio::PositionReader<FsReader>) = {
            let file = self.core.fs_open(path).await?;
            Ok((
                RpRead::default(),
                oio::PositionReader::new(FsReader::new(self.core.clone(), file)),
            ))
        }?;

        Ok((rp, output))
    }

    async fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        op: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        let (rp, output): (_, FsWriters) = {
            let is_append = op.append();
            let concurrent = op.concurrent();

            let writer = FsWriter::create(self.core.clone(), path, op).await?;

            let writer = if is_append {
                FsWriters::One(writer)
            } else {
                FsWriters::Two(oio::PositionWriter::new(
                    ctx.executor().clone(),
                    writer,
                    concurrent,
                ))
            };

            Ok((RpWrite::default(), writer))
        }?;

        Ok((rp, output))
    }

    async fn delete(&self, _ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, output): (_, oio::OneShotDeleter<FsDeleter>) = {
            Ok((
                RpDelete::default(),
                oio::OneShotDeleter::new(FsDeleter::new(self.core.clone())),
            ))
        }?;

        Ok((rp, output))
    }

    async fn list(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        let (rp, output): (_, Option<FsLister<tokio::fs::ReadDir>>) = {
            match self.core.fs_list(path).await? {
                Some(f) => {
                    let rd = FsLister::new(&self.core.root, path, f);
                    Ok((RpList::default(), Some(rd)))
                }
                None => Ok((RpList::default(), None)),
            }
        }?;

        Ok((rp, output))
    }

    async fn copy(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
        let (rp, output): (_, ()) = {
            self.core.fs_copy(from, to).await?;
            Ok((RpCopy::default(), ()))
        }?;

        Ok((rp, output))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        self.core.fs_rename(from, to).await?;
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
fn read_at(f: &File, buf: &mut [u8], offset: u64) -> Result<usize> {
    use std::os::windows::fs::FileExt;
    f.seek_read(buf, offset).map_err(new_std_io_error)
}

#[cfg(unix)]
fn read_at(f: &File, buf: &mut [u8], offset: u64) -> Result<usize> {
    use std::os::unix::fs::FileExt;
    f.read_at(buf, offset).map_err(new_std_io_error)
}
