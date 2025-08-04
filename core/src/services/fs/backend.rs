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

use std::path::PathBuf;
use std::sync::Arc;

use log::debug;

use super::core::*;
use super::delete::FsDeleter;
use super::lister::FsLister;
use super::reader::FsReader;
use super::writer::FsWriter;
use super::writer::FsWriters;
use super::DEFAULT_SCHEME;
use crate::raw::*;
use crate::services::FsConfig;
use crate::*;
impl Configurator for FsConfig {
    type Builder = FsBuilder;
    fn into_builder(self) -> Self::Builder {
        FsBuilder { config: self }
    }
}

/// POSIX file system support.
#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct FsBuilder {
    config: FsConfig,
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

    fn build(self) -> Result<impl Access> {
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
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(DEFAULT_SCHEME)
                        .set_root(&root.to_string_lossy())
                        .set_native_capability(Capability {
                            stat: true,

                            read: true,

                            write: true,
                            write_can_empty: true,
                            write_can_append: true,
                            write_can_multi: true,
                            write_with_if_not_exists: true,

                            create_dir: true,
                            delete: true,

                            list: true,

                            copy: true,
                            rename: true,

                            shared: true,

                            ..Default::default()
                        });

                    am.into()
                },
                root,
                atomic_write_dir,
                buf_pool: oio::PooledBuf::new(16).with_initial_capacity(256 * 1024),
            }),
        })
    }
}

/// Backend is used to serve `Accessor` support for posix-like fs.
#[derive(Debug, Clone)]
pub struct FsBackend {
    core: Arc<FsCore>,
}

impl Access for FsBackend {
    type Reader = FsReader<tokio::fs::File>;
    type Writer = FsWriters;
    type Lister = Option<FsLister<tokio::fs::ReadDir>>;
    type Deleter = oio::OneShotDeleter<FsDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.core.fs_create_dir(path).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let m = self.core.fs_stat(path).await?;
        Ok(RpStat::new(m))
    }

    /// # Notes
    ///
    /// There are three ways to get the total file length:
    ///
    /// - call std::fs::metadata directly and then open. (400ns)
    /// - open file first, and then use `f.metadata()` (300ns)
    /// - open file first, and then use `seek`. (100ns)
    ///
    /// Benchmark could be found [here](https://gist.github.com/Xuanwo/48f9cfbc3022ea5f865388bb62e1a70f)
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let f = self.core.fs_read(path, &args).await?;
        let r = FsReader::new(
            self.core.clone(),
            f,
            args.range().size().unwrap_or(u64::MAX) as _,
        );
        Ok((RpRead::new(), r))
    }

    async fn write(&self, path: &str, op: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let is_append = op.append();
        let concurrent = op.concurrent();

        let writer = FsWriter::create(self.core.clone(), path, op).await?;

        let writer = if is_append {
            FsWriters::One(writer)
        } else {
            FsWriters::Two(oio::PositionWriter::new(
                self.info().clone(),
                writer,
                concurrent,
            ))
        };

        Ok((RpWrite::default(), writer))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(FsDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        match self.core.fs_list(path).await? {
            Some(f) => {
                let rd = FsLister::new(&self.core.root, path, f);
                Ok((RpList::default(), Some(rd)))
            }
            None => Ok((RpList::default(), None)),
        }
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        self.core.fs_copy(from, to).await?;
        Ok(RpCopy::default())
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        self.core.fs_rename(from, to).await?;
        Ok(RpRename::default())
    }
}
