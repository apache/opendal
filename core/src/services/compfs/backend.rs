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

use std::io::Cursor;
use std::sync::Arc;

use compio::dispatcher::Dispatcher;
use compio::fs::OpenOptions;

use super::core::CompfsCore;
use super::delete::CompfsDeleter;
use super::lister::CompfsLister;
use super::reader::CompfsReader;
use super::writer::CompfsWriter;
use crate::raw::oio::OneShotDeleter;
use crate::raw::*;
use crate::services::CompfsConfig;
use crate::*;

impl Configurator for CompfsConfig {
    type Builder = CompfsBuilder;
    fn into_builder(self) -> Self::Builder {
        CompfsBuilder { config: self }
    }
}

/// [`compio`]-based file system support.
#[derive(Debug, Clone, Default)]
pub struct CompfsBuilder {
    config: CompfsConfig,
}

impl CompfsBuilder {
    /// Set root for Compfs
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }
}

impl Builder for CompfsBuilder {
    const SCHEME: Scheme = Scheme::Compfs;
    type Config = CompfsConfig;

    fn build(self) -> Result<impl Access> {
        let root = match self.config.root {
            Some(root) => Ok(root),
            None => Err(Error::new(
                ErrorKind::ConfigInvalid,
                "root is not specified",
            )),
        }?;

        // If root dir does not exist, we must create it.
        if let Err(e) = std::fs::metadata(&root) {
            if e.kind() == std::io::ErrorKind::NotFound {
                std::fs::create_dir_all(&root).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "create root dir failed")
                        .with_operation("Builder::build")
                        .with_context("root", root.as_str())
                        .set_source(e)
                })?;
            }
        }

        let dispatcher = Dispatcher::new().map_err(|_| {
            Error::new(
                ErrorKind::Unexpected,
                "failed to initiate compio dispatcher",
            )
        })?;
        let core = CompfsCore {
            info: {
                let am = AccessorInfo::default();
                am.set_scheme(Scheme::Compfs)
                    .set_root(&root)
                    .set_native_capability(Capability {
                        stat: true,
                        stat_has_last_modified: true,

                        read: true,

                        write: true,
                        write_can_empty: true,
                        write_can_multi: true,
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
            root: root.into(),
            dispatcher,
            buf_pool: oio::PooledBuf::new(16),
        };
        Ok(CompfsBackend {
            core: Arc::new(core),
        })
    }
}

#[derive(Debug)]
pub struct CompfsBackend {
    core: Arc<CompfsCore>,
}

impl Access for CompfsBackend {
    type Reader = CompfsReader;
    type Writer = CompfsWriter;
    type Lister = Option<CompfsLister>;
    type Deleter = OneShotDeleter<CompfsDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let path = self.core.prepare_path(path);

        self.core
            .exec(move || async move { compio::fs::create_dir_all(path).await })
            .await?;

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let path = self.core.prepare_path(path);

        let meta = self
            .core
            .exec(move || async move { compio::fs::metadata(path).await })
            .await?;
        let ty = meta.file_type();
        let mode = if ty.is_dir() {
            EntryMode::DIR
        } else if ty.is_file() {
            EntryMode::FILE
        } else {
            EntryMode::Unknown
        };
        let last_mod = meta.modified().map_err(new_std_io_error)?.into();
        let ret = Metadata::new(mode)
            .with_last_modified(last_mod)
            .with_content_length(meta.len());

        Ok(RpStat::new(ret))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            OneShotDeleter::new(CompfsDeleter::new(self.core.clone())),
        ))
    }

    async fn copy(&self, from: &str, to: &str, _: OpCopy) -> Result<RpCopy> {
        let from = self.core.prepare_path(from);
        let to = self.core.prepare_path(to);

        self.core
            .exec(move || async move {
                let from = OpenOptions::new().read(true).open(from).await?;
                if let Some(parent) = to.parent() {
                    compio::fs::create_dir_all(parent).await?;
                }
                let to = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(to)
                    .await?;

                let (mut from, mut to) = (Cursor::new(from), Cursor::new(to));
                compio::io::copy(&mut from, &mut to).await?;

                Ok(())
            })
            .await?;

        Ok(RpCopy::default())
    }

    async fn rename(&self, from: &str, to: &str, _: OpRename) -> Result<RpRename> {
        let from = self.core.prepare_path(from);
        let to = self.core.prepare_path(to);

        self.core
            .exec(move || async move {
                if let Some(parent) = to.parent() {
                    compio::fs::create_dir_all(parent).await?;
                }
                compio::fs::rename(from, to).await
            })
            .await?;

        Ok(RpRename::default())
    }

    async fn read(&self, path: &str, op: OpRead) -> Result<(RpRead, Self::Reader)> {
        let path = self.core.prepare_path(path);

        let file = self
            .core
            .exec(|| async move { compio::fs::OpenOptions::new().read(true).open(&path).await })
            .await?;

        let r = CompfsReader::new(self.core.clone(), file, op.range());
        Ok((RpRead::new(), r))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let path = self.core.prepare_path(path);
        let append = args.append();
        let file = self
            .core
            .exec(move || async move {
                if let Some(parent) = path.parent() {
                    compio::fs::create_dir_all(parent).await?;
                }
                let file = compio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(!append)
                    .open(path)
                    .await?;
                let mut file = Cursor::new(file);
                if append {
                    let len = file.get_ref().metadata().await?.len();
                    file.set_position(len);
                }
                Ok(file)
            })
            .await?;

        let w = CompfsWriter::new(self.core.clone(), file);
        Ok((RpWrite::new(), w))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let path = self.core.prepare_path(path);

        let read_dir = match self
            .core
            .exec_blocking({
                let path = path.clone();
                move || std::fs::read_dir(path)
            })
            .await?
        {
            Ok(rd) => rd,
            Err(e) => {
                return if e.kind() == std::io::ErrorKind::NotFound {
                    Ok((RpList::default(), None))
                } else {
                    Err(new_std_io_error(e))
                };
            }
        };

        let lister = CompfsLister::new(self.core.clone(), &path, read_dir);
        Ok((RpList::default(), Some(lister)))
    }
}
