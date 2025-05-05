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

use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::DateTime;
use log::debug;

use super::core::*;
use super::delete::FsDeleter;
use super::lister::FsLister;
use super::reader::FsReader;
use super::writer::FsWriter;
use super::writer::FsWriters;
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
    const SCHEME: Scheme = Scheme::Fs;
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
            .with_operation("Builder::build")
            .with_context("root", root.to_string_lossy())
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
                    am.set_scheme(Scheme::Fs)
                        .set_root(&root.to_string_lossy())
                        .set_native_capability(Capability {
                            stat: true,
                            stat_has_content_length: true,
                            stat_has_last_modified: true,

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
                            blocking: true,

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
    type BlockingReader = FsReader<std::fs::File>;
    type BlockingWriter = FsWriter<std::fs::File>;
    type BlockingLister = Option<FsLister<std::fs::ReadDir>>;
    type BlockingDeleter = oio::OneShotDeleter<FsDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let p = self.core.root.join(path.trim_end_matches('/'));

        tokio::fs::create_dir_all(&p)
            .await
            .map_err(new_std_io_error)?;

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = self.core.root.join(path.trim_end_matches('/'));

        let meta = tokio::fs::metadata(&p).await.map_err(new_std_io_error)?;

        let mode = if meta.is_dir() {
            EntryMode::DIR
        } else if meta.is_file() {
            EntryMode::FILE
        } else {
            EntryMode::Unknown
        };
        let m = Metadata::new(mode)
            .with_content_length(meta.len())
            .with_last_modified(
                meta.modified()
                    .map(DateTime::from)
                    .map_err(new_std_io_error)?,
            );

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
        let p = self.core.root.join(path.trim_end_matches('/'));

        let mut f = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&p)
            .await
            .map_err(new_std_io_error)?;

        if args.range().offset() != 0 {
            use tokio::io::AsyncSeekExt;

            f.seek(SeekFrom::Start(args.range().offset()))
                .await
                .map_err(new_std_io_error)?;
        }

        let r = FsReader::new(
            self.core.clone(),
            f,
            args.range().size().unwrap_or(u64::MAX) as _,
        );
        Ok((RpRead::new(), r))
    }

    async fn write(&self, path: &str, op: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let (target_path, tmp_path) = if let Some(atomic_write_dir) = &self.core.atomic_write_dir {
            let target_path = self
                .core
                .ensure_write_abs_path(&self.core.root, path)
                .await?;
            let tmp_path = self
                .core
                .ensure_write_abs_path(atomic_write_dir, &tmp_file_of(path))
                .await?;

            // If the target file exists, we should append to the end of it directly.
            if op.append()
                && tokio::fs::try_exists(&target_path)
                    .await
                    .map_err(new_std_io_error)?
            {
                (target_path, None)
            } else {
                (target_path, Some(tmp_path))
            }
        } else {
            let p = self
                .core
                .ensure_write_abs_path(&self.core.root, path)
                .await?;

            (p, None)
        };

        let mut open_options = tokio::fs::OpenOptions::new();
        if op.if_not_exists() {
            open_options.create_new(true);
        } else {
            open_options.create(true);
        }

        open_options.write(true);

        if op.append() {
            open_options.append(true);
        } else {
            open_options.truncate(true);
        }

        let f = open_options
            .open(tmp_path.as_ref().unwrap_or(&target_path))
            .await
            .map_err(|e| {
                match e.kind() {
                    std::io::ErrorKind::AlreadyExists => {
                        // Map io AlreadyExists to opendal ConditionNotMatch
                        Error::new(
                            ErrorKind::ConditionNotMatch,
                            "The file already exists in the filesystem",
                        )
                        .set_source(e)
                    }
                    _ => new_std_io_error(e),
                }
            })?;

        let w = FsWriter::new(target_path, tmp_path, f);

        let w = if op.append() {
            FsWriters::One(w)
        } else {
            FsWriters::Two(oio::PositionWriter::new(
                self.info().clone(),
                w,
                op.concurrent(),
            ))
        };

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(FsDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let p = self.core.root.join(path.trim_end_matches('/'));

        let f = match tokio::fs::read_dir(&p).await {
            Ok(rd) => rd,
            Err(e) => {
                return if e.kind() == std::io::ErrorKind::NotFound {
                    Ok((RpList::default(), None))
                } else {
                    Err(new_std_io_error(e))
                };
            }
        };

        let rd = FsLister::new(&self.core.root, path, f);
        Ok((RpList::default(), Some(rd)))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let from = self.core.root.join(from.trim_end_matches('/'));

        // try to get the metadata of the source file to ensure it exists
        tokio::fs::metadata(&from).await.map_err(new_std_io_error)?;

        let to = self
            .core
            .ensure_write_abs_path(&self.core.root, to.trim_end_matches('/'))
            .await?;

        tokio::fs::copy(from, to).await.map_err(new_std_io_error)?;

        Ok(RpCopy::default())
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let from = self.core.root.join(from.trim_end_matches('/'));

        // try to get the metadata of the source file to ensure it exists
        tokio::fs::metadata(&from).await.map_err(new_std_io_error)?;

        let to = self
            .core
            .ensure_write_abs_path(&self.core.root, to.trim_end_matches('/'))
            .await?;

        tokio::fs::rename(from, to)
            .await
            .map_err(new_std_io_error)?;

        Ok(RpRename::default())
    }

    fn blocking_create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let p = self.core.root.join(path.trim_end_matches('/'));

        std::fs::create_dir_all(p).map_err(new_std_io_error)?;

        Ok(RpCreateDir::default())
    }

    fn blocking_stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = self.core.root.join(path.trim_end_matches('/'));

        let meta = std::fs::metadata(p).map_err(new_std_io_error)?;

        let mode = if meta.is_dir() {
            EntryMode::DIR
        } else if meta.is_file() {
            EntryMode::FILE
        } else {
            EntryMode::Unknown
        };
        let m = Metadata::new(mode)
            .with_content_length(meta.len())
            .with_last_modified(
                meta.modified()
                    .map(DateTime::from)
                    .map_err(new_std_io_error)?,
            );

        Ok(RpStat::new(m))
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let p = self.core.root.join(path.trim_end_matches('/'));

        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .open(p)
            .map_err(new_std_io_error)?;

        if args.range().offset() != 0 {
            use std::io::Seek;

            f.seek(SeekFrom::Start(args.range().offset()))
                .map_err(new_std_io_error)?;
        }

        let r = FsReader::new(
            self.core.clone(),
            f,
            args.range().size().unwrap_or(u64::MAX) as _,
        );
        Ok((RpRead::new(), r))
    }

    fn blocking_write(&self, path: &str, op: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let (target_path, tmp_path) = if let Some(atomic_write_dir) = &self.core.atomic_write_dir {
            let target_path = self
                .core
                .blocking_ensure_write_abs_path(&self.core.root, path)?;
            let tmp_path = self
                .core
                .blocking_ensure_write_abs_path(atomic_write_dir, &tmp_file_of(path))?;

            // If the target file exists, we should append to the end of it directly.
            if op.append()
                && Path::new(&target_path)
                    .try_exists()
                    .map_err(new_std_io_error)?
            {
                (target_path, None)
            } else {
                (target_path, Some(tmp_path))
            }
        } else {
            let p = self
                .core
                .blocking_ensure_write_abs_path(&self.core.root, path)?;

            (p, None)
        };

        let mut f = std::fs::OpenOptions::new();

        if op.if_not_exists() {
            f.create_new(true);
        } else {
            f.create(true);
        }

        f.write(true);

        if op.append() {
            f.append(true);
        } else {
            f.truncate(true);
        }

        let f = f
            .open(tmp_path.as_ref().unwrap_or(&target_path))
            .map_err(|e| {
                match e.kind() {
                    std::io::ErrorKind::AlreadyExists => {
                        // Map io AlreadyExists to opendal ConditionNotMatch
                        Error::new(
                            ErrorKind::ConditionNotMatch,
                            "The file already exists in the filesystem",
                        )
                        .set_source(e)
                    }
                    _ => new_std_io_error(e),
                }
            })?;

        Ok((RpWrite::new(), FsWriter::new(target_path, tmp_path, f)))
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(FsDeleter::new(self.core.clone())),
        ))
    }

    fn blocking_list(&self, path: &str, _: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let p = self.core.root.join(path.trim_end_matches('/'));

        let f = match std::fs::read_dir(p) {
            Ok(rd) => rd,
            Err(e) => {
                return if e.kind() == std::io::ErrorKind::NotFound {
                    Ok((RpList::default(), None))
                } else {
                    Err(new_std_io_error(e))
                };
            }
        };

        let rd = FsLister::new(&self.core.root, path, f);
        Ok((RpList::default(), Some(rd)))
    }

    fn blocking_copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let from = self.core.root.join(from.trim_end_matches('/'));

        // try to get the metadata of the source file to ensure it exists
        std::fs::metadata(&from).map_err(new_std_io_error)?;

        let to = self
            .core
            .blocking_ensure_write_abs_path(&self.core.root, to.trim_end_matches('/'))?;

        std::fs::copy(from, to).map_err(new_std_io_error)?;

        Ok(RpCopy::default())
    }

    fn blocking_rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let from = self.core.root.join(from.trim_end_matches('/'));

        // try to get the metadata of the source file to ensure it exists
        std::fs::metadata(&from).map_err(new_std_io_error)?;

        let to = self
            .core
            .blocking_ensure_write_abs_path(&self.core.root, to.trim_end_matches('/'))?;

        std::fs::rename(from, to).map_err(new_std_io_error)?;

        Ok(RpRename::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tmp_file_of() {
        let cases = vec![
            ("hello.txt", "hello.txt"),
            ("/tmp/opendal.log", "opendal.log"),
            ("/abc/def/hello.parquet", "hello.parquet"),
        ];

        for (path, expected_prefix) in cases {
            let tmp_file = tmp_file_of(path);
            assert!(tmp_file.len() > expected_prefix.len());
            assert!(tmp_file.starts_with(expected_prefix));
        }
    }
}
