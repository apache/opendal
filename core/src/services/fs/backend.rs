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
use std::path::Path;
use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use log::debug;
use uuid::Uuid;

use super::lister::FsLister;
use super::writer::FsWriter;
use crate::raw::*;
use crate::services::fs::reader::FsReader;
use crate::*;

/// POSIX file system support.
#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct FsBuilder {
    root: Option<PathBuf>,
    atomic_write_dir: Option<PathBuf>,
}

impl FsBuilder {
    /// Set root for backend.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(PathBuf::from(root))
        };

        self
    }

    /// Set temp dir for atomic write.
    ///
    /// # Notes
    ///
    /// - When append is enabled, we will not use atomic write
    /// to avoid data loss and performance issue.
    pub fn atomic_write_dir(&mut self, dir: &str) -> &mut Self {
        self.atomic_write_dir = if dir.is_empty() {
            None
        } else {
            Some(PathBuf::from(dir))
        };

        self
    }

    /// OpenDAL requires all input path are normalized to make sure the
    /// behavior is consistent. By enable path check, we can make sure
    /// fs will behave the same as other services.
    ///
    /// Enabling this feature will lead to extra metadata call in all
    /// operations.
    #[deprecated(note = "path always checked since RFC-3243 List Prefix")]
    pub fn enable_path_check(&mut self) -> &mut Self {
        self
    }
}

impl Builder for FsBuilder {
    const SCHEME: Scheme = Scheme::Fs;
    type Accessor = FsBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = FsBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("atomic_write_dir")
            .map(|v| builder.atomic_write_dir(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = match self.root.take() {
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

        let atomic_write_dir = self.atomic_write_dir.take();

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

        debug!("backend build finished: {:?}", &self);
        Ok(FsBackend {
            root,
            atomic_write_dir,
        })
    }
}

/// Backend is used to serve `Accessor` support for posix alike fs.
#[derive(Debug, Clone)]
pub struct FsBackend {
    root: PathBuf,
    atomic_write_dir: Option<PathBuf>,
}

#[inline]
fn tmp_file_of(path: &str) -> String {
    let name = get_basename(path);
    let uuid = Uuid::new_v4().to_string();

    format!("{name}.{uuid}")
}

impl FsBackend {
    // Synchronously build write path and ensure the parent dirs created
    fn blocking_ensure_write_abs_path(parent: &Path, path: &str) -> Result<PathBuf> {
        let p = parent.join(path);

        // Create dir before write path.
        //
        // TODO(xuanwo): There are many works to do here:
        //   - Is it safe to create dir concurrently?
        //   - Do we need to extract this logic as new util functions?
        //   - Is it better to check the parent dir exists before call mkdir?
        let parent = PathBuf::from(&p)
            .parent()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "path should have parent but not, it must be malformed",
                )
                .with_context("input", p.to_string_lossy())
            })?
            .to_path_buf();

        std::fs::create_dir_all(parent).map_err(new_std_io_error)?;

        Ok(p)
    }

    // Build write path and ensure the parent dirs created
    async fn ensure_write_abs_path(parent: &Path, path: &str) -> Result<PathBuf> {
        let p = parent.join(path);

        // Create dir before write path.
        //
        // TODO(xuanwo): There are many works to do here:
        //   - Is it safe to create dir concurrently?
        //   - Do we need to extract this logic as new util functions?
        //   - Is it better to check the parent dir exists before call mkdir?
        let parent = PathBuf::from(&p)
            .parent()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "path should have parent but not, it must be malformed",
                )
                .with_context("input", p.to_string_lossy())
            })?
            .to_path_buf();

        tokio::fs::create_dir_all(&parent)
            .await
            .map_err(new_std_io_error)?;

        Ok(p)
    }
}

#[async_trait]
impl Accessor for FsBackend {
    type Reader = FsReader;
    type Writer = FsWriter<tokio::fs::File>;
    type Lister = Option<FsLister<tokio::fs::ReadDir>>;
    type BlockingReader = FsReader;
    type BlockingWriter = FsWriter<std::fs::File>;
    type BlockingLister = Option<FsLister<std::fs::ReadDir>>;

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Fs)
            .set_root(&self.root.to_string_lossy())
            .set_native_capability(Capability {
                stat: true,

                read: true,
                read_can_seek: true,

                write: true,
                write_can_empty: true,
                write_can_append: true,
                write_can_multi: true,
                create_dir: true,
                delete: true,

                list: true,

                copy: true,
                rename: true,
                blocking: true,

                ..Default::default()
            });

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let p = self.root.join(path.trim_end_matches('/'));

        tokio::fs::create_dir_all(&p)
            .await
            .map_err(new_std_io_error)?;

        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = self.root.join(path.trim_end_matches('/'));

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
    /// - call std::fs::metadata directly and than open. (400ns)
    /// - open file first, and than use `f.metadata()` (300ns)
    /// - open file first, and than use `seek`. (100ns)
    ///
    /// Benchmark could be found [here](https://gist.github.com/Xuanwo/48f9cfbc3022ea5f865388bb62e1a70f)
    async fn read(&self, path: &str, _: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = self.root.join(path.trim_end_matches('/'));

        let f = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&p)
            .await
            .map_err(new_std_io_error)?;

        let r = FsReader::new(f.into_std().await);
        Ok((RpRead::new(), r))
    }

    async fn write(&self, path: &str, op: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let (target_path, tmp_path) = if let Some(atomic_write_dir) = &self.atomic_write_dir {
            let target_path = Self::ensure_write_abs_path(&self.root, path).await?;
            let tmp_path =
                Self::ensure_write_abs_path(atomic_write_dir, &tmp_file_of(path)).await?;

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
            let p = Self::ensure_write_abs_path(&self.root, path).await?;

            (p, None)
        };

        let mut open_options = tokio::fs::OpenOptions::new();
        open_options.create(true).write(true);
        if op.append() {
            open_options.append(true);
        } else {
            open_options.truncate(true);
        }

        let f = open_options
            .open(tmp_path.as_ref().unwrap_or(&target_path))
            .await
            .map_err(new_std_io_error)?;

        Ok((RpWrite::new(), FsWriter::new(target_path, tmp_path, f)))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let p = self.root.join(path.trim_end_matches('/'));

        let meta = tokio::fs::metadata(&p).await;

        match meta {
            Ok(meta) => {
                if meta.is_dir() {
                    tokio::fs::remove_dir(&p).await.map_err(new_std_io_error)?;
                } else {
                    tokio::fs::remove_file(&p).await.map_err(new_std_io_error)?;
                }

                Ok(RpDelete::default())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(RpDelete::default()),
            Err(err) => Err(new_std_io_error(err)),
        }
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let p = self.root.join(path.trim_end_matches('/'));

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

        let rd = FsLister::new(&self.root, f);

        Ok((RpList::default(), Some(rd)))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let from = self.root.join(from.trim_end_matches('/'));

        // try to get the metadata of the source file to ensure it exists
        tokio::fs::metadata(&from).await.map_err(new_std_io_error)?;

        let to = Self::ensure_write_abs_path(&self.root, to.trim_end_matches('/')).await?;

        tokio::fs::copy(from, to).await.map_err(new_std_io_error)?;

        Ok(RpCopy::default())
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let from = self.root.join(from.trim_end_matches('/'));

        // try to get the metadata of the source file to ensure it exists
        tokio::fs::metadata(&from).await.map_err(new_std_io_error)?;

        let to = Self::ensure_write_abs_path(&self.root, to.trim_end_matches('/')).await?;

        tokio::fs::rename(from, to)
            .await
            .map_err(new_std_io_error)?;

        Ok(RpRename::default())
    }

    fn blocking_create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let p = self.root.join(path.trim_end_matches('/'));

        std::fs::create_dir_all(p).map_err(new_std_io_error)?;

        Ok(RpCreateDir::default())
    }

    fn blocking_stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = self.root.join(path.trim_end_matches('/'));

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

    fn blocking_read(&self, path: &str, _: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let p = self.root.join(path.trim_end_matches('/'));

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(p)
            .map_err(new_std_io_error)?;

        let r = FsReader::new(f);
        Ok((RpRead::new(), r))
    }

    fn blocking_write(&self, path: &str, op: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let (target_path, tmp_path) = if let Some(atomic_write_dir) = &self.atomic_write_dir {
            let target_path = Self::blocking_ensure_write_abs_path(&self.root, path)?;
            let tmp_path =
                Self::blocking_ensure_write_abs_path(atomic_write_dir, &tmp_file_of(path))?;

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
            let p = Self::blocking_ensure_write_abs_path(&self.root, path)?;

            (p, None)
        };

        let mut f = std::fs::OpenOptions::new();
        f.create(true).write(true);

        if op.append() {
            f.append(true);
        } else {
            f.truncate(true);
        }

        let f = f
            .open(tmp_path.as_ref().unwrap_or(&target_path))
            .map_err(new_std_io_error)?;

        Ok((RpWrite::new(), FsWriter::new(target_path, tmp_path, f)))
    }

    fn blocking_delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let p = self.root.join(path.trim_end_matches('/'));

        let meta = std::fs::metadata(&p);

        match meta {
            Ok(meta) => {
                if meta.is_dir() {
                    std::fs::remove_dir(&p).map_err(new_std_io_error)?;
                } else {
                    std::fs::remove_file(&p).map_err(new_std_io_error)?;
                }

                Ok(RpDelete::default())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(RpDelete::default()),
            Err(err) => Err(new_std_io_error(err)),
        }
    }

    fn blocking_list(&self, path: &str, _: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let p = self.root.join(path.trim_end_matches('/'));

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

        let rd = FsLister::new(&self.root, f);

        Ok((RpList::default(), Some(rd)))
    }

    fn blocking_copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let from = self.root.join(from.trim_end_matches('/'));

        // try to get the metadata of the source file to ensure it exists
        std::fs::metadata(&from).map_err(new_std_io_error)?;

        let to = Self::blocking_ensure_write_abs_path(&self.root, to.trim_end_matches('/'))?;

        std::fs::copy(from, to).map_err(new_std_io_error)?;

        Ok(RpCopy::default())
    }

    fn blocking_rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let from = self.root.join(from.trim_end_matches('/'));

        // try to get the metadata of the source file to ensure it exists
        std::fs::metadata(&from).map_err(new_std_io_error)?;

        let to = Self::blocking_ensure_write_abs_path(&self.root, to.trim_end_matches('/'))?;

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
