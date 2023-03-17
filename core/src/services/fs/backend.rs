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
use std::io;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;

use async_compat::Compat;
use async_trait::async_trait;
use log::debug;
use time::OffsetDateTime;
use tokio::fs;
use uuid::Uuid;

use super::error::parse_io_error;
use super::pager::FsPager;
use super::writer::FsWriter;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// POSIX file system support.
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
/// # Configuration
///
/// - `root`: Set the work dir for backend.
///
/// Refer to [`FsBuilder`]'s public API docs for more information.
///
/// # Example
///
/// ## Via Builder
///
/// ```
/// use std::sync::Arc;
///
/// use anyhow::Result;
/// use opendal::services::Fs;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Create fs backend builder.
///     let mut builder = Fs::default();
///     // Set the root for fs, all operations will happen under this root.
///     //
///     // NOTE: the root must be absolute path.
///     builder.root("/tmp");
///
///     // `Accessor` provides the low level APIs, we will use `Operator` normally.
///     let op: Operator = Operator::new(builder)?.finish();
///
///     Ok(())
/// }
/// ```
#[derive(Default, Debug)]
pub struct FsBuilder {
    root: Option<PathBuf>,
    atomic_write_dir: Option<PathBuf>,
    enable_path_check: bool,
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
    pub fn enable_path_check(&mut self) -> &mut Self {
        self.enable_path_check = true;

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
            if e.kind() == io::ErrorKind::NotFound {
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
                if e.kind() == io::ErrorKind::NotFound {
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
            enable_path_check: self.enable_path_check,
        })
    }
}

/// Backend is used to serve `Accessor` support for posix alike fs.
#[derive(Debug, Clone)]
pub struct FsBackend {
    root: PathBuf,
    atomic_write_dir: Option<PathBuf>,
    enable_path_check: bool,
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

        std::fs::create_dir_all(parent).map_err(parse_io_error)?;

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

        fs::create_dir_all(&parent).await.map_err(parse_io_error)?;

        Ok(p)
    }
}

#[async_trait]
impl Accessor for FsBackend {
    type Reader = oio::into_reader::FdReader<Compat<tokio::fs::File>>;
    type BlockingReader = oio::into_blocking_reader::FdReader<std::fs::File>;
    type Writer = FsWriter<tokio::fs::File>;
    type BlockingWriter = FsWriter<std::fs::File>;
    type Pager = Option<FsPager<tokio::fs::ReadDir>>;
    type BlockingPager = Option<FsPager<std::fs::ReadDir>>;

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Fs)
            .set_root(&self.root.to_string_lossy())
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
        let p = self.root.join(path.trim_end_matches('/'));

        if args.mode() == EntryMode::FILE {
            let parent = p
                .parent()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "path should have parent but not, it must be malformed",
                    )
                    .with_context("input", p.to_string_lossy())
                })?
                .to_path_buf();

            fs::create_dir_all(&parent).await.map_err(parse_io_error)?;

            fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&p)
                .await
                .map_err(parse_io_error)?;

            return Ok(RpCreate::default());
        }

        if args.mode() == EntryMode::DIR {
            fs::create_dir_all(&p).await.map_err(parse_io_error)?;

            return Ok(RpCreate::default());
        }

        unreachable!()
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
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        use oio::ReadExt;

        let p = self.root.join(path.trim_end_matches('/'));

        let mut f = fs::OpenOptions::new()
            .read(true)
            .open(&p)
            .await
            .map_err(parse_io_error)?;

        let total_length = if self.enable_path_check {
            // Get fs metadata of file at given path, ensuring it is not a false-positive due to slash normalization.
            let meta = f.metadata().await.map_err(parse_io_error)?;
            if meta.is_dir() != path.ends_with('/') {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "file mode is not match with its path",
                ));
            }
            if meta.is_dir() {
                return Err(Error::new(
                    ErrorKind::IsADirectory,
                    "given path is a directory",
                ));
            }

            meta.len()
        } else {
            use tokio::io::AsyncSeekExt;

            f.seek(SeekFrom::End(0)).await.map_err(parse_io_error)?
        };

        let f = Compat::new(f);

        let br = args.range();
        let (start, end) = match (br.offset(), br.size()) {
            // Read a specific range.
            (Some(offset), Some(size)) => (offset, min(offset + size, total_length)),
            // Read from offset.
            (Some(offset), None) => (offset, total_length),
            // Read the last size bytes.
            (None, Some(size)) => (
                if total_length > size {
                    total_length - size
                } else {
                    0
                },
                total_length,
            ),
            // Read the whole file.
            (None, None) => (0, total_length),
        };

        let mut r = oio::into_reader::from_fd(f, start, end);

        // Rewind to make sure we are on the correct offset.
        r.seek(SeekFrom::Start(0)).await?;

        Ok((RpRead::new(end - start), r))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let (target_path, tmp_path) = if let Some(atomic_write_dir) = &self.atomic_write_dir {
            let target_path = Self::ensure_write_abs_path(&self.root, path).await?;
            let tmp_path =
                Self::ensure_write_abs_path(atomic_write_dir, &tmp_file_of(path)).await?;
            (target_path, Some(tmp_path))
        } else {
            let p = Self::ensure_write_abs_path(&self.root, path).await?;

            (p, None)
        };

        let f = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(tmp_path.as_ref().unwrap_or(&target_path))
            .await
            .map_err(parse_io_error)?;

        Ok((RpWrite::new(), FsWriter::new(target_path, tmp_path, f)))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = self.root.join(path.trim_end_matches('/'));

        let meta = tokio::fs::metadata(&p).await.map_err(parse_io_error)?;

        if self.enable_path_check && meta.is_dir() != path.ends_with('/') {
            return Err(Error::new(
                ErrorKind::NotFound,
                "file mode is not match with its path",
            ));
        }

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
                    .map(OffsetDateTime::from)
                    .map_err(parse_io_error)?,
            );

        Ok(RpStat::new(m))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let p = self.root.join(path.trim_end_matches('/'));

        let meta = tokio::fs::metadata(&p).await;

        match meta {
            Ok(meta) => {
                if meta.is_dir() {
                    fs::remove_dir(&p).await.map_err(parse_io_error)?;
                } else {
                    fs::remove_file(&p).await.map_err(parse_io_error)?;
                }

                Ok(RpDelete::default())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(RpDelete::default()),
            Err(err) => Err(parse_io_error(err)),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let p = self.root.join(path.trim_end_matches('/'));

        let f = match tokio::fs::read_dir(&p).await {
            Ok(rd) => rd,
            Err(e) => {
                return if e.kind() == io::ErrorKind::NotFound {
                    Ok((RpList::default(), None))
                } else {
                    Err(parse_io_error(e))
                };
            }
        };

        let rd = FsPager::new(&self.root, f, args.limit());

        Ok((RpList::default(), Some(rd)))
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let p = self.root.join(path.trim_end_matches('/'));

        if args.mode() == EntryMode::FILE {
            let parent = p
                .parent()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "path should have parent but not, it must be malformed",
                    )
                    .with_context("input", p.to_string_lossy())
                })?
                .to_path_buf();

            std::fs::create_dir_all(parent).map_err(parse_io_error)?;

            std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&p)
                .map_err(parse_io_error)?;

            return Ok(RpCreate::default());
        }

        if args.mode() == EntryMode::DIR {
            std::fs::create_dir_all(&p).map_err(parse_io_error)?;

            return Ok(RpCreate::default());
        }

        unreachable!()
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        use oio::BlockingRead;

        let p = self.root.join(path.trim_end_matches('/'));

        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .open(p)
            .map_err(parse_io_error)?;

        let total_length = if self.enable_path_check {
            // Get fs metadata of file at given path, ensuring it is not a false-positive due to slash normalization.
            let meta = f.metadata().map_err(parse_io_error)?;
            if meta.is_dir() != path.ends_with('/') {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "file mode is not match with its path",
                ));
            }
            if meta.is_dir() {
                return Err(Error::new(
                    ErrorKind::IsADirectory,
                    "given path is a directory",
                ));
            }

            meta.len()
        } else {
            use std::io::Seek;

            f.seek(SeekFrom::End(0)).map_err(parse_io_error)?
        };

        let br = args.range();
        let (start, end) = match (br.offset(), br.size()) {
            // Read a specific range.
            (Some(offset), Some(size)) => (offset, min(offset + size, total_length)),
            // Read from offset.
            (Some(offset), None) => (offset, total_length),
            // Read the last size bytes.
            (None, Some(size)) => (
                if total_length > size {
                    total_length - size
                } else {
                    0
                },
                total_length,
            ),
            // Read the whole file.
            (None, None) => (0, total_length),
        };

        let mut r = oio::into_blocking_reader::from_fd(f, start, end);

        // Rewind to make sure we are on the correct offset.
        r.seek(SeekFrom::Start(0))?;

        Ok((RpRead::new(end - start), r))
    }

    fn blocking_write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let (target_path, tmp_path) = if let Some(atomic_write_dir) = &self.atomic_write_dir {
            let target_path = Self::blocking_ensure_write_abs_path(&self.root, path)?;
            let tmp_path =
                Self::blocking_ensure_write_abs_path(atomic_write_dir, &tmp_file_of(path))?;
            (target_path, Some(tmp_path))
        } else {
            let p = Self::blocking_ensure_write_abs_path(&self.root, path)?;

            (p, None)
        };

        let f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(tmp_path.as_ref().unwrap_or(&target_path))
            .map_err(parse_io_error)?;

        Ok((RpWrite::new(), FsWriter::new(target_path, tmp_path, f)))
    }

    fn blocking_stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = self.root.join(path.trim_end_matches('/'));

        let meta = std::fs::metadata(p).map_err(parse_io_error)?;

        if self.enable_path_check && meta.is_dir() != path.ends_with('/') {
            return Err(Error::new(
                ErrorKind::NotFound,
                "file mode is not match with its path",
            ));
        }

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
                    .map(OffsetDateTime::from)
                    .map_err(parse_io_error)?,
            );

        Ok(RpStat::new(m))
    }

    fn blocking_delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let p = self.root.join(path.trim_end_matches('/'));

        let meta = std::fs::metadata(&p);

        match meta {
            Ok(meta) => {
                if meta.is_dir() {
                    std::fs::remove_dir(&p).map_err(parse_io_error)?;
                } else {
                    std::fs::remove_file(&p).map_err(parse_io_error)?;
                }

                Ok(RpDelete::default())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(RpDelete::default()),
            Err(err) => Err(parse_io_error(err)),
        }
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        let p = self.root.join(path.trim_end_matches('/'));

        let f = match std::fs::read_dir(p) {
            Ok(rd) => rd,
            Err(e) => {
                return if e.kind() == io::ErrorKind::NotFound {
                    Ok((RpList::default(), None))
                } else {
                    Err(parse_io_error(e))
                };
            }
        };

        let rd = FsPager::new(&self.root, f, args.limit());

        Ok((RpList::default(), Some(rd)))
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
