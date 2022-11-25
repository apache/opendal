// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::min;
use std::io;
use std::io::Read;
use std::io::SeekFrom;
use std::path::PathBuf;

use async_compat::Compat;
use async_trait::async_trait;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use log::debug;
use time::OffsetDateTime;
use tokio::fs;

use super::dir_stream::BlockingDirPager;
use super::dir_stream::DirPager;
use super::error::parse_io_error;
use crate::object::*;
use crate::raw::*;
use crate::*;

/// Builder for fs backend.
#[derive(Default, Debug)]
pub struct Builder {
    root: Option<String>,
    atomic_write_dir: Option<String>,
}

impl Builder {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Self {
        let mut builder = Builder::default();

        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                "atomic_write_dir" => builder.atomic_write_dir(v),
                _ => continue,
            };
        }

        builder
    }

    /// Set root for backend.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set temp dir for atomic write.
    pub fn atomic_write_dir(&mut self, dir: &str) -> &mut Self {
        self.atomic_write_dir = if dir.is_empty() {
            None
        } else {
            Some(dir.to_string())
        };

        self
    }

    /// Consume current builder to build a fs backend.
    pub fn build(&mut self) -> Result<impl Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        let atomic_write_dir = self.atomic_write_dir.as_deref().map(normalize_root);

        // If root dir is not exist, we must create it.
        if let Err(e) = std::fs::metadata(&root) {
            if e.kind() == io::ErrorKind::NotFound {
                std::fs::create_dir_all(&root).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "create root dir failed")
                        .with_operation("Builder::build")
                        .with_context("root", &root)
                        .set_source(e)
                })?;
            }
        }

        // If atomic write dir is not exist, we must create it.
        if let Some(d) = &atomic_write_dir {
            if let Err(e) = std::fs::metadata(d) {
                if e.kind() == io::ErrorKind::NotFound {
                    std::fs::create_dir_all(d).map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "create atomic write dir failed")
                            .with_operation("Builder::build")
                            .with_context("atomic_write_dir", d)
                            .set_source(e)
                    })?;
                }
            }
        }

        debug!("backend build finished: {:?}", &self);
        Ok(apply_wrapper(Backend {
            root,
            atomic_write_dir,
        }))
    }
}

/// Backend is used to serve `Accessor` support for posix alike fs.
#[derive(Debug, Clone)]
pub struct Backend {
    root: String,
    atomic_write_dir: Option<String>,
}

impl Backend {
    // Get fs metadata of file at given path, ensuring it is not a false-positive due to slash normalization.
    #[inline]
    async fn fs_metadata(path: &str) -> Result<std::fs::Metadata> {
        match fs::metadata(&path).await {
            Ok(meta) => {
                if meta.is_dir() != path.ends_with('/') {
                    Err(Error::new(
                        ErrorKind::ObjectNotFound,
                        "file mode is not match with its path",
                    ))
                } else {
                    Ok(meta)
                }
            }

            Err(e) => Err(parse_io_error(e)),
        }
    }

    // Synchronously get fs metadata of file at given path, ensuring it is not a false-positive due to slash normalization.
    #[inline]
    fn blocking_fs_metadata(path: &str) -> Result<std::fs::Metadata> {
        match std::fs::metadata(path) {
            Ok(meta) => {
                if meta.is_dir() != path.ends_with('/') {
                    Err(Error::new(
                        ErrorKind::ObjectNotFound,
                        "filemode is not match with its path",
                    ))
                } else {
                    Ok(meta)
                }
            }

            Err(e) => Err(parse_io_error(e)),
        }
    }

    // Synchronously build write path and ensure the parent dirs created
    fn blocking_ensure_write_abs_path(parent: &str, path: &str) -> Result<String> {
        let p = build_rooted_abs_path(parent, path);

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
                    "path shoud have parent but not, it must be malformed",
                )
                .with_context("input", &p)
            })?
            .to_path_buf();

        std::fs::create_dir_all(&parent).map_err(parse_io_error)?;

        Ok(p)
    }

    // Build write path and ensure the parent dirs created
    async fn ensure_write_abs_path(parent: &str, path: &str) -> Result<String> {
        let p = build_rooted_abs_path(parent, path);

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
                    "path shoud have parent but not, it must be malformed",
                )
                .with_context("input", &p)
            })?
            .to_path_buf();

        fs::create_dir_all(&parent).await.map_err(parse_io_error)?;

        Ok(p)
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Fs)
            .set_root(&self.root)
            .set_capabilities(
                AccessorCapability::Read
                    | AccessorCapability::Write
                    | AccessorCapability::List
                    | AccessorCapability::Blocking,
            );

        am
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let p = build_rooted_abs_path(&self.root, path);

        if args.mode() == ObjectMode::FILE {
            let parent = PathBuf::from(&p)
                .parent()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "path shoud have parent but not, it must be malformed",
                    )
                    .with_context("input", &p)
                })?
                .to_path_buf();

            fs::create_dir_all(&parent).await.map_err(parse_io_error)?;

            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&p)
                .await
                .map_err(parse_io_error)?;

            return Ok(RpCreate::default());
        }

        if args.mode() == ObjectMode::DIR {
            fs::create_dir_all(&p).await.map_err(parse_io_error)?;

            return Ok(RpCreate::default());
        }

        unreachable!()
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, BytesReader)> {
        let p = build_rooted_abs_path(&self.root, path);

        // Validate if input path is a valid file.
        let meta = Self::fs_metadata(&p).await?;
        if meta.is_dir() {
            return Err(Error::new(
                ErrorKind::ObjectIsADirectory,
                "given path is a directoty",
            ));
        }

        let f = fs::OpenOptions::new()
            .read(true)
            .open(&p)
            .await
            .map_err(parse_io_error)?;

        let mut f = Compat::new(f);

        let br = args.range();
        let (r, size): (BytesReader, _) = match (br.offset(), br.size()) {
            // Read a specific range.
            (Some(offset), Some(size)) => {
                f.seek(SeekFrom::Start(offset))
                    .await
                    .map_err(parse_io_error)?;
                (Box::new(f.take(size)), min(size, meta.len() - offset))
            }
            // Read from offset.
            (Some(offset), None) => {
                f.seek(SeekFrom::Start(offset))
                    .await
                    .map_err(parse_io_error)?;
                (Box::new(f), meta.len() - offset)
            }
            // Read the last size bytes.
            (None, Some(size)) => {
                f.seek(SeekFrom::End(-(size as i64)))
                    .await
                    .map_err(parse_io_error)?;
                (Box::new(f), size)
            }
            // Read the whole file.
            (None, None) => (Box::new(f), meta.len()),
        };

        Ok((RpRead::new(size), Box::new(r) as BytesReader))
    }

    async fn write(&self, path: &str, _: OpWrite, r: BytesReader) -> Result<RpWrite> {
        if let Some(atomic_write_dir) = &self.atomic_write_dir {
            let temp_path = Self::ensure_write_abs_path(atomic_write_dir, path).await?;
            let target_path = Self::ensure_write_abs_path(&self.root, path).await?;
            let f = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&temp_path)
                .await
                .map_err(parse_io_error)?;

            let size = {
                // Implicitly flush and close temp file
                let mut f = Compat::new(f);
                futures::io::copy(r, &mut f).await.map_err(parse_io_error)?
            };
            fs::rename(&temp_path, &target_path)
                .await
                .map_err(parse_io_error)?;

            Ok(RpWrite::new(size))
        } else {
            let p = Self::ensure_write_abs_path(&self.root, path).await?;

            let f = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&p)
                .await
                .map_err(parse_io_error)?;

            let mut f = Compat::new(f);

            let size = futures::io::copy(r, &mut f).await.map_err(parse_io_error)?;

            Ok(RpWrite::new(size))
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = Self::fs_metadata(&p).await?;

        let mode = if meta.is_dir() {
            ObjectMode::DIR
        } else if meta.is_file() {
            ObjectMode::FILE
        } else {
            ObjectMode::Unknown
        };
        let m = ObjectMetadata::new(mode)
            .with_content_length(meta.len() as u64)
            .with_last_modified(
                meta.modified()
                    .map(OffsetDateTime::from)
                    .map_err(parse_io_error)?,
            );

        Ok(RpStat::new(m))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let p = build_rooted_abs_path(&self.root, path);

        // PathBuf.is_dir() is not free, call metadata directly instead.
        let meta = Self::fs_metadata(&p).await;

        if let Err(err) = meta {
            return if err.kind() == ErrorKind::ObjectNotFound {
                Ok(RpDelete::default())
            } else {
                Err(err)
            };
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let f = if meta.is_dir() {
            fs::remove_dir(&p).await
        } else {
            fs::remove_file(&p).await
        };

        f.map_err(parse_io_error)?;

        Ok(RpDelete::default())
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, ObjectPager)> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = match tokio::fs::read_dir(&p).await {
            Ok(rd) => rd,
            Err(e) => {
                return if e.kind() == io::ErrorKind::NotFound {
                    Ok((RpList::default(), Box::new(EmptyObjectPager)))
                } else {
                    Err(parse_io_error(e))
                }
            }
        };

        let rd = DirPager::new(&self.root, f);

        Ok((RpList::default(), Box::new(rd)))
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let p = build_rooted_abs_path(&self.root, path);

        if args.mode() == ObjectMode::FILE {
            let parent = PathBuf::from(&p)
                .parent()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "path shoud have parent but not, it must be malformed",
                    )
                    .with_context("input", &p)
                })?
                .to_path_buf();

            std::fs::create_dir_all(&parent).map_err(parse_io_error)?;

            std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&p)
                .map_err(parse_io_error)?;

            return Ok(RpCreate::default());
        }

        if args.mode() == ObjectMode::DIR {
            std::fs::create_dir_all(&p).map_err(parse_io_error)?;

            return Ok(RpCreate::default());
        }

        unreachable!()
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, BlockingBytesReader)> {
        use std::io::Seek;

        let p = build_rooted_abs_path(&self.root, path);

        // Validate if input path is a valid file.
        let meta = Self::blocking_fs_metadata(&p)?;

        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .open(&p)
            .map_err(parse_io_error)?;

        let br = args.range();
        let (r, size): (BlockingBytesReader, _) = match (br.offset(), br.size()) {
            // Read a specific range.
            (Some(offset), Some(size)) => {
                f.seek(SeekFrom::Start(offset)).map_err(parse_io_error)?;
                (Box::new(f.take(size)), min(size, meta.len() - offset))
            }
            // Read from offset.
            (Some(offset), None) => {
                f.seek(SeekFrom::Start(offset)).map_err(parse_io_error)?;
                (Box::new(f), meta.len() - offset)
            }
            // Read the last size bytes.
            (None, Some(size)) => {
                f.seek(SeekFrom::End(-(size as i64)))
                    .map_err(parse_io_error)?;
                (Box::new(f), size)
            }
            // Read the whole file.
            (None, None) => (Box::new(f), meta.len()),
        };

        Ok((RpRead::new(size), Box::new(r) as BlockingBytesReader))
    }

    fn blocking_write(
        &self,
        path: &str,
        _: OpWrite,
        mut r: BlockingBytesReader,
    ) -> Result<RpWrite> {
        if let Some(atomic_write_dir) = &self.atomic_write_dir {
            let temp_path = Self::blocking_ensure_write_abs_path(atomic_write_dir, path)?;
            let target_path = Self::blocking_ensure_write_abs_path(&self.root, path)?;

            let size = {
                // Implicitly flush and close temp file
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&temp_path)
                    .map_err(parse_io_error)?;

                std::io::copy(&mut r, &mut f).map_err(parse_io_error)?
            };
            std::fs::rename(&temp_path, &target_path).map_err(parse_io_error)?;

            Ok(RpWrite::new(size))
        } else {
            let p = Self::blocking_ensure_write_abs_path(&self.root, path)?;

            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&p)
                .map_err(parse_io_error)?;

            let size = std::io::copy(&mut r, &mut f).map_err(parse_io_error)?;

            Ok(RpWrite::new(size))
        }
    }

    fn blocking_stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = Self::blocking_fs_metadata(&p)?;

        let mode = if meta.is_dir() {
            ObjectMode::DIR
        } else if meta.is_file() {
            ObjectMode::FILE
        } else {
            ObjectMode::Unknown
        };
        let m = ObjectMetadata::new(mode)
            .with_content_length(meta.len() as u64)
            .with_last_modified(
                meta.modified()
                    .map(OffsetDateTime::from)
                    .map_err(parse_io_error)?,
            );

        Ok(RpStat::new(m))
    }

    fn blocking_delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let p = build_rooted_abs_path(&self.root, path);

        // PathBuf.is_dir() is not free, call metadata directly instead.
        let meta = Self::blocking_fs_metadata(&p);

        if let Err(err) = meta {
            return if err.kind() == ErrorKind::ObjectNotFound {
                Ok(RpDelete::default())
            } else {
                Err(err)
            };
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let f = if meta.is_dir() {
            std::fs::remove_dir(&p)
        } else {
            std::fs::remove_file(&p)
        };

        f.map_err(parse_io_error)?;

        Ok(RpDelete::default())
    }

    fn blocking_list(&self, path: &str, _: OpList) -> Result<(RpList, BlockingObjectPager)> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = match std::fs::read_dir(&p) {
            Ok(rd) => rd,
            Err(e) => {
                return if e.kind() == io::ErrorKind::NotFound {
                    Ok((
                        RpList::default(),
                        Box::new(EmptyBlockingObjectPager) as BlockingObjectPager,
                    ))
                } else {
                    Err(parse_io_error(e))
                }
            }
        };

        let rd = BlockingDirPager::new(&self.root, f);

        Ok((RpList::default(), Box::new(rd)))
    }
}
