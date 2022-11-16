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
use std::io::ErrorKind;
use std::io::Read;
use std::io::Result;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
use async_compat::Compat;
use async_trait::async_trait;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use log::debug;
use time::OffsetDateTime;
use tokio::fs;

use super::dir_stream::DirStream;
use super::error::parse_io_error;
use crate::accessor::AccessorCapability;
use crate::accessor::AccessorMetadata;
use crate::error::new_other_object_error;
use crate::object::EmptyObjectIterator;
use crate::object::EmptyObjectStreamer;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::Operation;
use crate::path::build_rel_path;
use crate::path::build_rooted_abs_path;
use crate::path::normalize_root;
use crate::Accessor;
use crate::BlockingBytesReader;
use crate::BytesReader;
use crate::ObjectEntry;
use crate::ObjectIterator;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::ObjectReader;
use crate::ObjectStreamer;
use crate::Scheme;

/// Builder for fs backend.
#[derive(Default, Debug)]
pub struct Builder {
    root: Option<String>,
}

impl Builder {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Self {
        let mut builder = Builder::default();

        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
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

    /// Consume current builder to build a fs backend.
    pub fn build(&mut self) -> Result<impl Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        // If root dir is not exist, we must create it.
        if let Err(e) = std::fs::metadata(&root) {
            if e.kind() == ErrorKind::NotFound {
                std::fs::create_dir_all(&root).map_err(|e| {
                    std::io::Error::new(
                        ErrorKind::Other,
                        anyhow!("create dir in {} error {:?}", &root, e),
                    )
                })?;
            }
        }

        debug!("backend build finished: {:?}", &self);
        Ok(Backend { root })
    }
}

/// Backend is used to serve `Accessor` support for posix alike fs.
#[derive(Debug, Clone)]
pub struct Backend {
    root: String,
}

impl Backend {
    // Get fs metadata of file at given path, ensuring it is not a false-positive due to slash normalization.
    #[inline]
    async fn fs_metadata(path: &str) -> Result<std::fs::Metadata> {
        match fs::metadata(&path).await {
            Ok(meta) => {
                if meta.is_dir() != path.ends_with('/') {
                    Err(ErrorKind::NotFound.into())
                } else {
                    Ok(meta)
                }
            }

            Err(e) => Err(e),
        }
    }

    // Synchronously get fs metadata of file at given path, ensuring it is not a false-positive due to slash normalization.
    #[inline]
    fn blocking_fs_metadata(path: &str) -> Result<std::fs::Metadata> {
        match std::fs::metadata(path) {
            Ok(meta) => {
                if meta.is_dir() != path.ends_with('/') {
                    Err(ErrorKind::NotFound.into())
                } else {
                    Ok(meta)
                }
            }

            Err(e) => Err(e),
        }
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

    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);

        if args.mode() == ObjectMode::FILE {
            let parent = PathBuf::from(&p)
                .parent()
                .ok_or_else(|| {
                    new_other_object_error(
                        Operation::Create,
                        path,
                        anyhow!("malformed path: {:?}", path),
                    )
                })?
                .to_path_buf();

            fs::create_dir_all(&parent)
                .await
                .map_err(|e| parse_io_error(e, Operation::Create, &parent.to_string_lossy()))?;

            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&p)
                .await
                .map_err(|e| parse_io_error(e, Operation::Create, path))?;

            return Ok(());
        }

        if args.mode() == ObjectMode::DIR {
            fs::create_dir_all(&p)
                .await
                .map_err(|e| parse_io_error(e, Operation::Create, path))?;

            return Ok(());
        }

        unreachable!()
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<ObjectReader> {
        let p = build_rooted_abs_path(&self.root, path);

        // Validate if input path is a valid file.
        let meta = Self::fs_metadata(&p)
            .await
            .map_err(|e| parse_io_error(e, Operation::Read, path))?;
        if meta.is_dir() {
            return Err(new_other_object_error(
                Operation::Read,
                path,
                anyhow!("Is a directory"),
            ));
        }

        let f = fs::OpenOptions::new()
            .read(true)
            .open(&p)
            .await
            .map_err(|e| parse_io_error(e, Operation::Read, path))?;

        let mut f = Compat::new(f);

        let br = args.range();
        let (r, size): (BytesReader, _) = match (br.offset(), br.size()) {
            // Read a specific range.
            (Some(offset), Some(size)) => {
                f.seek(SeekFrom::Start(offset))
                    .await
                    .map_err(|e| parse_io_error(e, Operation::Read, path))?;
                (Box::new(f.take(size)), min(size, meta.len() - offset))
            }
            // Read from offset.
            (Some(offset), None) => {
                f.seek(SeekFrom::Start(offset))
                    .await
                    .map_err(|e| parse_io_error(e, Operation::Read, path))?;
                (Box::new(f), meta.len() - offset)
            }
            // Read the last size bytes.
            (None, Some(size)) => {
                f.seek(SeekFrom::End(-(size as i64)))
                    .await
                    .map_err(|e| parse_io_error(e, Operation::Read, path))?;
                (Box::new(f), size)
            }
            // Read the whole file.
            (None, None) => (Box::new(f), meta.len()),
        };

        Ok(ObjectReader::new(Box::new(r))
            .with_meta(ObjectMetadata::new(ObjectMode::FILE).with_content_length(size)))
    }

    async fn write(&self, path: &str, _: OpWrite, r: BytesReader) -> Result<u64> {
        let p = build_rooted_abs_path(&self.root, path);

        // Create dir before write path.
        //
        // TODO(xuanwo): There are many works to do here:
        //   - Is it safe to create dir concurrently?
        //   - Do we need to extract this logic as new util functions?
        //   - Is it better to check the parent dir exists before call mkdir?
        let parent = PathBuf::from(&p)
            .parent()
            .ok_or_else(|| {
                new_other_object_error(
                    Operation::Write,
                    path,
                    anyhow!("malformed path: {:?}", path),
                )
            })?
            .to_path_buf();

        fs::create_dir_all(&parent)
            .await
            .map_err(|e| parse_io_error(e, Operation::Write, &parent.to_string_lossy()))?;

        let f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&p)
            .await
            .map_err(|e| parse_io_error(e, Operation::Write, path))?;

        let mut f = Compat::new(f);

        let size = futures::io::copy(r, &mut f).await?;

        Ok(size)
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<ObjectMetadata> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = Self::fs_metadata(&p)
            .await
            .map_err(|e| parse_io_error(e, Operation::Stat, path))?;

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
                    .map_err(|e| parse_io_error(e, Operation::Stat, path))?,
            );

        Ok(m)
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);

        // PathBuf.is_dir() is not free, call metadata directly instead.
        let meta = Self::fs_metadata(&p).await;

        if let Err(err) = meta {
            return if err.kind() == ErrorKind::NotFound {
                Ok(())
            } else {
                Err(parse_io_error(err, Operation::Delete, path))
            };
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let f = if meta.is_dir() {
            fs::remove_dir(&p).await
        } else {
            fs::remove_file(&p).await
        };

        f.map_err(|e| parse_io_error(e, Operation::Delete, path))?;

        Ok(())
    }

    async fn list(&self, path: &str, _: OpList) -> Result<ObjectStreamer> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = match std::fs::read_dir(&p) {
            Ok(rd) => rd,
            Err(e) => {
                return if e.kind() == ErrorKind::NotFound {
                    Ok(Box::new(EmptyObjectStreamer))
                } else {
                    Err(parse_io_error(e, Operation::List, path))
                }
            }
        };

        let rd = DirStream::new(Arc::new(self.clone()), &self.root, path, f);

        Ok(Box::new(rd))
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);

        if args.mode() == ObjectMode::FILE {
            let parent = PathBuf::from(&p)
                .parent()
                .ok_or_else(|| {
                    new_other_object_error(
                        Operation::BlockingCreate,
                        path,
                        anyhow!("malformed path: {:?}", path),
                    )
                })?
                .to_path_buf();

            std::fs::create_dir_all(&parent).map_err(|e| {
                parse_io_error(e, Operation::BlockingCreate, &parent.to_string_lossy())
            })?;

            std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&p)
                .map_err(|e| parse_io_error(e, Operation::BlockingCreate, path))?;

            return Ok(());
        }

        if args.mode() == ObjectMode::DIR {
            std::fs::create_dir_all(&p)
                .map_err(|e| parse_io_error(e, Operation::BlockingCreate, path))?;

            return Ok(());
        }

        unreachable!()
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<BlockingBytesReader> {
        use std::io::Seek;

        let p = build_rooted_abs_path(&self.root, path);

        // Validate if input path is a valid file.
        let meta = Self::blocking_fs_metadata(&p)
            .map_err(|e| parse_io_error(e, Operation::BlockingRead, path))?;
        if meta.is_dir() {
            return Err(new_other_object_error(
                Operation::BlockingRead,
                path,
                anyhow!("Is a directory"),
            ));
        }

        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .open(&p)
            .map_err(|e| parse_io_error(e, Operation::BlockingRead, path))?;

        let br = args.range();
        if let Some(offset) = br.offset() {
            f.seek(SeekFrom::Start(offset))
                .map_err(|e| parse_io_error(e, Operation::BlockingRead, path))?;
        };

        let f: BlockingBytesReader = match br.size() {
            Some(size) => Box::new(f.take(size)),
            None => Box::new(f),
        };

        Ok(f)
    }

    fn blocking_write(&self, path: &str, _: OpWrite, mut r: BlockingBytesReader) -> Result<u64> {
        let p = build_rooted_abs_path(&self.root, path);

        // Create dir before write path.
        //
        // TODO(xuanwo): There are many works to do here:
        //   - Is it safe to create dir concurrently?
        //   - Do we need to extract this logic as new util functions?
        //   - Is it better to check the parent dir exists before call mkdir?
        let parent = PathBuf::from(&p)
            .parent()
            .ok_or_else(|| {
                new_other_object_error(
                    Operation::BlockingWrite,
                    path,
                    anyhow!("malformed path: {:?}", path),
                )
            })?
            .to_path_buf();

        std::fs::create_dir_all(&parent)
            .map_err(|e| parse_io_error(e, Operation::BlockingWrite, &parent.to_string_lossy()))?;

        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&p)
            .map_err(|e| parse_io_error(e, Operation::BlockingWrite, path))?;

        let size = std::io::copy(&mut r, &mut f)?;

        Ok(size)
    }

    fn blocking_stat(&self, path: &str, _: OpStat) -> Result<ObjectMetadata> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = Self::blocking_fs_metadata(&p)
            .map_err(|e| parse_io_error(e, Operation::BlockingStat, path))?;

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
                    .map_err(|e| parse_io_error(e, Operation::BlockingStat, path))?,
            );

        Ok(m)
    }

    fn blocking_delete(&self, path: &str, _: OpDelete) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);

        // PathBuf.is_dir() is not free, call metadata directly instead.
        let meta = Self::blocking_fs_metadata(&p);

        if let Err(err) = meta {
            return if err.kind() == ErrorKind::NotFound {
                Ok(())
            } else {
                Err(parse_io_error(err, Operation::BlockingDelete, path))
            };
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let f = if meta.is_dir() {
            std::fs::remove_dir(&p)
        } else {
            std::fs::remove_file(&p)
        };

        f.map_err(|e| parse_io_error(e, Operation::BlockingDelete, path))?;

        Ok(())
    }

    fn blocking_list(&self, path: &str, _: OpList) -> Result<ObjectIterator> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = match std::fs::read_dir(&p) {
            Ok(rd) => rd,
            Err(e) => {
                return if e.kind() == ErrorKind::NotFound {
                    Ok(Box::new(EmptyObjectIterator))
                } else {
                    Err(parse_io_error(e, Operation::BlockingList, path))
                }
            }
        };

        let acc = Arc::new(self.clone());

        let root = self.root.clone();
        let path = path.to_string();

        let f = f.map(move |v| match v {
            Ok(de) => {
                let path = build_rel_path(&root, &de.path().to_string_lossy());

                // On Windows and most Unix platforms this function is free
                // (no extra system calls needed), but some Unix platforms may
                // require the equivalent call to symlink_metadata to learn about
                // the target file type.
                let file_type = de.file_type()?;

                let d = if file_type.is_file() {
                    ObjectEntry::new(acc.clone(), &path, ObjectMetadata::new(ObjectMode::FILE))
                } else if file_type.is_dir() {
                    // Make sure we are returning the correct path.
                    ObjectEntry::new(
                        acc.clone(),
                        &format!("{}/", &path),
                        ObjectMetadata::new(ObjectMode::DIR),
                    )
                    .with_complete()
                } else {
                    ObjectEntry::new(acc.clone(), &path, ObjectMetadata::new(ObjectMode::Unknown))
                };

                Ok(d)
            }

            Err(err) => Err(parse_io_error(err, Operation::BlockingList, &path)),
        });

        Ok(Box::new(f))
    }
}
