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

use super::error::*;
use crate::raw::*;
use crate::*;

#[derive(Debug)]
pub struct FsCore {
    pub info: Arc<AccessorInfo>,
    pub root: PathBuf,
    pub atomic_write_dir: Option<PathBuf>,
    pub buf_pool: oio::PooledBuf,
}

impl FsCore {
    // Build write path and ensure the parent dirs created
    pub async fn ensure_write_abs_path(&self, parent: &Path, path: &str) -> Result<PathBuf> {
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

    pub async fn fs_create_dir(&self, path: &str) -> Result<()> {
        let p = self.root.join(path.trim_end_matches('/'));
        tokio::fs::create_dir_all(&p)
            .await
            .map_err(new_std_io_error)?;
        Ok(())
    }

    pub async fn fs_stat(&self, path: &str) -> Result<Metadata> {
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
            .with_last_modified(Timestamp::try_from(
                meta.modified().map_err(new_std_io_error)?,
            )?);
        Ok(m)
    }

    pub async fn fs_read(&self, path: &str, args: &OpRead) -> Result<tokio::fs::File> {
        let p = self.root.join(path.trim_end_matches('/'));

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

        Ok(f)
    }

    pub async fn fs_write(&self, path: &PathBuf, op: &OpWrite) -> Result<tokio::fs::File> {
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

        let f = open_options.open(path).await.map_err(parse_error)?;

        Ok(f)
    }

    /// This function is used to build a tempfile for writing.
    ///
    /// We don't care about the OpWrite since every check should be performed on target path directly.
    pub async fn fs_tempfile_write(
        &self,
        path: &str,
    ) -> Result<(tokio::fs::File, Option<PathBuf>)> {
        let Some(atomic_write_dir) = self.atomic_write_dir.as_ref() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "fs didn't configure atomic_write_dir, but we're still entering the tempfile logic. This might be a bug.",
            ));
        };

        let tmp_path = self
            .ensure_write_abs_path(atomic_write_dir, &build_tmp_path_of(path))
            .await?;

        let mut open_options = tokio::fs::OpenOptions::new();

        // tempfile should always be new file.
        open_options.create_new(true);
        open_options.write(true);
        open_options.truncate(true);

        let f = open_options.open(&tmp_path).await.map_err(parse_error)?;

        Ok((f, Some(tmp_path)))
    }

    pub async fn fs_list(&self, path: &str) -> Result<Option<tokio::fs::ReadDir>> {
        let p = self.root.join(path.trim_end_matches('/'));

        match tokio::fs::read_dir(&p).await {
            Ok(rd) => Ok(Some(rd)),
            Err(e) => {
                match e.kind() {
                    // Return empty list if the directory not found
                    std::io::ErrorKind::NotFound => Ok(None),
                    // If the path is not a directory, return an empty list
                    //
                    // The path could be a file or a symbolic link in this case.
                    // Returning a NotADirectory error to the user isn't helpful; instead,
                    // providing an empty directory is a more user-friendly. In fact, the dir
                    // `path/` does not exist.
                    std::io::ErrorKind::NotADirectory => Ok(None),
                    _ => Err(new_std_io_error(e)),
                }
            }
        }
    }

    pub async fn fs_copy(&self, from: &str, to: &str) -> Result<()> {
        let from = self.root.join(from.trim_end_matches('/'));
        // try to get the metadata of the source file to ensure it exists
        tokio::fs::metadata(&from).await.map_err(new_std_io_error)?;

        let to = self
            .ensure_write_abs_path(&self.root, to.trim_end_matches('/'))
            .await?;

        tokio::fs::copy(from, to).await.map_err(new_std_io_error)?;
        Ok(())
    }

    pub async fn fs_rename(&self, from: &str, to: &str) -> Result<()> {
        let from = self.root.join(from.trim_end_matches('/'));
        tokio::fs::metadata(&from).await.map_err(new_std_io_error)?;

        let to = self
            .ensure_write_abs_path(&self.root, to.trim_end_matches('/'))
            .await?;

        tokio::fs::rename(from, to)
            .await
            .map_err(new_std_io_error)?;
        Ok(())
    }
}
