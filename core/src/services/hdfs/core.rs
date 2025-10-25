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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::io;
use std::io::SeekFrom;
use std::sync::Arc;

use crate::raw::*;
use crate::*;

/// HdfsCore contains code that directly interacts with HDFS.
#[derive(Clone)]
pub struct HdfsCore {
    pub info: Arc<AccessorInfo>,
    pub root: String,
    pub atomic_write_dir: Option<String>,
    pub client: Arc<hdrs::Client>,
}

impl Debug for HdfsCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsCore")
            .field("root", &self.root)
            .field("atomic_write_dir", &self.atomic_write_dir)
            .finish_non_exhaustive()
    }
}

impl HdfsCore {
    pub fn hdfs_create_dir(&self, path: &str) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);
        self.client.create_dir(&p).map_err(new_std_io_error)?;
        Ok(())
    }

    pub fn hdfs_stat(&self, path: &str) -> Result<Metadata> {
        let p = build_rooted_abs_path(&self.root, path);
        let meta = self.client.metadata(&p).map_err(new_std_io_error)?;

        let mode = if meta.is_dir() {
            EntryMode::DIR
        } else if meta.is_file() {
            EntryMode::FILE
        } else {
            EntryMode::Unknown
        };
        let mut m = Metadata::new(mode);
        m.set_content_length(meta.len());
        m.set_last_modified(Timestamp::try_from(meta.modified())?);

        Ok(m)
    }

    pub async fn hdfs_read(&self, path: &str, args: &OpRead) -> Result<hdrs::AsyncFile> {
        let p = build_rooted_abs_path(&self.root, path);

        let client = self.client.clone();
        let mut f = client
            .open_file()
            .read(true)
            .async_open(&p)
            .await
            .map_err(new_std_io_error)?;

        if args.range().offset() != 0 {
            use futures::AsyncSeekExt;

            f.seek(SeekFrom::Start(args.range().offset()))
                .await
                .map_err(new_std_io_error)?;
        }

        Ok(f)
    }

    pub async fn hdfs_write(
        &self,
        path: &str,
        op: &OpWrite,
    ) -> Result<(String, Option<String>, hdrs::AsyncFile, bool, u64)> {
        let target_path = build_rooted_abs_path(&self.root, path);
        let mut initial_size = 0;
        let target_exists = match self.client.metadata(&target_path) {
            Ok(meta) => {
                initial_size = meta.len();
                true
            }
            Err(err) => {
                if err.kind() != io::ErrorKind::NotFound {
                    return Err(new_std_io_error(err));
                }
                false
            }
        };

        let should_append = op.append() && target_exists;
        let tmp_path = self.atomic_write_dir.as_ref().and_then(|atomic_write_dir| {
            // If the target file exists, we should append to the end of it directly.
            (!should_append).then_some(build_rooted_abs_path(
                atomic_write_dir,
                &build_tmp_path_of(path),
            ))
        });

        if !target_exists {
            let parent = get_parent(&target_path);
            self.client.create_dir(parent).map_err(new_std_io_error)?;
        }
        if !should_append {
            initial_size = 0;
        }

        let mut open_options = self.client.open_file();
        open_options.create(true);
        if should_append {
            open_options.append(true);
        } else {
            open_options.write(true);
        }

        let f = open_options
            .async_open(tmp_path.as_ref().unwrap_or(&target_path))
            .await
            .map_err(new_std_io_error)?;

        Ok((target_path, tmp_path, f, target_exists, initial_size))
    }

    pub fn hdfs_list(&self, path: &str) -> Result<Option<hdrs::Readdir>> {
        let p = build_rooted_abs_path(&self.root, path);

        match self.client.read_dir(&p) {
            Ok(f) => Ok(Some(f)),
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(new_std_io_error(e))
                }
            }
        }
    }

    pub fn hdfs_rename(&self, from: &str, to: &str) -> Result<()> {
        let from_path = build_rooted_abs_path(&self.root, from);
        self.client.metadata(&from_path).map_err(new_std_io_error)?;

        let to_path = build_rooted_abs_path(&self.root, to);
        let result = self.client.metadata(&to_path);
        match result {
            Err(err) => {
                // Early return if other error happened.
                if err.kind() != io::ErrorKind::NotFound {
                    return Err(new_std_io_error(err));
                }

                let parent = std::path::PathBuf::from(&to_path)
                    .parent()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "path should have parent but not, it must be malformed",
                        )
                        .with_context("input", &to_path)
                    })?
                    .to_path_buf();

                self.client
                    .create_dir(&parent.to_string_lossy())
                    .map_err(new_std_io_error)?;
            }
            Ok(metadata) => {
                if metadata.is_file() {
                    self.client
                        .remove_file(&to_path)
                        .map_err(new_std_io_error)?;
                } else {
                    return Err(Error::new(ErrorKind::IsADirectory, "path should be a file")
                        .with_context("input", &to_path));
                }
            }
        }

        self.client
            .rename_file(&from_path, &to_path)
            .map_err(new_std_io_error)?;

        Ok(())
    }
}
