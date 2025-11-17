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
use std::sync::Arc;

use hdfs_native::HdfsError;
use hdfs_native::WriteOptions;

use super::error::parse_hdfs_error;
use crate::raw::*;
use crate::*;

/// HdfsNativeCore contains code that directly interacts with HDFS Native client.
#[derive(Clone)]
pub struct HdfsNativeCore {
    pub info: Arc<AccessorInfo>,
    pub root: String,
    pub client: Arc<hdfs_native::Client>,
    pub enable_append: bool,
}

impl Debug for HdfsNativeCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsNativeCore")
            .field("root", &self.root)
            .field("enable_append", &self.enable_append)
            .finish_non_exhaustive()
    }
}

impl HdfsNativeCore {
    pub async fn hdfs_create_dir(&self, path: &str) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);

        self.client
            .mkdirs(&p, 0o777, true)
            .await
            .map_err(parse_hdfs_error)?;

        Ok(())
    }

    pub async fn hdfs_stat(&self, path: &str) -> Result<Metadata> {
        let p = build_rooted_abs_path(&self.root, path);

        let status: hdfs_native::client::FileStatus = self
            .client
            .get_file_info(&p)
            .await
            .map_err(parse_hdfs_error)?;

        let mode = if status.isdir {
            EntryMode::DIR
        } else {
            EntryMode::FILE
        };

        let mut metadata = Metadata::new(mode);
        metadata
            .set_last_modified(Timestamp::from_millisecond(
                status.modification_time as i64,
            )?)
            .set_content_length(status.length as u64);

        Ok(metadata)
    }

    pub async fn hdfs_read(
        &self,
        path: &str,
        args: &OpRead,
    ) -> Result<(hdfs_native::file::FileReader, u64, u64)> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = self.client.read(&p).await.map_err(parse_hdfs_error)?;

        let offset = args.range().offset();
        let size = args.range().size().unwrap_or(u64::MAX);

        Ok((f, offset, size))
    }

    pub async fn hdfs_write(
        &self,
        path: &str,
        args: &OpWrite,
    ) -> Result<(hdfs_native::file::FileWriter, u64)> {
        let target_path = build_rooted_abs_path(&self.root, path);
        let mut initial_size = 0;

        let target_exists = match self.client.get_file_info(&target_path).await {
            Ok(status) => {
                initial_size = status.length as u64;
                true
            }
            Err(err) => match &err {
                HdfsError::FileNotFound(_) => false,
                _ => return Err(parse_hdfs_error(err)),
            },
        };

        let f = if target_exists {
            if args.append() {
                assert!(self.enable_append, "append is not enabled");
                self.client
                    .append(&target_path)
                    .await
                    .map_err(parse_hdfs_error)?
            } else {
                initial_size = 0;
                self.client
                    .create(&target_path, WriteOptions::default().overwrite(true))
                    .await
                    .map_err(parse_hdfs_error)?
            }
        } else {
            initial_size = 0;
            self.client
                .create(&target_path, WriteOptions::default())
                .await
                .map_err(parse_hdfs_error)?
        };

        Ok((f, initial_size))
    }

    pub async fn hdfs_delete(&self, path: &str) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);

        self.client
            .delete(&p, true)
            .await
            .map_err(parse_hdfs_error)?;

        Ok(())
    }

    pub async fn hdfs_list(&self, path: &str) -> Result<Option<(String, Option<String>)>> {
        let p: String = build_rooted_abs_path(&self.root, path);

        let isdir = match self.client.get_file_info(&p).await {
            Ok(status) => status.isdir,
            Err(err) => {
                return match &err {
                    HdfsError::FileNotFound(_) => Ok(None),
                    _ => Err(parse_hdfs_error(err)),
                };
            }
        };

        let current_path = if isdir {
            if !path.ends_with("/") {
                Some(path.to_string() + "/")
            } else {
                Some(path.to_string())
            }
        } else {
            None
        };

        Ok(Some((p, current_path)))
    }

    pub async fn hdfs_rename(&self, from: &str, to: &str) -> Result<()> {
        let from_path = build_rooted_abs_path(&self.root, from);
        let to_path = build_rooted_abs_path(&self.root, to);

        match self.client.get_file_info(&to_path).await {
            Ok(status) => {
                if status.isdir {
                    return Err(Error::new(ErrorKind::IsADirectory, "path should be a file")
                        .with_context("input", &to_path));
                } else {
                    self.client
                        .delete(&to_path, true)
                        .await
                        .map_err(parse_hdfs_error)?;
                }
            }
            Err(err) => match &err {
                HdfsError::FileNotFound(_) => {
                    self.client
                        .create(&to_path, WriteOptions::default().create_parent(true))
                        .await
                        .map_err(parse_hdfs_error)?;
                }
                _ => return Err(parse_hdfs_error(err)),
            },
        };

        self.client
            .rename(&from_path, &to_path, true)
            .await
            .map_err(parse_hdfs_error)?;

        Ok(())
    }
}
