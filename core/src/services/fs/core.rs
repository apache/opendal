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

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

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
    // Synchronously build write path and ensure the parent dirs created
    pub fn blocking_ensure_write_abs_path(&self, parent: &Path, path: &str) -> Result<PathBuf> {
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
}

#[inline]
pub fn tmp_file_of(path: &str) -> String {
    let name = get_basename(path);
    let uuid = Uuid::new_v4().to_string();

    format!("{name}.{uuid}")
}
