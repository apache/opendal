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

#[cfg(all(feature = "services-fs-direct-io", target_family = "unix"))]
const O_DIRECT: i32 = 0x4000;

#[derive(Debug)]
pub struct FsCore {
    pub info: Arc<AccessorInfo>,
    pub root: PathBuf,
    pub atomic_write_dir: Option<PathBuf>,
    pub buf_pool: oio::PooledBuf,
    pub direct_io: bool,
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

    /// Open file for reading with optional direct IO
    pub async fn open_read(&self, path: &Path) -> Result<tokio::fs::File> {
        let mut open_options = tokio::fs::OpenOptions::new();
        open_options.read(true);

        self.apply_direct_io_flags(&mut open_options);

        open_options.open(path).await.map_err(new_std_io_error)
    }

    /// Open file for writing with optional direct IO
    pub async fn open_write(
        &self,
        path: &Path,
        create: bool,
        append: bool,
        if_not_exists: bool,
    ) -> Result<tokio::fs::File> {
        let mut open_options = tokio::fs::OpenOptions::new();
        open_options.write(true);

        if if_not_exists {
            open_options.create_new(true);
        } else if create {
            open_options.create(true);
        }

        if append {
            open_options.append(true);
        } else {
            open_options.truncate(true);
        }

        self.apply_direct_io_flags(&mut open_options);

        open_options.open(path).await.map_err(|e| match e.kind() {
            std::io::ErrorKind::AlreadyExists => Error::new(
                ErrorKind::ConditionNotMatch,
                "The file already exists in the filesystem",
            )
            .set_source(e),
            _ => new_std_io_error(e),
        })
    }

    /// Apply direct IO flags to OpenOptions if enabled and supported
    fn apply_direct_io_flags(&self, open_options: &mut tokio::fs::OpenOptions) {
        if self.direct_io {
            #[cfg(all(feature = "services-fs-direct-io", target_family = "unix"))]
            {
                use std::os::unix::fs::OpenOptionsExt;
                open_options.custom_flags(O_DIRECT);
            }
        }
    }
}

#[inline]
pub fn tmp_file_of(path: &str) -> String {
    let name = get_basename(path);
    let uuid = Uuid::new_v4().to_string();

    format!("{name}.{uuid}")
}
