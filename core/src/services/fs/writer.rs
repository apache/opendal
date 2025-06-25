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

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Buf;
use tokio::io::AsyncWriteExt;

use crate::raw::*;
use crate::services::fs::core::FsCore;
use crate::*;

pub type FsWriters = TwoWays<FsWriter, oio::PositionWriter<FsWriter>>;

pub struct FsWriter {
    target_path: PathBuf,
    /// The temp_path is used to specify whether we should move to target_path after the file has been closed.
    temp_path: Option<PathBuf>,
    f: tokio::fs::File,
}

impl FsWriter {
    pub async fn create(core: Arc<FsCore>, path: &str, op: OpWrite) -> Result<Self> {
        let target_path = core.ensure_write_abs_path(&core.root, path).await?;

        // Create a target file using our OpWrite to check for permissions and existence.
        //
        // If target check passed, we can go decide which path we should go for writing.
        let target_file = core.fs_write(&target_path, &op).await?;

        // file is created success with append.
        let is_append = op.append();
        // file is created success with if_not_exists.
        let is_exist = !op.if_not_exists();

        let (mut f, mut temp_path) = (target_file, None);
        if core.atomic_write_dir.is_some() {
            // The only case we allow write in place is the file
            // exists and users request for append writing.
            if !(is_append && is_exist) {
                (f, temp_path) = core.fs_tempfile_write(path).await?;
            }
        }

        Ok(Self {
            target_path,
            temp_path,
            f,
        })
    }
}

/// # Safety
///
/// We will only take `&mut Self` reference for FsWriter.
unsafe impl Sync for FsWriter {}

impl oio::Write for FsWriter {
    async fn write(&mut self, mut bs: Buffer) -> Result<()> {
        while bs.has_remaining() {
            let n = self.f.write(bs.chunk()).await.map_err(new_std_io_error)?;
            bs.advance(n);
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.f.flush().await.map_err(new_std_io_error)?;
        self.f.sync_all().await.map_err(new_std_io_error)?;

        if let Some(temp_path) = &self.temp_path {
            tokio::fs::rename(temp_path, &self.target_path)
                .await
                .map_err(new_std_io_error)?;
        }

        let file_meta = self.f.metadata().await.map_err(new_std_io_error)?;
        let meta = Metadata::new(EntryMode::FILE)
            .with_content_length(file_meta.len())
            .with_last_modified(file_meta.modified().map_err(new_std_io_error)?.into());
        Ok(meta)
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(temp_path) = &self.temp_path {
            tokio::fs::remove_file(temp_path)
                .await
                .map_err(new_std_io_error)
        } else {
            Err(Error::new(
                ErrorKind::Unsupported,
                "Fs doesn't support abort if atomic_write_dir is not set",
            ))
        }
    }
}

impl oio::PositionWrite for FsWriter {
    async fn write_all_at(&self, offset: u64, buf: Buffer) -> Result<()> {
        let f = self
            .f
            .try_clone()
            .await
            .map_err(new_std_io_error)?
            .into_std()
            .await;

        tokio::task::spawn_blocking(move || {
            let mut buf = buf;
            let mut offset = offset;
            while !buf.is_empty() {
                match write_at(&f, buf.chunk(), offset) {
                    Ok(n) => {
                        buf.advance(n);
                        offset += n as u64
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        })
        .await
        .map_err(new_task_join_error)?
    }

    async fn close(&self) -> Result<Metadata> {
        let mut f = self
            .f
            .try_clone()
            .await
            .map_err(new_std_io_error)?
            .into_std()
            .await;

        f.flush().map_err(new_std_io_error)?;
        f.sync_all().map_err(new_std_io_error)?;

        if let Some(temp_path) = &self.temp_path {
            tokio::fs::rename(temp_path, &self.target_path)
                .await
                .map_err(new_std_io_error)?;
        }

        let file_meta = f.metadata().map_err(new_std_io_error)?;
        let mode = if file_meta.is_file() {
            EntryMode::FILE
        } else if file_meta.is_dir() {
            EntryMode::DIR
        } else {
            EntryMode::Unknown
        };
        let meta = Metadata::new(mode)
            .with_content_length(file_meta.len())
            .with_last_modified(file_meta.modified().map_err(new_std_io_error)?.into());
        Ok(meta)
    }

    async fn abort(&self) -> Result<()> {
        if let Some(temp_path) = &self.temp_path {
            tokio::fs::remove_file(temp_path)
                .await
                .map_err(new_std_io_error)
        } else {
            Err(Error::new(
                ErrorKind::Unsupported,
                "Fs doesn't support abort if atomic_write_dir is not set",
            ))
        }
    }
}

#[cfg(windows)]
fn write_at(f: &File, buf: &[u8], offset: u64) -> Result<usize> {
    use std::os::windows::fs::FileExt;
    f.seek_write(buf, offset).map_err(new_std_io_error)
}

#[cfg(unix)]
fn write_at(f: &File, buf: &[u8], offset: u64) -> Result<usize> {
    use std::os::unix::fs::FileExt;
    f.write_at(buf, offset).map_err(new_std_io_error)
}
