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

#[cfg(unix)]
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Buf;
#[cfg(target_os = "linux")]
use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use super::core::{ErrorContext, FsCore, parse_error, publish_staged_file};
#[cfg(target_os = "linux")]
use super::direct_io::DirectIoAlignment;
use opendal_core::raw::*;
use opendal_core::*;

type FsWriters = TwoWays<FsWriter, oio::PositionWriter<FsWriter>>;

pub struct FsLazyWriter {
    core: Arc<FsCore>,
    executor: Executor,
    path: String,
    op: OpWrite,
    inner: Option<FsWriters>,
    #[cfg(target_os = "linux")]
    direct_buffer: Option<DirectWriteBuffer>,
}

impl FsLazyWriter {
    pub(super) fn new(core: Arc<FsCore>, executor: Executor, path: &str, op: OpWrite) -> Self {
        Self {
            core,
            executor,
            path: path.to_string(),
            op,
            inner: None,
            #[cfg(target_os = "linux")]
            direct_buffer: None,
        }
    }

    async fn open(&mut self) -> Result<()> {
        if self.inner.is_some() {
            return Ok(());
        }

        let writer = FsWriter::create(&self.core, &self.path, &self.op).await?;
        #[cfg(target_os = "linux")]
        if let Some(alignment) = writer.direct_io {
            self.direct_buffer = Some(DirectWriteBuffer {
                offset_align: alignment.offset_align,
                tail: Bytes::new(),
            });
        }

        self.inner = Some(if self.op.append() {
            FsWriters::One(writer)
        } else {
            FsWriters::Two(oio::PositionWriter::new(
                self.executor.clone(),
                writer,
                self.op.concurrent(),
            ))
        });
        Ok(())
    }
}

impl oio::Write for FsLazyWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.open().await?;
        let inner = self.inner.as_mut().expect("writer must be initialized");
        #[cfg(target_os = "linux")]
        if let Some(direct) = &mut self.direct_buffer {
            return direct.write(inner, bs).await;
        }
        inner.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.open().await?;
        let inner = self.inner.as_mut().expect("writer must be initialized");
        #[cfg(target_os = "linux")]
        if let Some(direct) = &mut self.direct_buffer
            && !direct.tail.is_empty()
        {
            // PositionWriter waits for all write tasks before closing with the
            // logical byte count, while write_all_at pads the aligned tail.
            inner.write(direct.tail.clone().into()).await?;
            direct.tail = Bytes::new();
        }
        inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(inner) = &mut self.inner {
            inner.abort().await?;
        }
        #[cfg(target_os = "linux")]
        if let Some(direct) = &mut self.direct_buffer {
            direct.tail = Bytes::new();
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
struct DirectWriteBuffer {
    offset_align: usize,
    tail: Bytes,
}

#[cfg(target_os = "linux")]
impl DirectWriteBuffer {
    async fn write(&mut self, writer: &mut FsWriters, bs: Buffer) -> Result<()> {
        if bs.is_empty() {
            return Ok(());
        }
        let mut pending: Buffer = if self.tail.is_empty() {
            bs
        } else {
            std::iter::once(self.tail.clone()).chain(bs).collect()
        };
        let aligned_len = pending.len() / self.offset_align * self.offset_align;
        if aligned_len > 0 {
            oio::Write::write(writer, pending.split_to(aligned_len)).await?;
        }
        // Own the small tail without retaining the caller's larger allocations.
        self.tail = pending.to_vec().into();
        Ok(())
    }
}

pub struct FsWriter {
    target_path: PathBuf,
    /// The staging path for a write that publishes the file on close.
    temp_path: Option<PathBuf>,
    if_not_exists: bool,
    f: tokio::fs::File,
    #[cfg(target_os = "linux")]
    direct_io: Option<DirectIoAlignment>,
    /// User metadata to be written to xattr on Unix systems.
    #[cfg(unix)]
    user_metadata: Option<HashMap<String, String>>,
}

impl FsWriter {
    async fn create(core: &FsCore, path: &str, op: &OpWrite) -> Result<Self> {
        let target_path = core.ensure_write_abs_path(&core.root, path).await?;

        #[cfg(unix)]
        let user_metadata = op.user_metadata().map(|metadata| {
            metadata
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect()
        });

        let mut temp_path = None;
        if let Some(atomic_write_dir) = &core.atomic_write_dir {
            // Fail fast on an existing destination instead of staging content
            // that the conditional commit must reject.
            let target_exists = (op.if_not_exists() || op.append())
                && tokio::fs::try_exists(&target_path)
                    .await
                    .map_err(new_std_io_error)?;
            if op.if_not_exists() && target_exists {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "file already exists, doesn't match the condition if_not_exists",
                ));
            }
            // Only an unconditional append may write to an existing file in place.
            let append_in_place = op.append() && target_exists;
            if !append_in_place {
                temp_path = Some(
                    core.ensure_write_abs_path(atomic_write_dir, &build_tmp_path_of(path))
                        .await?,
                );
            }
        }

        let mut options = OpenOptions::new();
        options.write(true);
        let operation = if temp_path.is_some() {
            options.create_new(true);
            ServiceOperation("OpenTemporaryFile")
        } else {
            options
                .create_new(op.if_not_exists())
                .create(!op.if_not_exists());
            options.append(op.append());
            ServiceOperation("OpenForWrite")
        };
        let open_path = temp_path.as_ref().unwrap_or(&target_path);

        #[cfg(target_os = "linux")]
        if core.direct_io {
            let open_path = open_path.clone();
            let temporary = temp_path.is_some();
            let cleanup_on_error = temporary || op.if_not_exists();
            let (f, alignment) = tokio::task::spawn_blocking(move || {
                let (file, alignment) =
                    DirectIoAlignment::open(&open_path, options, operation, cleanup_on_error)?;
                // Validate O_DIRECT on the opened fd before truncating an overwrite.
                if !temporary {
                    file.set_len(0).map_err(new_std_io_error)?;
                }
                Ok::<_, Error>((file, alignment))
            })
            .await
            .map_err(new_task_join_error)??;
            return Ok(Self {
                target_path,
                temp_path,
                if_not_exists: op.if_not_exists(),
                f: tokio::fs::File::from_std(f),
                direct_io: Some(alignment),
                #[cfg(unix)]
                user_metadata,
            });
        }

        options.truncate(!op.append() || temp_path.is_some());
        let f = tokio::fs::OpenOptions::from(options)
            .open(open_path)
            .await
            .map_err(|err| parse_error(ErrorContext::new(operation), err))?;
        Ok(Self {
            target_path,
            temp_path,
            if_not_exists: op.if_not_exists(),
            f,
            #[cfg(target_os = "linux")]
            direct_io: None,
            #[cfg(unix)]
            user_metadata,
        })
    }

    async fn commit_staged_file(&self) -> Result<()> {
        let Some(temp_path) = &self.temp_path else {
            return Ok(());
        };

        if self.if_not_exists {
            let temp_path = temp_path.clone();
            let target_path = self.target_path.clone();
            return tokio::task::spawn_blocking(move || {
                publish_staged_file(&temp_path, &target_path).map_err(|err| {
                    // The staged file is discarded, so this close cannot be retried.
                    let err = err.set_permanent();
                    match std::fs::remove_file(&temp_path) {
                        Ok(()) => err,
                        Err(cleanup_err) if cleanup_err.kind() == std::io::ErrorKind::NotFound => {
                            err
                        }
                        Err(cleanup_err) => err.with_context("staging_cleanup", cleanup_err),
                    }
                })
            })
            .await
            .map_err(new_task_join_error)?;
        }

        tokio::fs::rename(temp_path, &self.target_path)
            .await
            .map_err(new_std_io_error)
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

        self.commit_staged_file().await?;

        // Write user metadata to xattr on Unix systems.
        #[cfg(unix)]
        if let Some(ref user_metadata) = self.user_metadata {
            FsCore::set_user_metadata(&self.target_path, user_metadata)?;
        }

        let file_meta = self.f.metadata().await.map_err(new_std_io_error)?;
        let mut meta = MetadataBuilder::file(file_meta.len());
        meta.last_modified(Timestamp::try_from(
            file_meta.modified().map_err(new_std_io_error)?,
        )?);
        Ok(meta.build())
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

        #[cfg(target_os = "linux")]
        let direct_io = self.direct_io;
        tokio::task::spawn_blocking(move || {
            #[cfg(target_os = "linux")]
            if let Some(alignment) = direct_io {
                return alignment.write_all_at(&f, offset, buf);
            }
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

    async fn close(&self, _size: u64) -> Result<Metadata> {
        #[cfg(target_os = "linux")]
        if self.direct_io.is_some() {
            self.f.set_len(_size).await.map_err(new_std_io_error)?;
        }

        let mut f = self
            .f
            .try_clone()
            .await
            .map_err(new_std_io_error)?
            .into_std()
            .await;

        f.flush().map_err(new_std_io_error)?;
        f.sync_all().map_err(new_std_io_error)?;

        self.commit_staged_file().await?;

        // Write user metadata to xattr on Unix systems.
        #[cfg(unix)]
        if let Some(ref user_metadata) = self.user_metadata {
            FsCore::set_user_metadata(&self.target_path, user_metadata)?;
        }

        let file_meta = f.metadata().map_err(new_std_io_error)?;
        let mode = if file_meta.is_file() {
            EntryMode::FILE
        } else if file_meta.is_dir() {
            EntryMode::DIR
        } else {
            EntryMode::Unknown
        };
        let mut meta = match mode {
            EntryMode::FILE => MetadataBuilder::file(file_meta.len()),
            EntryMode::DIR => MetadataBuilder::dir(),
            EntryMode::Unknown => MetadataBuilder::unknown(),
        };
        meta.last_modified(Timestamp::try_from(
            file_meta.modified().map_err(new_std_io_error)?,
        )?);
        Ok(meta.build())
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
