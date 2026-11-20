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

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Buf;
use opendal_core::raw::*;
use opendal_core::*;

use crate::backend::Hf3fsCore;
use crate::driver::{IoDriver, RegisteredFile};

pub(crate) struct Hf3fsWriter {
    core: Arc<Hf3fsCore>,
    path: PathBuf,
    executor: Executor,
    options: OpWrite,
    inner: Option<oio::PositionWriter<Hf3fsWriteHandle>>,
}

impl Hf3fsWriter {
    pub fn new(core: Arc<Hf3fsCore>, path: PathBuf, executor: Executor, options: OpWrite) -> Self {
        Self {
            core,
            path,
            executor,
            options,
            inner: None,
        }
    }

    async fn inner(&mut self) -> Result<&mut oio::PositionWriter<Hf3fsWriteHandle>> {
        if self.inner.is_none() {
            let path = self.path.clone();
            let api = self.core.api.clone();
            let exclusive = self.options.if_not_exists();
            let file = tokio::task::spawn_blocking(move || {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(new_std_io_error)?;
                }
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .create_new(exclusive)
                    .truncate(true)
                    .open(path)
                    .map_err(|e| {
                        if exclusive && e.kind() == std::io::ErrorKind::AlreadyExists {
                            Error::new(ErrorKind::ConditionNotMatch, "file already exists")
                                .set_source(e)
                        } else {
                            new_std_io_error(e)
                        }
                    })?;
                RegisteredFile::new(api, file)
            })
            .await
            .map_err(new_task_join_error)??;
            self.inner = Some(oio::PositionWriter::new(
                self.executor.clone(),
                Hf3fsWriteHandle {
                    file,
                    driver: self.core.write.clone(),
                },
                self.options.concurrent(),
            ));
        }
        Ok(self.inner.as_mut().expect("writer initialized"))
    }
}

impl oio::Write for Hf3fsWriter {
    async fn write(&mut self, buffer: Buffer) -> Result<()> {
        self.inner().await?.write(buffer).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner().await?.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "3FS writes cannot be rolled back",
        ))
    }
}

struct Hf3fsWriteHandle {
    file: Arc<RegisteredFile>,
    driver: IoDriver,
}

impl oio::PositionWrite for Hf3fsWriteHandle {
    async fn write_all_at(&self, mut offset: u64, mut buffer: Buffer) -> Result<()> {
        while !buffer.is_empty() {
            let size = buffer.len().min(self.driver.block_size);
            let (written, _) = self
                .driver
                .submit(self.file.clone(), offset, size, Some(buffer.slice(..size)))
                .await?;
            if written == 0 {
                return Err(new_std_io_error(std::io::ErrorKind::WriteZero.into()));
            }
            offset += written as u64;
            buffer.advance(written);
        }
        Ok(())
    }

    async fn close(&self, _: u64) -> Result<Metadata> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || {
            file.file.sync_all().map_err(new_std_io_error)?;
            let stat = file.file.metadata().map_err(new_std_io_error)?;
            let mut metadata = MetadataBuilder::file(stat.len());
            metadata.last_modified(Timestamp::try_from(
                stat.modified().map_err(new_std_io_error)?,
            )?);
            Ok(metadata.build())
        })
        .await
        .map_err(new_task_join_error)?
    }

    async fn abort(&self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "3FS writes cannot be rolled back",
        ))
    }
}
