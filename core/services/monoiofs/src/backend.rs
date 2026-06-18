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
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use monoio::fs::OpenOptions;
use opendal_core::raw::*;
use opendal_core::*;

use super::config::MonoiofsConfig;
use super::core::BUFFER_SIZE;
use super::core::MonoiofsCore;
use super::deleter::MonoiofsDeleter;
use super::reader::MonoiofsReader;
use super::writer::MonoiofsWriter;

/// File system support via [`monoio`].
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct MonoiofsBuilder {
    pub(super) config: MonoiofsConfig,
}

impl MonoiofsBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };
        self
    }
}

impl Builder for MonoiofsBuilder {
    type Config = MonoiofsConfig;

    fn build(self) -> Result<impl Service> {
        let root = self.config.root.map(PathBuf::from).ok_or(
            Error::new(ErrorKind::ConfigInvalid, "root is not specified")
                .with_operation("Builder::build"),
        )?;
        if let Err(e) = std::fs::metadata(&root) {
            if e.kind() == io::ErrorKind::NotFound {
                std::fs::create_dir_all(&root).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "create root dir failed")
                        .with_operation("Builder::build")
                        .with_context("root", root.to_string_lossy())
                        .set_source(e)
                })?;
            }
        }
        let root = root.canonicalize().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                "canonicalize of root directory failed",
            )
            .with_operation("Builder::build")
            .with_context("root", root.to_string_lossy())
            .set_source(e)
        })?;
        let worker_threads = 1; // TODO: test concurrency and default to available_parallelism and bind cpu
        let io_uring_entries = 1024;
        Ok(MonoiofsBackend {
            core: Arc::new(MonoiofsCore::new(root, worker_threads, io_uring_entries)),
        })
    }
}

#[derive(Debug, Clone)]
pub struct MonoiofsBackend {
    core: Arc<MonoiofsCore>,
}

pub struct MonoiofsPositionReader {
    core: Arc<MonoiofsCore>,
    path: PathBuf,
}

impl MonoiofsPositionReader {
    fn new(core: Arc<MonoiofsCore>, path: PathBuf) -> Self {
        Self { core, path }
    }
}

impl oio::PositionRead for MonoiofsPositionReader {
    type Handle = MonoiofsReader;

    async fn open(&self) -> Result<Self::Handle> {
        MonoiofsReader::new(self.core.clone(), self.path.clone()).await
    }

    async fn read_at(handle: &Self::Handle, offset: u64, size: usize) -> Result<Buffer> {
        handle.read_at(offset, size).await
    }
}

pub struct MonoiofsLazyWriter {
    core: Arc<MonoiofsCore>,
    path: String,
    append: bool,
    inner: Option<MonoiofsWriter>,
}

impl MonoiofsLazyWriter {
    fn new(core: Arc<MonoiofsCore>, path: &str, args: OpWrite) -> Self {
        Self {
            core,
            path: path.to_string(),
            append: args.append(),
            inner: None,
        }
    }

    async fn inner(&mut self) -> Result<&mut MonoiofsWriter> {
        if self.inner.is_none() {
            let path = self.core.prepare_write_path(&self.path).await?;
            let writer = MonoiofsWriter::new(self.core.clone(), path, self.append).await?;
            self.inner = Some(writer);
        }

        Ok(self
            .inner
            .as_mut()
            .expect("monoiofs writer must be initialized"))
    }
}

impl oio::Write for MonoiofsLazyWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner().await?.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner().await?.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        match &mut self.inner {
            Some(w) => w.abort().await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "Monoiofs doesn't support abort",
            )),
        }
    }
}

impl Service for MonoiofsBackend {
    type Reader = oio::PositionReader<MonoiofsPositionReader>;
    type Writer = MonoiofsLazyWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<MonoiofsDeleter>;
    type Copier = oio::OneShotCopier;

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _args: OpStat) -> Result<RpStat> {
        let path = self.core.prepare_path(path)?;
        let meta = self
            .core
            .dispatch(move || monoio::fs::metadata(path))
            .await
            .map_err(new_std_io_error)?;
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
        Ok(RpStat::new(m))
    }
    fn read(&self, _ctx: &OperationContext, path: &str, _args: OpRead) -> Result<Self::Reader> {
        let path = self.core.prepare_path(path)?;
        Ok(oio::PositionReader::new(MonoiofsPositionReader::new(
            self.core.clone(),
            path,
        )))
    }

    fn write(&self, _ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(MonoiofsLazyWriter::new(self.core.clone(), path, args))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<MonoiofsDeleter> = {
            Ok(oio::OneShotDeleter::new(MonoiofsDeleter::new(
                self.core.clone(),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, _ctx: &OperationContext, _path: &str, _args: OpList) -> Result<Self::Lister> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        let from = self.core.prepare_path(from)?;
        // ensure file exists
        self.core
            .dispatch({
                let from = from.clone();
                move || monoio::fs::metadata(from)
            })
            .await
            .map_err(new_std_io_error)?;
        let to = self.core.prepare_write_path(to).await?;
        self.core
            .dispatch(move || monoio::fs::rename(from, to))
            .await
            .map_err(new_std_io_error)?;
        Ok(RpRename::default())
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let path = self.core.prepare_path(path)?;
        self.core
            .dispatch(move || monoio::fs::create_dir_all(path))
            .await
            .map_err(new_std_io_error)?;
        Ok(RpCreateDir::default())
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> Result<Self::Copier> {
        let core = self.core.clone();
        let from = self.core.prepare_path(from)?;
        let to = to.to_string();

        let copier = oio::OneShotCopier::new(async move {
            // ensure file exists
            core.dispatch({
                let from = from.clone();
                move || monoio::fs::metadata(from)
            })
            .await
            .map_err(new_std_io_error)?;
            let to = core.prepare_write_path(&to).await?;
            core.dispatch({
                let core = core.clone();
                move || async move {
                    let from = OpenOptions::new().read(true).open(from).await?;
                    let to = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(to)
                        .await?;

                    // AsyncReadRent and AsyncWriteRent is not implemented
                    // for File, so we can't write this:
                    // monoio::io::copy(&mut from, &mut to).await?;

                    let mut pos = 0;
                    // allocate and resize buffer
                    let mut buf = core.buf_pool.get();
                    // set capacity of buf to exact size to avoid excessive read
                    buf.reserve(BUFFER_SIZE);
                    let _ = buf.split_off(BUFFER_SIZE);

                    loop {
                        let result;
                        (result, buf) = from.read_at(buf, pos).await;
                        if result? == 0 {
                            // EOF
                            break;
                        }
                        let result;
                        (result, buf) = to.write_all_at(buf, pos).await;
                        result?;
                        pos += buf.len() as u64;
                        buf.clear();
                    }
                    core.buf_pool.put(buf);
                    Ok(())
                }
            })
            .await
            .map_err(new_std_io_error)?;
            Ok(Metadata::default())
        });

        Ok(copier)
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}
