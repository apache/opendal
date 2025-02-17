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

use chrono::DateTime;
use monoio::fs::OpenOptions;

use super::core::MonoiofsCore;
use super::core::BUFFER_SIZE;
use super::delete::MonoiofsDeleter;
use super::reader::MonoiofsReader;
use super::writer::MonoiofsWriter;
use crate::raw::*;
use crate::services::MonoiofsConfig;
use crate::*;

impl Configurator for MonoiofsConfig {
    type Builder = MonoiofsBuilder;
    fn into_builder(self) -> Self::Builder {
        MonoiofsBuilder { config: self }
    }
}

/// File system support via [`monoio`].
#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct MonoiofsBuilder {
    config: MonoiofsConfig,
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
    const SCHEME: Scheme = Scheme::Monoiofs;
    type Config = MonoiofsConfig;

    fn build(self) -> Result<impl Access> {
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

impl Access for MonoiofsBackend {
    type Reader = MonoiofsReader;
    type Writer = MonoiofsWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<MonoiofsDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Monoiofs)
            .set_root(&self.core.root().to_string_lossy())
            .set_native_capability(Capability {
                stat: true,
                stat_has_content_length: true,
                stat_has_last_modified: true,

                read: true,

                write: true,
                write_can_append: true,
                write_has_last_modified: true,

                delete: true,
                rename: true,
                create_dir: true,
                copy: true,
                shared: true,
                ..Default::default()
            });
        am.into()
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let path = self.core.prepare_path(path);
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
            .with_last_modified(
                meta.modified()
                    .map(DateTime::from)
                    .map_err(new_std_io_error)?,
            );
        Ok(RpStat::new(m))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let path = self.core.prepare_path(path);
        let reader = MonoiofsReader::new(self.core.clone(), path, args.range()).await?;
        Ok((RpRead::default(), reader))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let path = self.core.prepare_write_path(path).await?;
        let writer = MonoiofsWriter::new(self.core.clone(), path, args.append()).await?;
        Ok((RpWrite::default(), writer))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(MonoiofsDeleter::new(self.core.clone())),
        ))
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let from = self.core.prepare_path(from);
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

    async fn create_dir(&self, path: &str, _args: OpCreateDir) -> Result<RpCreateDir> {
        let path = self.core.prepare_path(path);
        self.core
            .dispatch(move || monoio::fs::create_dir_all(path))
            .await
            .map_err(new_std_io_error)?;
        Ok(RpCreateDir::default())
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let from = self.core.prepare_path(from);
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
            .dispatch({
                let core = self.core.clone();
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
        Ok(RpCopy::default())
    }
}
