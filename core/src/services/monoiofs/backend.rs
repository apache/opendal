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
use serde::Deserialize;
use serde::Serialize;

use super::core::MonoiofsCore;
use super::reader::MonoiofsReader;
use super::writer::MonoiofsWriter;
use crate::raw::*;
use crate::*;

/// Config for monoiofs services support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct MonoiofsConfig {
    /// The Root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// Builder::build will return error if not set.
    pub root: Option<String>,
}

impl Configurator for MonoiofsConfig {
    fn into_builder(self) -> impl Builder {
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
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Monoiofs)
            .set_root(&self.core.root().to_string_lossy())
            .set_native_capability(Capability {
                stat: true,
                read: true,
                write: true,
                delete: true,
                ..Default::default()
            });
        am.into()
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let path = self.core.prepare_path(path);
        // TODO: borrowed from FsBackend because statx support for monoio
        // is not released yet, but stat capability is required for read
        // and write
        let meta = tokio::fs::metadata(&path).await.map_err(new_std_io_error)?;
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

    async fn write(&self, path: &str, _args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        // TODO: create parent directory before write
        let path = self.core.prepare_path(path);
        let writer = MonoiofsWriter::new(self.core.clone(), path).await?;
        Ok((RpWrite::default(), writer))
    }

    async fn delete(&self, path: &str, _args: OpDelete) -> Result<RpDelete> {
        let path = self.core.prepare_path(path);
        // TODO: borrowed from FsBackend because monoio doesn't support unlink,
        // but delete capability is required for behavior tests
        let meta = tokio::fs::metadata(&path).await;
        match meta {
            Ok(meta) => {
                if meta.is_dir() {
                    tokio::fs::remove_dir(&path)
                        .await
                        .map_err(new_std_io_error)?;
                } else {
                    tokio::fs::remove_file(&path)
                        .await
                        .map_err(new_std_io_error)?;
                }

                Ok(RpDelete::default())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(RpDelete::default()),
            Err(err) => Err(new_std_io_error(err)),
        }
    }
}
