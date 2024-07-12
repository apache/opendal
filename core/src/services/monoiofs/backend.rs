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
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;

use super::core::MonoiofsCore;
use crate::raw::*;
use crate::*;

/// Config for monoiofs services support.
#[derive(Default, Deserialize, Debug)]
#[serde(default)]
#[non_exhaustive]
pub struct MonoiofsConfig {
    /// The Root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// Builder::build will return error if not set.
    pub root: Option<String>,
    /// Count of worker threads that each runs a monoio runtime.
    ///
    /// Default to 1.
    pub worker_threads: Option<NonZeroUsize>,
    /// Size of io_uring queue entries for monoio runtime.
    ///
    /// Default to 1024, must be at least 256.
    pub io_uring_entries: Option<u32>,
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
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };
        self
    }

    /// Set count of worker threads that each runs a monoio runtime.
    pub fn worker_threads(&mut self, worker_threads: NonZeroUsize) -> &mut Self {
        self.config.worker_threads = Some(worker_threads);
        self
    }

    /// Set size of io_uring queue entries for monoio runtime.
    pub fn io_uring_entries(&mut self, io_uring_entries: u32) -> &mut Self {
        self.config.io_uring_entries = Some(io_uring_entries);
        self
    }
}

impl Builder for MonoiofsBuilder {
    const SCHEME: Scheme = Scheme::Monoiofs;

    type Accessor = MonoiofsBackend;

    fn from_map(map: std::collections::HashMap<String, String>) -> Self {
        let config = MonoiofsConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize should success");
        MonoiofsBuilder { config }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let root = self.config.root.take().map(PathBuf::from).ok_or(
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
        let worker_threads = self.config.worker_threads.map_or(1, |n| n.get());
        let io_uring_entries = self.config.io_uring_entries.unwrap_or(1024);
        let io_uring_entries = if io_uring_entries < 256 {
            256
        } else {
            io_uring_entries
        };
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
    type Reader = ();
    type Writer = ();
    type Lister = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Monoiofs)
            .set_root(&self.core.root().to_string_lossy())
            .set_native_capability(Capability {
                ..Default::default()
            });
        am
    }
}
