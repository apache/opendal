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

use serde::{Deserialize, Serialize};

use super::core::MonoiofsCore;
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
}

impl Builder for MonoiofsBuilder {
    const SCHEME: Scheme = Scheme::Monoiofs;
    type Accessor = MonoiofsBackend;
    type Config = MonoiofsConfig;

    fn from_config(config: Self::Config) -> Self {
        MonoiofsBuilder { config }
    }

    fn from_map(map: std::collections::HashMap<String, String>) -> Self {
        MonoiofsConfig::deserialize(ConfigDeserializer::new(map))
            .map(Self::from_config)
            .expect("config deserialize should success")
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
    type Reader = ();
    type Writer = ();
    type Lister = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Monoiofs)
            .set_root(&self.core.root().to_string_lossy())
            .set_native_capability(Capability {
                ..Default::default()
            });
        am.into()
    }
}
