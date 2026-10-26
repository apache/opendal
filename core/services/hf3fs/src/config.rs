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

use opendal_core::{Configurator, OperatorUri, Result};
use serde::{Deserialize, Serialize};

use crate::Hf3fs;

/// Configuration for 3FS USRBIO access on Linux.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct Hf3fsConfig {
    /// Existing absolute directory on a mounted 3FS instance. Required.
    pub root: Option<String>,
    /// USRBIO shared library path. Defaults to `libhf3fs_api_shared.so`.
    pub library_path: Option<String>,
    /// Maximum in-flight requests per read or write ring. Defaults to 32.
    pub queue_depth: Option<usize>,
    /// Maximum bytes per request. Defaults to 1 MiB.
    /// Shared memory usage is approximately `3 * queue_depth * block_size`.
    pub block_size: Option<usize>,
}

impl Configurator for Hf3fsConfig {
    type Builder = Hf3fs;

    fn from_uri(uri: &OperatorUri) -> Result<Self> {
        let mut options = uri.options().clone();
        if let Some(root) = uri.root().filter(|v| !v.is_empty()) {
            options
                .entry("root".into())
                .or_insert_with(|| format!("/{root}"));
        }
        Self::from_iter(options)
    }

    fn into_builder(self) -> Self::Builder {
        Hf3fs { config: self }
    }
}
