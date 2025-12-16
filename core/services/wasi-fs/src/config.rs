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

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use super::backend::WasiFsBuilder;

/// Configuration for WASI filesystem service.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct WasiFsConfig {
    /// Root path within the WASI filesystem.
    ///
    /// This path must be within or equal to a preopened directory.
    /// If not specified, defaults to the first preopened directory.
    pub root: Option<String>,
}

impl Debug for WasiFsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasiFsConfig")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl opendal_core::Configurator for WasiFsConfig {
    type Builder = WasiFsBuilder;

    fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(path) = uri.root() {
            if !path.is_empty() {
                map.entry("root".to_string())
                    .or_insert_with(|| format!("/{path}"));
            }
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        WasiFsBuilder { config: self }
    }
}
