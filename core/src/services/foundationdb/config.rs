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
use std::fmt::Formatter;

use super::backend::FoundationdbBuilder;
use serde::Deserialize;
use serde::Serialize;

/// [foundationdb](https://www.foundationdb.org/) service support.
///Config for FoundationDB.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct FoundationdbConfig {
    ///root of the backend.
    pub root: Option<String>,
    ///config_path for the backend.
    pub config_path: Option<String>,
}

impl Debug for FoundationdbConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("FoundationConfig");

        ds.field("root", &self.root);
        ds.field("config_path", &self.config_path);

        ds.finish()
    }
}

impl crate::Configurator for FoundationdbConfig {
    type Builder = FoundationdbBuilder;
    fn into_builder(self) -> Self::Builder {
        FoundationdbBuilder { config: self }
    }
}
