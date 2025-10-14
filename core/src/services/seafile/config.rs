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

use super::backend::SeafileBuilder;
use serde::Deserialize;
use serde::Serialize;

/// Config for seafile services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SeafileConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// endpoint address of this backend.
    pub endpoint: Option<String>,
    /// username of this backend.
    pub username: Option<String>,
    /// password of this backend.
    pub password: Option<String>,
    /// repo_name of this backend.
    ///
    /// required.
    pub repo_name: String,
}

impl Debug for SeafileConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SeafileConfig");

        d.field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("username", &self.username)
            .field("repo_name", &self.repo_name);

        d.finish_non_exhaustive()
    }
}

impl crate::Configurator for SeafileConfig {
    type Builder = SeafileBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        SeafileBuilder {
            config: self,
            http_client: None,
        }
    }
}
