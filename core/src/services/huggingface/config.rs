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

use serde::Deserialize;
use serde::Serialize;

/// Configuration for Huggingface service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct HuggingfaceConfig {
    /// Repo type of this backend. Default is model.
    ///
    /// Available values:
    /// - model
    /// - dataset
    pub repo_type: Option<String>,
    /// Repo id of this backend.
    ///
    /// This is required.
    pub repo_id: Option<String>,
    /// Revision of this backend.
    ///
    /// Default is main.
    pub revision: Option<String>,
    /// Root of this backend. Can be "/path/to/dir".
    ///
    /// Default is "/".
    pub root: Option<String>,
    /// Token of this backend.
    ///
    /// This is optional.
    pub token: Option<String>,
}

impl Debug for HuggingfaceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("HuggingfaceConfig");

        if let Some(repo_type) = &self.repo_type {
            ds.field("repo_type", &repo_type);
        }
        if let Some(repo_id) = &self.repo_id {
            ds.field("repo_id", &repo_id);
        }
        if let Some(revision) = &self.revision {
            ds.field("revision", &revision);
        }
        if let Some(root) = &self.root {
            ds.field("root", &root);
        }
        if self.token.is_some() {
            ds.field("token", &"<redacted>");
        }

        ds.finish()
    }
}
