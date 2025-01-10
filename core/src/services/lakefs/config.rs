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

/// Configuration for Lakefs service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct LakefsConfig {
    /// Base url.
    ///
    /// This is required.
    pub endpoint: Option<String>,
    /// Username for Lakefs basic authentication.
    ///
    /// This is required.
    pub username: Option<String>,
    /// Password for Lakefs basic authentication.
    ///
    /// This is required.
    pub password: Option<String>,
    /// Root of this backend. Can be "/path/to/dir".
    ///
    /// Default is "/".
    pub root: Option<String>,

    /// The repository name
    ///
    /// This is required.
    pub repository: Option<String>,
    /// Name of the branch or a commit ID. Default is main.
    ///
    /// This is optional.
    pub branch: Option<String>,
}

impl Debug for LakefsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("LakefsConfig");

        if let Some(endpoint) = &self.endpoint {
            ds.field("endpoint", &endpoint);
        }
        if let Some(_username) = &self.username {
            ds.field("username", &"<redacted>");
        }
        if let Some(_password) = &self.password {
            ds.field("password", &"<redacted>");
        }
        if let Some(root) = &self.root {
            ds.field("root", &root);
        }
        if let Some(repository) = &self.repository {
            ds.field("repository", &repository);
        }
        if let Some(branch) = &self.branch {
            ds.field("branch", &branch);
        }

        ds.finish()
    }
}
