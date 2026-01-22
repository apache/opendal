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

use serde::Deserialize;
use serde::Serialize;

use super::backend::GitBuilder;

/// Configuration for Git service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct GitConfig {
    /// Git repository URL (required).
    ///
    /// Examples:
    /// - https://github.com/apache/opendal.git
    /// - https://gitlab.com/user/repo.git
    /// - https://huggingface.co/meta-llama/Llama-2-7b
    pub repository: Option<String>,

    /// Git reference (branch, tag, or commit hash).
    ///
    /// If not set, will use whatever HEAD points to in the remote repository.
    pub reference: Option<String>,

    /// Root path within the repository.
    ///
    /// Default is "/".
    pub root: Option<String>,

    /// Username for authentication (optional).
    pub username: Option<String>,

    /// Password or personal access token for authentication (optional).
    pub password: Option<String>,

    /// Whether to resolve Git LFS pointer files.
    ///
    /// When enabled (default), the service will automatically detect
    /// Git LFS pointer files and stream the actual LFS content instead
    /// of the pointer file itself.
    ///
    /// Default is true.
    pub resolve_lfs: Option<bool>,
}

impl Debug for GitConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitConfig")
            .field("repository", &self.repository)
            .field("reference", &self.reference)
            .field("root", &self.root)
            .field("username", &self.username)
            .field("resolve_lfs", &self.resolve_lfs)
            .finish_non_exhaustive()
    }
}

impl opendal_core::Configurator for GitConfig {
    type Builder = GitBuilder;

    fn into_builder(self) -> Self::Builder {
        GitBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_git_config_default() {
        let config = GitConfig::default();
        assert_eq!(config.repository, None);
        assert_eq!(config.reference, None);
        assert_eq!(config.root, None);
    }
}
