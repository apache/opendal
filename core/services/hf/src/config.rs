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

use super::HF_SCHEME;
use super::backend::HfBuilder;
use super::uri::HfUri;
use super::uri::RepoType;

/// Configuration for Hugging Face service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct HfConfig {
    /// Repo type of this backend. Default is model.
    ///
    /// Default is model
    pub repo_type: RepoType,
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
    /// TODO(kszucs): consider removing it
    pub root: Option<String>,
    /// Token of this backend.
    ///
    /// This is optional.
    pub token: Option<String>,
    /// Endpoint of the Hugging Face Hub.
    ///
    /// Default is "https://huggingface.co".
    pub endpoint: Option<String>,
    /// Enable XET storage protocol for reads.
    ///
    /// When true and the `xet` feature is compiled in, reads will
    /// check for XET-backed files and use the XET protocol for
    /// downloading. Default is false.
    pub xet: bool,
}

impl Debug for HfConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HfConfig")
            .field("repo_type", &self.repo_type)
            .field("repo_id", &self.repo_id)
            .field("revision", &self.revision)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl opendal_core::Configurator for HfConfig {
    type Builder = HfBuilder;

    fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
        // Reconstruct the full path from authority (name) and root.
        // OperatorUri splits "hf://datasets/user/repo" into
        // name="datasets" and root="user/repo".
        let mut path = String::new();
        if let Some(name) = uri.name() {
            if !name.is_empty() {
                path.push_str(name);
            }
        }
        if let Some(root) = uri.root() {
            if !root.is_empty() {
                if !path.is_empty() {
                    path.push('/');
                }
                path.push_str(root);
            }
        }

        let parsed = HfUri::parse(&path)?;
        Ok(Self {
            repo_type: parsed.repo.repo_type,
            repo_id: Some(parsed.repo.repo_id),
            revision: parsed.repo.revision,
            ..Default::default()
        })
    }

    fn into_builder(self) -> Self::Builder {
        HfBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_uri_with_all_components() {
        use opendal_core::Configurator;
        use opendal_core::OperatorUri;

        let uri = OperatorUri::new(
            "hf://datasets/username/my_dataset@dev/train/data.csv",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = HfConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.repo_type, RepoType::Dataset);
        assert_eq!(cfg.repo_id.as_deref(), Some("username/my_dataset"));
        assert_eq!(cfg.revision.as_deref(), Some("dev"));
        assert!(cfg.root.is_none());
    }
}
