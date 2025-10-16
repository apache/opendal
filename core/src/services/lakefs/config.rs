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

use super::backend::LakefsBuilder;
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

impl crate::Configurator for LakefsConfig {
    type Builder = LakefsBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let authority = uri.authority().ok_or_else(|| {
            crate::Error::new(crate::ErrorKind::ConfigInvalid, "uri authority is required")
                .with_context("service", crate::Scheme::Lakefs)
        })?;

        let raw_path = uri.root().ok_or_else(|| {
            crate::Error::new(
                crate::ErrorKind::ConfigInvalid,
                "uri path must contain repository",
            )
            .with_context("service", crate::Scheme::Lakefs)
        })?;

        let (repository, remainder) = match raw_path.split_once('/') {
            Some((repo, rest)) => (repo, Some(rest)),
            None => (raw_path, None),
        };

        let repository = if repository.is_empty() {
            None
        } else {
            Some(repository)
        }
        .ok_or_else(|| {
            crate::Error::new(
                crate::ErrorKind::ConfigInvalid,
                "repository is required in uri path",
            )
            .with_context("service", crate::Scheme::Lakefs)
        })?;

        let mut map = uri.options().clone();
        map.insert("endpoint".to_string(), format!("https://{authority}"));
        map.insert("repository".to_string(), repository.to_string());

        if let Some(rest) = remainder {
            if map.contains_key("branch") {
                if !rest.is_empty() {
                    map.insert("root".to_string(), rest.to_string());
                }
            } else {
                let (branch, maybe_root) = match rest.split_once('/') {
                    Some((branch_part, root_part)) => (branch_part, Some(root_part)),
                    None => (rest, None),
                };

                if !branch.is_empty() {
                    map.insert("branch".to_string(), branch.to_string());
                }

                if let Some(root_part) = maybe_root {
                    if !root_part.is_empty() {
                        map.insert("root".to_string(), root_part.to_string());
                    }
                }
            }
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        LakefsBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Configurator;
    use crate::types::OperatorUri;

    #[test]
    fn from_uri_sets_endpoint_repository_branch_and_root() {
        let uri = OperatorUri::new(
            "lakefs://api.example.com/sample/main/data/dir",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = LakefsConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("https://api.example.com"));
        assert_eq!(cfg.repository.as_deref(), Some("sample"));
        assert_eq!(cfg.branch.as_deref(), Some("main"));
        assert_eq!(cfg.root.as_deref(), Some("data/dir"));
    }

    #[test]
    fn from_uri_requires_repository() {
        let uri =
            OperatorUri::new("lakefs://api.example.com", Vec::<(String, String)>::new()).unwrap();

        assert!(LakefsConfig::from_uri(&uri).is_err());
    }

    #[test]
    fn from_uri_respects_branch_override_and_sets_root() {
        let uri = OperatorUri::new(
            "lakefs://api.example.com/sample/content",
            vec![("branch".to_string(), "develop".to_string())],
        )
        .unwrap();

        let cfg = LakefsConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.branch.as_deref(), Some("develop"));
        assert_eq!(cfg.root.as_deref(), Some("content"));
    }
}
