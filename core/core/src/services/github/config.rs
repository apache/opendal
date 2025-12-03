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

use super::GITHUB_SCHEME;
use super::backend::GithubBuilder;

/// Config for GitHub services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct GithubConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// GitHub access_token.
    ///
    /// optional.
    /// If not provided, the backend will only support read operations for public repositories.
    /// And rate limit will be limited to 60 requests per hour.
    pub token: Option<String>,
    /// GitHub repo owner.
    ///
    /// required.
    pub owner: String,
    /// GitHub repo name.
    ///
    /// required.
    pub repo: String,
}

impl Debug for GithubConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GithubConfig")
            .field("root", &self.root)
            .field("owner", &self.owner)
            .field("repo", &self.repo)
            .finish_non_exhaustive()
    }
}

impl crate::Configurator for GithubConfig {
    type Builder = GithubBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let owner = uri.name().ok_or_else(|| {
            crate::Error::new(
                crate::ErrorKind::ConfigInvalid,
                "uri host must contain owner",
            )
            .with_context("service", GITHUB_SCHEME)
        })?;

        let raw_path = uri.root().ok_or_else(|| {
            crate::Error::new(
                crate::ErrorKind::ConfigInvalid,
                "uri path must contain repository",
            )
            .with_context("service", GITHUB_SCHEME)
        })?;

        let (repo, remainder) = match raw_path.split_once('/') {
            Some((repo, rest)) => (repo, Some(rest)),
            None => (raw_path, None),
        };

        if repo.is_empty() {
            return Err(crate::Error::new(
                crate::ErrorKind::ConfigInvalid,
                "repository name is required",
            )
            .with_context("service", GITHUB_SCHEME));
        }

        let mut map = uri.options().clone();
        map.insert("owner".to_string(), owner.to_string());
        map.insert("repo".to_string(), repo.to_string());

        if let Some(rest) = remainder {
            if !rest.is_empty() {
                map.insert("root".to_string(), rest.to_string());
            }
        }

        Self::from_iter(map)
    }

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        GithubBuilder {
            config: self,
            http_client: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Configurator;
    use crate::types::OperatorUri;

    #[test]
    fn from_uri_sets_owner_repo_and_root() {
        let uri = OperatorUri::new(
            "github://apache/opendal/src/services",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = GithubConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.owner, "apache".to_string());
        assert_eq!(cfg.repo, "opendal".to_string());
        assert_eq!(cfg.root.as_deref(), Some("src/services"));
    }

    #[test]
    fn from_uri_requires_repository() {
        let uri = OperatorUri::new("github://apache", Vec::<(String, String)>::new()).unwrap();

        assert!(GithubConfig::from_uri(&uri).is_err());
    }
}
