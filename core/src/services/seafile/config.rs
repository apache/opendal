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

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let authority = uri.authority().ok_or_else(|| {
            crate::Error::new(crate::ErrorKind::ConfigInvalid, "uri authority is required")
                .with_context("service", crate::Scheme::Seafile)
        })?;

        let raw_path = uri.root().ok_or_else(|| {
            crate::Error::new(
                crate::ErrorKind::ConfigInvalid,
                "uri path must start with repo name",
            )
            .with_context("service", crate::Scheme::Seafile)
        })?;

        let mut segments = raw_path.splitn(2, '/');
        let repo_name = segments.next().filter(|s| !s.is_empty()).ok_or_else(|| {
            crate::Error::new(
                crate::ErrorKind::ConfigInvalid,
                "repo name is required in uri path",
            )
            .with_context("service", crate::Scheme::Seafile)
        })?;

        let mut map = uri.options().clone();
        map.insert("endpoint".to_string(), format!("https://{authority}"));
        map.insert("repo_name".to_string(), repo_name.to_string());

        if let Some(rest) = segments.next() {
            if !rest.is_empty() {
                map.insert("root".to_string(), rest.to_string());
            }
        }

        Self::from_iter(map)
    }

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        SeafileBuilder {
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
    fn from_uri_sets_endpoint_repo_and_root() {
        let uri = OperatorUri::new(
            "seafile://files.example.com/myrepo/projects/app",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = SeafileConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("https://files.example.com"));
        assert_eq!(cfg.repo_name, "myrepo".to_string());
        assert_eq!(cfg.root.as_deref(), Some("projects/app"));
    }

    #[test]
    fn from_uri_requires_repo_name() {
        let uri = OperatorUri::new(
            "seafile://files.example.com",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert!(SeafileConfig::from_uri(&uri).is_err());
    }
}
