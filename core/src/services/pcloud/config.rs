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

use super::backend::PcloudBuilder;
use serde::Deserialize;
use serde::Serialize;

/// Config for Pcloud services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct PcloudConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    ///pCloud  endpoint address.
    pub endpoint: String,
    /// pCloud username.
    pub username: Option<String>,
    /// pCloud password.
    pub password: Option<String>,
}

impl Debug for PcloudConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Config");

        ds.field("root", &self.root);
        ds.field("endpoint", &self.endpoint);
        ds.field("username", &self.username);

        ds.finish()
    }
}

impl crate::Configurator for PcloudConfig {
    type Builder = PcloudBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let authority = uri.authority().ok_or_else(|| {
            crate::Error::new(crate::ErrorKind::ConfigInvalid, "uri authority is required")
                .with_context("service", crate::Scheme::Pcloud)
        })?;

        let mut map = uri.options().clone();
        map.insert("endpoint".to_string(), format!("https://{authority}"));

        if let Some(root) = uri.root() {
            if !root.is_empty() {
                map.insert("root".to_string(), root.to_string());
            }
        }

        Self::from_iter(map)
    }

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        PcloudBuilder {
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
    fn from_uri_sets_endpoint_and_root() {
        let uri = OperatorUri::new(
            "pcloud://api.pcloud.com/drive/photos".parse().unwrap(),
            vec![("username".to_string(), "alice".to_string())],
        )
        .unwrap();

        let cfg = PcloudConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint, "https://api.pcloud.com".to_string());
        assert_eq!(cfg.root.as_deref(), Some("drive/photos"));
        assert_eq!(cfg.username.as_deref(), Some("alice"));
    }

    #[test]
    fn from_uri_requires_authority() {
        assert!(
            OperatorUri::new(
                "pcloud:///drive".parse().unwrap(),
                Vec::<(String, String)>::new(),
            )
            .is_err()
        );
    }
}
