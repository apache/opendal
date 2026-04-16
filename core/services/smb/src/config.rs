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

use super::SMB_SCHEME;
use super::backend::SmbBuilder;

/// Config for SMB service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SmbConfig {
    /// Endpoint of this backend.
    pub endpoint: Option<String>,
    /// Share name of this backend.
    pub share: String,
    /// Root of this backend.
    pub root: Option<String>,
    /// User of this backend.
    pub user: Option<String>,
    /// Password of this backend.
    pub password: Option<String>,
}

impl Debug for SmbConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmbConfig")
            .field("endpoint", &self.endpoint)
            .field("share", &self.share)
            .field("root", &self.root)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

impl opendal_core::Configurator for SmbConfig {
    type Builder = SmbBuilder;

    fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(authority) = uri.authority() {
            map.insert("endpoint".to_string(), authority.to_string());
        }

        if let Some(username) = uri.username() {
            map.entry("user".to_string())
                .or_insert_with(|| username.to_string());
        }

        if let Some(password) = uri.password() {
            map.entry("password".to_string())
                .or_insert_with(|| password.to_string());
        }

        if let Some(root) = uri.root() {
            if let Some((share, rest)) = root.split_once('/') {
                if share.is_empty() {
                    return Err(opendal_core::Error::new(
                        opendal_core::ErrorKind::ConfigInvalid,
                        "share is required in uri path",
                    )
                    .with_context("service", SMB_SCHEME));
                }

                map.insert("share".to_string(), share.to_string());
                if !rest.is_empty() {
                    map.insert("root".to_string(), rest.to_string());
                }
            } else if !root.is_empty() {
                map.insert("share".to_string(), root.to_string());
            }
        }

        if !map.contains_key("share") {
            return Err(opendal_core::Error::new(
                opendal_core::ErrorKind::ConfigInvalid,
                "share is required",
            )
            .with_context("service", SMB_SCHEME));
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        SmbBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::Configurator;
    use opendal_core::OperatorUri;

    #[test]
    fn from_uri_sets_endpoint_share_root_and_auth() {
        let uri = OperatorUri::new(
            "smb://alice:secret@smb.example.com:1445/share/documents/reports",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = SmbConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("smb.example.com:1445"));
        assert_eq!(cfg.share, "share".to_string());
        assert_eq!(cfg.root.as_deref(), Some("documents/reports"));
        assert_eq!(cfg.user.as_deref(), Some("alice"));
        assert_eq!(cfg.password.as_deref(), Some("secret"));
    }

    #[test]
    fn from_uri_accepts_share_from_query() {
        let uri = OperatorUri::new(
            "smb://server",
            vec![("share".to_string(), "data".to_string())],
        )
        .unwrap();

        let cfg = SmbConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("server"));
        assert_eq!(cfg.share, "data".to_string());
    }
}
