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

use super::backend::SwiftBuilder;
use serde::Deserialize;
use serde::Serialize;

/// Config for OpenStack Swift support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SwiftConfig {
    /// The endpoint for Swift.
    pub endpoint: Option<String>,
    /// The container for Swift.
    pub container: Option<String>,
    /// The root for Swift.
    pub root: Option<String>,
    /// The token for Swift.
    pub token: Option<String>,
}

impl Debug for SwiftConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("SwiftConfig");

        ds.field("root", &self.root);
        ds.field("endpoint", &self.endpoint);
        ds.field("container", &self.container);

        if self.token.is_some() {
            ds.field("token", &"<redacted>");
        }

        ds.finish()
    }
}

impl crate::Configurator for SwiftConfig {
    type Builder = SwiftBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(authority) = uri.authority() {
            map.entry("endpoint".to_string())
                .or_insert_with(|| format!("https://{authority}"));
        } else if !map.contains_key("endpoint") {
            return Err(
                crate::Error::new(crate::ErrorKind::ConfigInvalid, "endpoint is required")
                    .with_context("service", crate::Scheme::Swift),
            );
        }

        if let Some(path) = uri.root() {
            if let Some((container, rest)) = path.split_once('/') {
                if !container.is_empty() {
                    map.insert("container".to_string(), container.to_string());
                }
                if !rest.is_empty() {
                    map.insert("root".to_string(), rest.to_string());
                }
            } else if !path.is_empty() {
                map.insert("container".to_string(), path.to_string());
            }
        }

        if !map.contains_key("container") {
            return Err(crate::Error::new(
                crate::ErrorKind::ConfigInvalid,
                "container is required",
            )
            .with_context("service", crate::Scheme::Swift));
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        SwiftBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Configurator;
    use crate::types::OperatorUri;

    #[test]
    fn from_uri_sets_endpoint_container_and_root() {
        let uri = OperatorUri::new(
            "swift://swift.example.com/container/assets/images",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = SwiftConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("https://swift.example.com"));
        assert_eq!(cfg.container.as_deref(), Some("container"));
        assert_eq!(cfg.root.as_deref(), Some("assets/images"));
    }

    #[test]
    fn from_uri_accepts_container_from_query() {
        let uri = OperatorUri::new(
            "swift://swift.example.com",
            vec![("container".to_string(), "logs".to_string())],
        )
        .unwrap();

        let cfg = SwiftConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.container.as_deref(), Some("logs"));
        assert_eq!(cfg.endpoint.as_deref(), Some("https://swift.example.com"));
    }
}
