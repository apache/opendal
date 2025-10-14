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

use super::backend::HttpBuilder;
use serde::Deserialize;
use serde::Serialize;

/// Config for Http service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct HttpConfig {
    /// endpoint of this backend
    pub endpoint: Option<String>,
    /// username of this backend
    pub username: Option<String>,
    /// password of this backend
    pub password: Option<String>,
    /// token of this backend
    pub token: Option<String>,
    /// root of this backend
    pub root: Option<String>,
}

impl Debug for HttpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("HttpConfig");
        de.field("endpoint", &self.endpoint);
        de.field("root", &self.root);

        de.finish_non_exhaustive()
    }
}

impl crate::Configurator for HttpConfig {
    type Builder = HttpBuilder;

    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let authority = uri.authority().ok_or_else(|| {
            crate::Error::new(crate::ErrorKind::ConfigInvalid, "uri authority is required")
                .with_context("service", crate::Scheme::Http)
        })?;

        let mut map = uri.options().clone();
        map.insert(
            "endpoint".to_string(),
            format!("{}://{}", uri.scheme(), authority),
        );

        if let Some(root) = uri.root() {
            map.insert("root".to_string(), root.to_string());
        }

        Self::from_iter(map)
    }

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        HttpBuilder {
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
            "http://example.com/static/assets".parse().unwrap(),
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = HttpConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.endpoint.as_deref(), Some("http://example.com"));
        assert_eq!(cfg.root.as_deref(), Some("static/assets"));
    }

    #[test]
    fn from_uri_preserves_query_options() {
        let uri = OperatorUri::new(
            "http://cdn.example.com/data?token=abc123".parse().unwrap(),
            Vec::<(String, String)>::new(),
        )
        .unwrap();
        let cfg = HttpConfig::from_uri(&uri).unwrap();

        assert_eq!(cfg.endpoint.as_deref(), Some("http://cdn.example.com"));
        assert_eq!(cfg.token.as_deref(), Some("abc123"));
    }

    #[test]
    fn from_uri_ignores_endpoint_override() {
        let uri = OperatorUri::new(
            "http://example.com/data".parse().unwrap(),
            vec![(
                "endpoint".to_string(),
                "https://cdn.example.com".to_string(),
            )],
        )
        .unwrap();
        let cfg = HttpConfig::from_uri(&uri).unwrap();

        assert_eq!(cfg.endpoint.as_deref(), Some("http://example.com"));
    }
}
