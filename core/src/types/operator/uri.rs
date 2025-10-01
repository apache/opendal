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

use std::collections::HashMap;

use http::Uri;
use percent_encoding::percent_decode_str;

use crate::{Error, ErrorKind, Result};

/// Parsed representation of an operator URI with normalized components.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorUri {
    scheme: String,
    name: Option<String>,
    root: Option<String>,
    options: HashMap<String, String>,
}

impl OperatorUri {
    /// Build [`OperatorUri`] from a [`Uri`] plus additional options.
    pub fn new(
        uri: Uri,
        extra_options: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self> {
        let scheme = uri
            .scheme_str()
            .ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, "uri scheme is required"))?
            .to_ascii_lowercase();

        let mut options = HashMap::new();

        if let Some(query) = uri.query() {
            for pair in query.split('&') {
                if pair.is_empty() {
                    continue;
                }
                let mut parts = pair.splitn(2, '=');
                let key = parts.next().unwrap_or("");
                let value = parts.next().unwrap_or("");
                let key = percent_decode_str(key)
                    .decode_utf8_lossy()
                    .to_ascii_lowercase();
                let value = percent_decode_str(value).decode_utf8_lossy().to_string();
                options.insert(key, value);
            }
        }

        for (key, value) in extra_options {
            options.insert(key.to_ascii_lowercase(), value);
        }

        let name = uri.authority().and_then(|authority| {
            let host = authority.host();
            if host.is_empty() {
                None
            } else {
                Some(host.to_string())
            }
        });

        let decoded_path = percent_decode_str(uri.path()).decode_utf8_lossy();
        let trimmed = decoded_path.trim_matches('/');
        let root = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        };

        Ok(Self {
            scheme,
            name,
            root,
            options,
        })
    }

    /// Normalized scheme in lowercase.
    pub fn scheme(&self) -> &str {
        self.scheme.as_str()
    }

    /// Name extracted from the URI authority, if present.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Root path (without leading slash) extracted from the URI path, if present.
    pub fn root(&self) -> Option<&str> {
        self.root.as_deref()
    }

    /// Normalized option map merged from query string and extra options (excluding reserved keys).
    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }
}

/// Conversion trait that builds [`OperatorUri`] from various inputs.
pub trait IntoOperatorUri {
    /// Convert the input into an [`OperatorUri`].
    fn into_operator_uri(self) -> Result<OperatorUri>;
}

impl IntoOperatorUri for OperatorUri {
    fn into_operator_uri(self) -> Result<OperatorUri> {
        Ok(self)
    }
}

impl IntoOperatorUri for &OperatorUri {
    fn into_operator_uri(self) -> Result<OperatorUri> {
        Ok(self.clone())
    }
}

impl IntoOperatorUri for Uri {
    fn into_operator_uri(self) -> Result<OperatorUri> {
        OperatorUri::new(self, Vec::<(String, String)>::new())
    }
}

impl IntoOperatorUri for &Uri {
    fn into_operator_uri(self) -> Result<OperatorUri> {
        OperatorUri::new(self.clone(), Vec::<(String, String)>::new())
    }
}

impl IntoOperatorUri for &str {
    fn into_operator_uri(self) -> Result<OperatorUri> {
        let uri = self.parse::<Uri>().map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "failed to parse uri").set_source(err)
        })?;
        OperatorUri::new(uri, Vec::<(String, String)>::new())
    }
}

impl IntoOperatorUri for String {
    fn into_operator_uri(self) -> Result<OperatorUri> {
        let uri = self.parse::<Uri>().map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "failed to parse uri").set_source(err)
        })?;
        OperatorUri::new(uri, Vec::<(String, String)>::new())
    }
}

impl<O, K, V> IntoOperatorUri for (Uri, O)
where
    O: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    fn into_operator_uri(self) -> Result<OperatorUri> {
        let (uri, extra) = self;
        let opts = extra
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Vec<_>>();
        OperatorUri::new(uri, opts)
    }
}

impl<O, K, V> IntoOperatorUri for (&Uri, O)
where
    O: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    fn into_operator_uri(self) -> Result<OperatorUri> {
        let (uri, extra) = self;
        let opts = extra
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Vec<_>>();
        OperatorUri::new(uri.clone(), opts)
    }
}

impl<O, K, V> IntoOperatorUri for (&str, O)
where
    O: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    fn into_operator_uri(self) -> Result<OperatorUri> {
        let (base, extra) = self;
        let uri = base.parse::<Uri>().map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "failed to parse uri").set_source(err)
        })?;
        let opts = extra
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Vec<_>>();
        OperatorUri::new(uri, opts)
    }
}

impl<O, K, V> IntoOperatorUri for (String, O)
where
    O: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    fn into_operator_uri(self) -> Result<OperatorUri> {
        let (base, extra) = self;
        (&base[..], extra).into_operator_uri()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::IntoOperatorUri;

    #[test]
    fn parse_uri_with_name_and_root() {
        let uri = OperatorUri::new(
            "s3://example-bucket/photos/2024".parse().unwrap(),
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert_eq!(uri.scheme(), "s3");
        assert_eq!(uri.name(), Some("example-bucket"));
        assert_eq!(uri.root(), Some("photos/2024"));
        assert!(uri.options().is_empty());
    }

    #[test]
    fn into_operator_uri_merges_extra_options() {
        let uri = (
            "s3://bucket/path?region=us-east-1",
            vec![("region", "override"), ("endpoint", "https://custom")],
        )
            .into_operator_uri()
            .unwrap();

        assert_eq!(uri.scheme(), "s3");
        assert_eq!(uri.name(), Some("bucket"));
        assert_eq!(uri.root(), Some("path"));
        assert_eq!(
            uri.options().get("region").map(String::as_str),
            Some("override")
        );
        assert_eq!(
            uri.options().get("endpoint").map(String::as_str),
            Some("https://custom")
        );
    }
}
