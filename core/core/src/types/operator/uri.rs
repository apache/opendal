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
use std::fmt;

use http::Uri;
use percent_encoding::percent_decode_str;
use url::Url;

use crate::{Error, ErrorKind, Result};

/// Parsed representation of an operator URI with normalized components.
#[derive(Clone, Eq, PartialEq)]
pub struct OperatorUri {
    scheme: String,
    authority: Option<String>,
    name: Option<String>,
    username: Option<String>,
    password: Option<String>,
    root: Option<String>,
    options: HashMap<String, String>,
}

impl fmt::Debug for OperatorUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut option_keys: Vec<_> = self.options.keys().collect();
        option_keys.sort_unstable();
        f.debug_struct("OperatorUri")
            .field("scheme", &self.scheme)
            .field("authority", &self.authority)
            .field("name", &self.name)
            .field("root", &self.root)
            .field("option_keys", &option_keys)
            .finish_non_exhaustive()
    }
}

impl OperatorUri {
    /// Build [`OperatorUri`] from a URI string plus additional options.
    pub fn new(
        base: &str,
        extra_options: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self> {
        let extra_opts = extra_options
            .into_iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v))
            .collect::<Vec<_>>();

        let mut options = HashMap::<String, String>::new();

        // Allow pure scheme (e.g. "s3") to align with from_iter semantics.
        if !base.contains("://") {
            for (key, value) in &extra_opts {
                options.insert(key.clone(), value.clone());
            }
            return Ok(Self {
                scheme: base.to_ascii_lowercase(),
                authority: None,
                name: None,
                username: None,
                password: None,
                root: None,
                options,
            });
        }

        let url = Url::parse(base).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "failed to parse uri").set_source(err)
        })?;

        let scheme = url.scheme().to_ascii_lowercase();

        for (key, value) in url.query_pairs() {
            options.insert(key.to_ascii_lowercase(), value.into_owned());
        }

        for (key, value) in extra_opts {
            options.insert(key, value);
        }

        // `Url` reports userinfo in percent-encoded form, so decode it the same
        // way as `root` and query options.
        let username = if url.username().is_empty() {
            None
        } else {
            Some(
                percent_decode_str(url.username())
                    .decode_utf8_lossy()
                    .into_owned(),
            )
        };

        let password = url
            .password()
            .map(|encoded| percent_decode_str(encoded).decode_utf8_lossy().into_owned());

        let authority = url.host_str().filter(|host| !host.is_empty()).map(|host| {
            if let Some(port) = url.port() {
                format!("{host}:{port}")
            } else {
                host.to_string()
            }
        });

        let name = url
            .host_str()
            .filter(|host| !host.is_empty())
            .map(|host| host.to_string());

        let decoded_path = percent_decode_str(url.path()).decode_utf8_lossy();
        let trimmed = decoded_path.trim_matches('/');
        let root = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        };

        Ok(Self {
            scheme,
            authority,
            name,
            username,
            password,
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

    /// Authority extracted from the URI, if present (host with optional port).
    pub fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }

    /// Username extracted from the URI, if present.
    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    /// Password extracted from the URI, if present.
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// Root path (without leading slash) extracted from the URI path, if present.
    pub fn root(&self) -> Option<&str> {
        self.root.as_deref()
    }

    /// Normalized options merged from the query string and extra options.
    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }

    /// Remove matching options while preserving the URI components.
    pub(crate) fn extract_options(
        &mut self,
        mut matches: impl FnMut(&str) -> bool,
    ) -> impl Iterator<Item = (String, String)> {
        self.options.extract_if(move |key, _| matches(key))
    }

    /// Retrieve a specific option by key (case-insensitive).
    pub fn option(&self, key: &str) -> Option<&str> {
        self.options
            .get(&key.to_ascii_lowercase())
            .map(String::as_str)
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
        let serialized = self.to_string();
        OperatorUri::new(&serialized, Vec::<(String, String)>::new())
    }
}

impl IntoOperatorUri for &Uri {
    fn into_operator_uri(self) -> Result<OperatorUri> {
        let serialized = self.to_string();
        OperatorUri::new(&serialized, Vec::<(String, String)>::new())
    }
}

impl IntoOperatorUri for &str {
    fn into_operator_uri(self) -> Result<OperatorUri> {
        OperatorUri::new(self, Vec::<(String, String)>::new())
    }
}

impl IntoOperatorUri for String {
    fn into_operator_uri(self) -> Result<OperatorUri> {
        OperatorUri::new(&self, Vec::<(String, String)>::new())
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
        let serialized = uri.to_string();
        let opts = extra
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Vec<_>>();
        OperatorUri::new(&serialized, opts)
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
        let serialized = uri.to_string();
        let opts = extra
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Vec<_>>();
        OperatorUri::new(&serialized, opts)
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
        let opts = extra
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Vec<_>>();
        OperatorUri::new(base, opts)
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
    fn debug_omits_userinfo_and_all_option_values() -> Result<()> {
        let uri = (
            "demo://dummy-user:dummy%2Dpass@example.com:8080/sample",
            [
                ("master_key", "dummy-master-key"),
                (
                    "context.http.proxy",
                    "http://dummy-proxy-user:dummy-proxy-pass@proxy.test",
                ),
                ("custom_option", "dummy-custom-value"),
            ],
        )
            .into_operator_uri()?;

        for output in [format!("{uri:?}"), format!("{uri:#?}")] {
            assert!(output.contains("example.com:8080"));
            for key in ["master_key", "context.http.proxy", "custom_option"] {
                assert!(output.contains(key));
            }
            assert!(!output.contains("dummy-"), "{output}");
        }
        Ok(())
    }

    #[test]
    fn parse_uri_with_name_and_root() {
        let uri = OperatorUri::new(
            "s3://example-bucket/photos/2024",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert_eq!(uri.scheme(), "s3");
        assert_eq!(uri.authority(), Some("example-bucket"));
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

    #[test]
    fn parse_uri_with_port_preserves_authority() {
        let uri = OperatorUri::new(
            "http://example.com:8080/root",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert_eq!(uri.scheme(), "http");
        assert_eq!(uri.authority(), Some("example.com:8080"));
        assert_eq!(uri.name(), Some("example.com"));
        assert_eq!(uri.root(), Some("root"));
    }

    #[test]
    fn parse_uri_with_credentials_splits_authority() {
        let uri = OperatorUri::new(
            "https://alice:secret@example.com:8443/path",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert_eq!(uri.scheme(), "https");
        assert_eq!(uri.authority(), Some("example.com:8443"));
        assert_eq!(uri.username(), Some("alice"));
        assert_eq!(uri.password(), Some("secret"));
        assert_eq!(uri.root(), Some("path"));
    }

    #[test]
    fn parse_uri_decodes_percent_encoded_userinfo() {
        let uri = OperatorUri::new(
            "https://alice%40corp.com:p%40ss%2Fw%3Ard%251@example.com:8443/path%20dir",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert_eq!(uri.authority(), Some("example.com:8443"));
        assert_eq!(uri.username(), Some("alice@corp.com"));
        assert_eq!(uri.password(), Some("p@ss/w:rd%1"));
        assert_eq!(uri.root(), Some("path dir"));
    }

    #[test]
    fn parse_uri_with_password_only_keeps_username_none() {
        let uri = OperatorUri::new(
            "https://:only%23pass@example.com/",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert_eq!(uri.username(), None);
        assert_eq!(uri.password(), Some("only#pass"));
        assert_eq!(uri.root(), None);
    }

    #[test]
    fn parse_uri_with_empty_password_keeps_username() {
        // `Url` normalizes `alice:@example.com` to `alice@example.com`, so an
        // empty password is reported as `None` instead of `Some("")`.
        let uri = OperatorUri::new(
            "https://alice:@example.com/",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert_eq!(uri.username(), Some("alice"));
        assert_eq!(uri.password(), None);
    }

    #[test]
    fn parse_uri_keeps_plain_userinfo_unchanged() {
        let uri = OperatorUri::new(
            "https://alice:pl+ain-pass@example.com/share",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        assert_eq!(uri.username(), Some("alice"));
        assert_eq!(uri.password(), Some("pl+ain-pass"));
        assert_eq!(uri.root(), Some("share"));
    }
}
