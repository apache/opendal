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

use opendal_core::Configurator;
use opendal_core::OperatorUri;
use opendal_core::Result;
use serde::Deserialize;
use serde::Serialize;

use crate::preset;
use crate::r2::R2Builder;

/// Configuration for Cloudflare R2.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct R2Config {
    /// Root within the bucket.
    ///
    /// All operations happen under this root. The default is `/`.
    ///
    /// <!-- @group General -->
    /// <!-- @default / -->
    pub root: Option<String>,
    /// Bucket name.
    ///
    /// This field is required.
    ///
    /// <!-- @group General -->
    /// <!-- @example my-bucket -->
    pub bucket: String,
    /// Cloudflare account ID used to derive the R2 endpoint.
    ///
    /// Set exactly one of `account_id` and `endpoint`.
    ///
    /// <!-- @group General -->
    /// <!-- @example example-account -->
    /// <!-- @minimal true -->
    pub account_id: Option<String>,
    /// R2 jurisdiction.
    ///
    /// Supported values are `eu` and `fedramp`. This field requires
    /// `account_id` and cannot be used with `endpoint`.
    ///
    /// <!-- @group General -->
    pub jurisdiction: Option<String>,
    /// Explicit R2-compatible endpoint.
    ///
    /// Use this field for a proxy, gateway, or test server. Set exactly one of
    /// `endpoint` and `account_id`.
    ///
    /// <!-- @group General -->
    /// <!-- @example https://example.r2.cloudflarestorage.com -->
    pub endpoint: Option<String>,
    /// Access key ID.
    ///
    /// Set this field together with `secret_access_key`.
    ///
    /// <!-- @group Credentials -->
    pub access_key_id: Option<String>,
    /// Secret access key.
    ///
    /// Set this field together with `access_key_id`.
    ///
    /// <!-- @group Credentials -->
    pub secret_access_key: Option<String>,
    /// Session token for temporary credentials.
    ///
    /// This field requires `access_key_id` and `secret_access_key`.
    ///
    /// <!-- @group Credentials -->
    pub session_token: Option<String>,
}

impl Debug for R2Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("R2Config")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("account_id", &self.account_id)
            .field("jurisdiction", &self.jurisdiction)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl Configurator for R2Config {
    type Builder = R2Builder;

    fn from_uri(uri: &OperatorUri) -> Result<Self> {
        preset::from_uri(uri)
    }

    fn into_builder(self) -> Self::Builder {
        R2Builder::from_config(self)
    }
}

#[cfg(test)]
mod tests {
    use std::iter;

    use opendal_core::ErrorKind;

    use super::*;

    #[test]
    fn from_uri_extracts_bucket_root_and_options() {
        let uri = OperatorUri::new(
            "r2://example-bucket/path/to/root?account_id=example-account",
            iter::empty(),
        )
        .unwrap();
        let config = R2Config::from_uri(&uri).unwrap();

        assert_eq!(config.bucket, "example-bucket");
        assert_eq!(config.root.as_deref(), Some("path/to/root"));
        assert_eq!(config.account_id.as_deref(), Some("example-account"));
    }

    #[test]
    fn rejects_unknown_fields() {
        let err = R2Config::from_iter([("role_arn".to_string(), "role".to_string())]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn debug_redacts_credentials() {
        let config = R2Config {
            bucket: "bucket".to_string(),
            access_key_id: Some("access-value".to_string()),
            secret_access_key: Some("secret-value".to_string()),
            session_token: Some("token-value".to_string()),
            ..Default::default()
        };

        let output = format!("{config:?}");
        assert!(!output.contains("access-value"));
        assert!(!output.contains("secret-value"));
        assert!(!output.contains("token-value"));
    }
}
