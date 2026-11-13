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

use crate::minio::MinioBuilder;
use crate::preset;

/// Configuration for a MinIO deployment.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct MinioConfig {
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
    /// MinIO endpoint.
    ///
    /// This field is required because MinIO deployments do not share a
    /// universal endpoint.
    ///
    /// <!-- @group General -->
    /// <!-- @example http://127.0.0.1:9000 -->
    /// <!-- @minimal true -->
    /// <!-- @required true -->
    pub endpoint: Option<String>,
    /// Signing region.
    ///
    /// The default is `auto`. Set this field when the deployment requires a
    /// configured region.
    ///
    /// <!-- @group General -->
    /// <!-- @default auto -->
    pub region: Option<String>,
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
    /// Send requests without signing them.
    ///
    /// This option cannot be combined with direct credentials.
    ///
    /// <!-- @group Credentials -->
    /// <!-- @default false -->
    pub skip_signature: bool,
}

impl Debug for MinioConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MinioConfig")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("skip_signature", &self.skip_signature)
            .finish_non_exhaustive()
    }
}

impl Configurator for MinioConfig {
    type Builder = MinioBuilder;

    fn from_uri(uri: &OperatorUri) -> Result<Self> {
        preset::from_uri(uri)
    }

    fn into_builder(self) -> Self::Builder {
        MinioBuilder::from_config(self)
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
            "minio://example-bucket/path/to/root?endpoint=http%3A%2F%2F127.0.0.1%3A9000",
            iter::empty(),
        )
        .unwrap();
        let config = MinioConfig::from_uri(&uri).unwrap();

        assert_eq!(config.bucket, "example-bucket");
        assert_eq!(config.root.as_deref(), Some("path/to/root"));
        assert_eq!(config.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
    }

    #[test]
    fn rejects_unknown_fields() {
        let err =
            MinioConfig::from_iter([("role_arn".to_string(), "role".to_string())]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn debug_redacts_credentials() {
        let config = MinioConfig {
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
