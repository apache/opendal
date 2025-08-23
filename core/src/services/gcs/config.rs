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

use serde::Deserialize;
use serde::Serialize;

/// [Google Cloud Storage](https://cloud.google.com/storage) services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct GcsConfig {
    /// root URI, all operations happens under `root`
    pub root: Option<String>,
    /// bucket name
    #[serde(alias = "google_bucket", alias = "google_bucket_name", alias = "bucket_name")]
    pub bucket: String,
    /// endpoint URI of GCS service,
    /// default is `https://storage.googleapis.com`
    pub endpoint: Option<String>,
    /// Scope for gcs.
    pub scope: Option<String>,
    /// Service Account for gcs.
    #[serde(alias = "google_service_account", alias = "google_service_account_path", alias = "service_account_path")]
    pub service_account: Option<String>,
    /// Credentials string for GCS service OAuth2 authentication.
    #[serde(alias = "google_service_account_key", alias = "service_account_key")]
    pub credential: Option<String>,
    /// Local path to credentials file for GCS service OAuth2 authentication.
    #[serde(alias = "google_application_credentials")]
    pub credential_path: Option<String>,
    /// The predefined acl for GCS.
    pub predefined_acl: Option<String>,
    /// The default storage class used by gcs.
    pub default_storage_class: Option<String>,
    /// Allow opendal to send requests without signing when credentials are not
    /// loaded.
    #[serde(alias = "google_skip_signature", alias = "skip_signature")]
    pub allow_anonymous: bool,
    /// Disable attempting to load credentials from the GCE metadata server when
    /// running within Google Cloud.
    pub disable_vm_metadata: bool,
    /// Disable loading configuration from the environment.
    pub disable_config_load: bool,
    /// A Google Cloud OAuth2 token.
    ///
    /// Takes precedence over `credential` and `credential_path`.
    pub token: Option<String>,
}

impl Debug for GcsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsConfig")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_aliases() {
        // Test bucket aliases
        let bucket_config = r#"{"google_bucket": "test-bucket"}"#;
        let config: GcsConfig = serde_json::from_str(bucket_config).unwrap();
        assert_eq!("test-bucket", config.bucket);

        let bucket_name_config = r#"{"google_bucket_name": "test-bucket-name"}"#;
        let config: GcsConfig = serde_json::from_str(bucket_name_config).unwrap();
        assert_eq!("test-bucket-name", config.bucket);

        let bucket_alias_config = r#"{"bucket_name": "test-bucket-alias"}"#;
        let config: GcsConfig = serde_json::from_str(bucket_alias_config).unwrap();
        assert_eq!("test-bucket-alias", config.bucket);

        // Test service account aliases
        let sa_config = r#"{"google_service_account": "/path/to/sa.json"}"#;
        let config: GcsConfig = serde_json::from_str(sa_config).unwrap();
        assert_eq!(Some("/path/to/sa.json".to_string()), config.service_account);

        let sa_path_config = r#"{"service_account_path": "/path/to/sa2.json"}"#;
        let config: GcsConfig = serde_json::from_str(sa_path_config).unwrap();
        assert_eq!(Some("/path/to/sa2.json".to_string()), config.service_account);

        // Test credential aliases  
        let cred_config = r#"{"google_service_account_key": "key-content"}"#;
        let config: GcsConfig = serde_json::from_str(cred_config).unwrap();
        assert_eq!(Some("key-content".to_string()), config.credential);

        let cred_alias_config = r#"{"service_account_key": "key-content-2"}"#;
        let config: GcsConfig = serde_json::from_str(cred_alias_config).unwrap();
        assert_eq!(Some("key-content-2".to_string()), config.credential);

        // Test credential_path aliases
        let app_cred_config = r#"{"google_application_credentials": "/path/to/app.json"}"#;
        let config: GcsConfig = serde_json::from_str(app_cred_config).unwrap();
        assert_eq!(Some("/path/to/app.json".to_string()), config.credential_path);

        // Test allow_anonymous aliases
        let skip_sig_config = r#"{"google_skip_signature": true}"#;
        let config: GcsConfig = serde_json::from_str(skip_sig_config).unwrap();
        assert!(config.allow_anonymous);

        let skip_alias_config = r#"{"skip_signature": true}"#;
        let config: GcsConfig = serde_json::from_str(skip_alias_config).unwrap();
        assert!(config.allow_anonymous);
    }
}
