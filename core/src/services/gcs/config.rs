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
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

/// [Google Cloud Storage](https://cloud.google.com/storage) services support.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct GcsConfig {
    /// root URI, all operations happens under `root`
    pub root: Option<String>,
    /// bucket name
    pub bucket: String,
    /// endpoint URI of GCS service,
    /// default is `https://storage.googleapis.com`
    pub endpoint: Option<String>,
    /// Scope for gcs.
    pub scope: Option<String>,
    /// Service Account for gcs.
    pub service_account: Option<String>,
    /// Credentials string for GCS service OAuth2 authentication.
    pub credential: Option<String>,
    /// Local path to credentials file for GCS service OAuth2 authentication.
    pub credential_path: Option<String>,
    /// The predefined acl for GCS.
    pub predefined_acl: Option<String>,
    /// The default storage class used by gcs.
    pub default_storage_class: Option<String>,
    /// Allow opendal to send requests without signing when credentials are not
    /// loaded.
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

    /// Only use HTTP/1 connections
    #[serde(alias = "google_http1_only", alias = "gcs_http1_only")]
    pub http1_only: bool,

    /// Only use HTTP/2 connections  
    #[serde(alias = "google_http2_only", alias = "gcs_http2_only")]
    pub http2_only: bool,

    /// Interval for HTTP/2 Ping frames to keep connections alive
    #[serde(
        alias = "google_http2_keep_alive_interval",
        alias = "gcs_http2_keep_alive_interval"
    )]
    pub http2_keep_alive_interval: Option<Duration>,

    /// Timeout for receiving acknowledgement of HTTP/2 keep-alive pings
    #[serde(
        alias = "google_http2_keep_alive_timeout",
        alias = "gcs_http2_keep_alive_timeout"
    )]
    pub http2_keep_alive_timeout: Option<Duration>,

    /// Enable HTTP/2 keep-alive pings for idle connections
    #[serde(
        alias = "google_http2_keep_alive_while_idle",
        alias = "gcs_http2_keep_alive_while_idle"
    )]
    pub http2_keep_alive_while_idle: bool,

    /// Maximum frame size for HTTP/2 connections
    #[serde(
        alias = "google_http2_max_frame_size",
        alias = "gcs_http2_max_frame_size"
    )]
    pub http2_max_frame_size: Option<u32>,

    /// Allow HTTP connections (defaults to true for GCS compatibility)
    #[serde(alias = "google_allow_http", alias = "gcs_allow_http")]
    pub allow_http: bool,

    /// Randomize order of resolved addresses (defaults to true)
    #[serde(
        alias = "google_randomize_addresses",
        alias = "gcs_randomize_addresses"
    )]
    pub randomize_addresses: bool,
}

impl Default for GcsConfig {
    fn default() -> Self {
        Self {
            root: None,
            bucket: String::default(),
            endpoint: None,
            scope: None,
            service_account: None,
            credential: None,
            credential_path: None,
            predefined_acl: None,
            default_storage_class: None,
            allow_anonymous: false,
            disable_vm_metadata: false,
            disable_config_load: false,
            token: None,
            http1_only: false,
            http2_only: false,
            http2_keep_alive_interval: None,
            http2_keep_alive_timeout: None,
            http2_keep_alive_while_idle: false,
            http2_max_frame_size: None,
            // Default to true for GCS compatibility (like arrow-rs-object-store)
            allow_http: true,
            // Default to true for better connection distribution
            randomize_addresses: true,
        }
    }
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
    use std::time::Duration;

    #[test]
    fn test_gcs_config_defaults() {
        let config = GcsConfig::default();
        assert!(config.allow_http); // Should default to true for GCS compatibility
        assert!(config.randomize_addresses); // Should default to true
        assert!(!config.http1_only);
        assert!(!config.http2_only);
        assert!(!config.http2_keep_alive_while_idle);
        assert_eq!(config.http2_keep_alive_interval, None);
        assert_eq!(config.http2_keep_alive_timeout, None);
        assert_eq!(config.http2_max_frame_size, None);
        assert_eq!(config.bucket, "");
        assert!(!config.allow_anonymous);
    }

    #[test]
    fn test_http1_only_alias() {
        // Test google_ alias
        let json = r#"{"bucket": "test-bucket", "google_http1_only": true}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert!(config.http1_only);

        // Test gcs_ alias
        let json = r#"{"bucket": "test-bucket", "gcs_http1_only": true}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert!(config.http1_only);
    }

    #[test]
    fn test_http2_only_alias() {
        // Test google_ alias
        let json = r#"{"bucket": "test-bucket", "google_http2_only": true}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert!(config.http2_only);

        // Test gcs_ alias
        let json = r#"{"bucket": "test-bucket", "gcs_http2_only": true}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert!(config.http2_only);
    }

    #[test]
    fn test_http2_keep_alive_interval_alias() {
        // Test google_ alias
        let json = r#"{"bucket": "test-bucket", "google_http2_keep_alive_interval": {"secs": 30, "nanos": 0}}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.http2_keep_alive_interval,
            Some(Duration::from_secs(30))
        );

        // Test gcs_ alias
        let json = r#"{"bucket": "test-bucket", "gcs_http2_keep_alive_interval": {"secs": 45, "nanos": 0}}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.http2_keep_alive_interval,
            Some(Duration::from_secs(45))
        );
    }

    #[test]
    fn test_http2_keep_alive_timeout_alias() {
        // Test google_ alias
        let json = r#"{"bucket": "test-bucket", "google_http2_keep_alive_timeout": {"secs": 5, "nanos": 0}}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.http2_keep_alive_timeout,
            Some(Duration::from_secs(5))
        );

        // Test gcs_ alias
        let json = r#"{"bucket": "test-bucket", "gcs_http2_keep_alive_timeout": {"secs": 10, "nanos": 0}}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.http2_keep_alive_timeout,
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn test_http2_keep_alive_while_idle_alias() {
        // Test google_ alias
        let json = r#"{"bucket": "test-bucket", "google_http2_keep_alive_while_idle": true}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert!(config.http2_keep_alive_while_idle);

        // Test gcs_ alias
        let json = r#"{"bucket": "test-bucket", "gcs_http2_keep_alive_while_idle": true}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert!(config.http2_keep_alive_while_idle);
    }

    #[test]
    fn test_http2_max_frame_size_alias() {
        // Test google_ alias
        let json = r#"{"bucket": "test-bucket", "google_http2_max_frame_size": 32768}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.http2_max_frame_size, Some(32768));

        // Test gcs_ alias
        let json = r#"{"bucket": "test-bucket", "gcs_http2_max_frame_size": 16384}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.http2_max_frame_size, Some(16384));
    }

    #[test]
    fn test_allow_http_alias() {
        // Test google_ alias
        let json = r#"{"bucket": "test-bucket", "google_allow_http": false}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert!(!config.allow_http);

        // Test gcs_ alias
        let json = r#"{"bucket": "test-bucket", "gcs_allow_http": false}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert!(!config.allow_http);
    }

    #[test]
    fn test_randomize_addresses_alias() {
        // Test google_ alias
        let json = r#"{"bucket": "test-bucket", "google_randomize_addresses": false}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert!(!config.randomize_addresses);

        // Test gcs_ alias
        let json = r#"{"bucket": "test-bucket", "gcs_randomize_addresses": false}"#;
        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert!(!config.randomize_addresses);
    }

    #[test]
    fn test_config_with_multiple_aliases() {
        let json = r#"{
            "bucket": "test-bucket",
            "google_http1_only": true,
            "gcs_http2_keep_alive_interval": {"secs": 30, "nanos": 0},
            "google_allow_http": false
        }"#;

        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.bucket, "test-bucket");
        assert!(config.http1_only);
        assert_eq!(
            config.http2_keep_alive_interval,
            Some(Duration::from_secs(30))
        );
        assert!(!config.allow_http);
    }

    #[test]
    fn test_original_field_names() {
        let json = r#"{
            "bucket": "test-bucket",
            "http1_only": true,
            "allow_http": false,
            "randomize_addresses": false
        }"#;

        let config: GcsConfig = serde_json::from_str(json).unwrap();
        assert!(config.http1_only);
        assert!(!config.allow_http);
        assert!(!config.randomize_addresses);
    }
}
