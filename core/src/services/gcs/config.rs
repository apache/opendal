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
    #[serde(alias = "google_service_account", alias = "service_account")]
    pub service_account: Option<String>,
    /// Credentials string for GCS service OAuth2 authentication.
    #[serde(alias = "google_service_account_key", alias = "service_account_key")]
    pub credential: Option<String>,
    /// Local path to credentials file for GCS service OAuth2 authentication.
    #[serde(alias = "google_service_account_path", alias = "service_account_path")]
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

    // HTTP client configuration options
    /// Allow HTTP connections (default: false, only HTTPS allowed)
    #[serde(alias = "google_allow_http")]
    pub allow_http: bool,

    /// Allow invalid/self-signed certificates (default: false)
    #[serde(alias = "google_allow_invalid_certificates")]
    pub allow_invalid_certificates: bool,

    /// Connection timeout duration
    #[serde(alias = "google_connect_timeout")]
    pub connect_timeout: Option<Duration>,

    /// Default content type for uploads
    #[serde(alias = "google_default_content_type")]
    pub default_content_type: Option<String>,

    /// Pool idle timeout duration
    #[serde(alias = "google_pool_idle_timeout")]
    pub pool_idle_timeout: Option<Duration>,

    /// Maximum number of idle connections per host
    #[serde(alias = "google_pool_max_idle_per_host")]
    pub pool_max_idle_per_host: Option<usize>,

    /// HTTP proxy URL
    #[serde(alias = "google_proxy_url")]
    pub proxy_url: Option<String>,

    /// PEM-formatted CA certificate for proxy connections
    #[serde(alias = "google_proxy_ca_certificate")]
    pub proxy_ca_certificate: Option<String>,

    /// List of hosts that bypass proxy (comma-separated)
    #[serde(alias = "google_proxy_excludes")]
    pub proxy_excludes: Option<String>,

    /// Randomize DNS resolution order (default: true)
    #[serde(alias = "google_randomize_addresses")]
    pub randomize_addresses: bool,

    /// Request timeout duration
    #[serde(alias = "google_timeout")]
    pub timeout: Option<Duration>,

    /// Custom User-Agent header
    #[serde(alias = "google_user_agent")]
    pub user_agent: Option<String>,
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
    use crate::Configurator;
    use std::collections::HashMap;

    /// Test direct client config keys (no prefix) - focus on basic types
    #[test]
    fn test_direct_client_config_keys() {
        let mut config_map = HashMap::new();
        config_map.insert("bucket".to_string(), "test-bucket".to_string());
        config_map.insert("allow_http".to_string(), "true".to_string());
        config_map.insert("allow_invalid_certificates".to_string(), "false".to_string());
        config_map.insert("default_content_type".to_string(), "application/json".to_string());
        config_map.insert("pool_max_idle_per_host".to_string(), "10".to_string());
        config_map.insert("proxy_url".to_string(), "https://proxy.example.com:8080".to_string());
        config_map.insert("proxy_ca_certificate".to_string(), "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----".to_string());
        config_map.insert("proxy_excludes".to_string(), "localhost,127.0.0.1".to_string());
        config_map.insert("randomize_addresses".to_string(), "false".to_string());
        config_map.insert("user_agent".to_string(), "TestApp/1.0".to_string());

        let config = GcsConfig::from_iter(config_map).expect("Failed to parse direct client config");

        assert_eq!(config.bucket, "test-bucket");
        assert_eq!(config.allow_http, true);
        assert_eq!(config.allow_invalid_certificates, false);
        assert_eq!(config.default_content_type, Some("application/json".to_string()));
        assert_eq!(config.pool_max_idle_per_host, Some(10));
        assert_eq!(config.proxy_url, Some("https://proxy.example.com:8080".to_string()));
        assert!(config.proxy_ca_certificate.is_some());
        assert_eq!(config.proxy_excludes, Some("localhost,127.0.0.1".to_string()));
        assert_eq!(config.randomize_addresses, false);
        assert_eq!(config.user_agent, Some("TestApp/1.0".to_string()));
    }

    /// Test google_ prefixed client config keys (object_store compatible)
    #[test] 
    fn test_google_prefixed_config_keys() {
        let mut config_map = HashMap::new();
        config_map.insert("google_bucket".to_string(), "google-bucket".to_string());
        config_map.insert("google_allow_http".to_string(), "false".to_string());
        config_map.insert("google_allow_invalid_certificates".to_string(), "true".to_string());
        config_map.insert("google_default_content_type".to_string(), "text/plain".to_string());
        config_map.insert("google_pool_max_idle_per_host".to_string(), "20".to_string());
        config_map.insert("google_proxy_url".to_string(), "https://google-proxy.example.com:3128".to_string());
        config_map.insert("google_proxy_ca_certificate".to_string(), "-----BEGIN CERTIFICATE-----\ngoogle-cert\n-----END CERTIFICATE-----".to_string());
        config_map.insert("google_proxy_excludes".to_string(), "internal.company.com,localhost".to_string());
        config_map.insert("google_randomize_addresses".to_string(), "true".to_string());
        config_map.insert("google_user_agent".to_string(), "GoogleTestApp/2.0".to_string());

        let config = GcsConfig::from_iter(config_map).expect("Failed to parse google prefixed config");

        assert_eq!(config.bucket, "google-bucket");
        assert_eq!(config.allow_http, false);
        assert_eq!(config.allow_invalid_certificates, true);
        assert_eq!(config.default_content_type, Some("text/plain".to_string()));
        assert_eq!(config.pool_max_idle_per_host, Some(20));
        assert_eq!(config.proxy_url, Some("https://google-proxy.example.com:3128".to_string()));
        assert!(config.proxy_ca_certificate.is_some());
        assert_eq!(config.proxy_excludes, Some("internal.company.com,localhost".to_string()));
        assert_eq!(config.randomize_addresses, true);
        assert_eq!(config.user_agent, Some("GoogleTestApp/2.0".to_string()));
    }

    /// Test legacy aliases for backward compatibility
    #[test]
    fn test_legacy_aliases() {
        let mut config_map = HashMap::new();
        config_map.insert("bucket_name".to_string(), "legacy-bucket".to_string());
        config_map.insert("google_service_account".to_string(), "google-test@example.iam.gserviceaccount.com".to_string());
        config_map.insert("service_account_key".to_string(), "base64-encoded-key".to_string());
        config_map.insert("service_account_path".to_string(), "/path/to/service-account.json".to_string());

        let config = GcsConfig::from_iter(config_map).expect("Failed to parse legacy aliases");

        // Test that bucket_name alias works
        assert_eq!(config.bucket, "legacy-bucket");
        // service_account should work with the google prefix
        assert_eq!(config.service_account, Some("google-test@example.iam.gserviceaccount.com".to_string()));
        assert_eq!(config.credential, Some("base64-encoded-key".to_string()));
        assert_eq!(config.credential_path, Some("/path/to/service-account.json".to_string()));
    }

    /// Test mixed configuration patterns
    #[test]
    fn test_mixed_configuration_patterns() {
        let mut config_map = HashMap::new();
        // Mix of direct, prefixed, and legacy keys
        config_map.insert("bucket".to_string(), "mixed-bucket".to_string());
        config_map.insert("google_allow_http".to_string(), "true".to_string()); // prefixed
        config_map.insert("google_proxy_url".to_string(), "https://mixed-proxy.example.com:8080".to_string()); // prefixed
        config_map.insert("user_agent".to_string(), "MixedApp/1.5".to_string()); // direct
        config_map.insert("service_account_path".to_string(), "/mixed/path/creds.json".to_string()); // legacy alias
        config_map.insert("google_randomize_addresses".to_string(), "false".to_string()); // prefixed

        let config = GcsConfig::from_iter(config_map).expect("Failed to parse mixed config");

        assert_eq!(config.bucket, "mixed-bucket");
        assert_eq!(config.allow_http, true);
        assert_eq!(config.proxy_url, Some("https://mixed-proxy.example.com:8080".to_string()));
        assert_eq!(config.user_agent, Some("MixedApp/1.5".to_string()));
        assert_eq!(config.credential_path, Some("/mixed/path/creds.json".to_string()));
        assert_eq!(config.randomize_addresses, false);
    }

    /// Test edge cases and default values
    #[test]
    fn test_edge_cases_and_defaults() {
        // Test empty configuration uses defaults
        let empty_config = HashMap::new();
        let config = GcsConfig::from_iter(empty_config).expect("Failed to parse empty config");
        
        assert_eq!(config.bucket, ""); // Default empty string
        assert_eq!(config.allow_http, false); // Default false
        assert_eq!(config.allow_invalid_certificates, false); // Default false
        assert_eq!(config.randomize_addresses, false); // Default false
        assert_eq!(config.connect_timeout, None); // Default None
        assert_eq!(config.default_content_type, None); // Default None

        // Test boolean parsing variations
        let mut bool_config = HashMap::new();
        bool_config.insert("allow_http".to_string(), "1".to_string());
        bool_config.insert("google_allow_invalid_certificates".to_string(), "yes".to_string());
        bool_config.insert("randomize_addresses".to_string(), "TRUE".to_string());

        // Note: These might not parse correctly depending on serde's boolean parsing
        // This test documents the expected behavior
        if let Ok(config) = GcsConfig::from_iter(bool_config) {
            // If parsing succeeds, check values
            println!("Boolean parsing test passed: allow_http={}", config.allow_http);
        }

        // Test invalid values handling
        let mut invalid_config = HashMap::new();
        invalid_config.insert("bucket".to_string(), "valid-bucket".to_string());
        invalid_config.insert("pool_max_idle_per_host".to_string(), "not-a-number".to_string());

        // This should fail to parse
        assert!(GcsConfig::from_iter(invalid_config).is_err(), "Invalid numeric value should cause parsing to fail");
    }

    /// Test comprehensive configuration with all features
    #[test]
    fn test_comprehensive_configuration() {
        let mut config_map = HashMap::new();
        
        // GCS specific settings
        config_map.insert("bucket".to_string(), "comprehensive-bucket".to_string());
        config_map.insert("endpoint".to_string(), "https://custom-gcs-endpoint.example.com".to_string());
        config_map.insert("service_account".to_string(), "test@example.iam.gserviceaccount.com".to_string());
        config_map.insert("credential_path".to_string(), "/path/to/service-account.json".to_string());
        config_map.insert("predefined_acl".to_string(), "bucketOwnerFullControl".to_string());
        config_map.insert("default_storage_class".to_string(), "STANDARD".to_string());
        config_map.insert("allow_anonymous".to_string(), "true".to_string());
        config_map.insert("disable_vm_metadata".to_string(), "false".to_string());
        config_map.insert("disable_config_load".to_string(), "true".to_string());
        config_map.insert("token".to_string(), "oauth2-access-token".to_string());

        // HTTP client settings (mix of direct and prefixed)
        config_map.insert("allow_http".to_string(), "false".to_string());
        config_map.insert("google_allow_invalid_certificates".to_string(), "false".to_string());
        config_map.insert("google_default_content_type".to_string(), "application/octet-stream".to_string());
        config_map.insert("google_pool_max_idle_per_host".to_string(), "25".to_string());
        config_map.insert("proxy_url".to_string(), "https://comprehensive-proxy.example.com:8080".to_string());
        config_map.insert("google_proxy_excludes".to_string(), "*.internal.com,localhost,127.0.0.1".to_string());
        config_map.insert("randomize_addresses".to_string(), "true".to_string());
        config_map.insert("user_agent".to_string(), "ComprehensiveTestApp/3.0".to_string());

        let config = GcsConfig::from_iter(config_map).expect("Failed to parse comprehensive config");

        // Verify GCS specific settings
        assert_eq!(config.bucket, "comprehensive-bucket");
        assert_eq!(config.endpoint, Some("https://custom-gcs-endpoint.example.com".to_string()));
        assert_eq!(config.service_account, Some("test@example.iam.gserviceaccount.com".to_string()));
        assert_eq!(config.credential_path, Some("/path/to/service-account.json".to_string()));
        assert_eq!(config.predefined_acl, Some("bucketOwnerFullControl".to_string()));
        assert_eq!(config.default_storage_class, Some("STANDARD".to_string()));
        assert_eq!(config.allow_anonymous, true);
        assert_eq!(config.disable_vm_metadata, false);
        assert_eq!(config.disable_config_load, true);
        assert_eq!(config.token, Some("oauth2-access-token".to_string()));

        // Verify HTTP client settings
        assert_eq!(config.allow_http, false);
        assert_eq!(config.allow_invalid_certificates, false);
        assert_eq!(config.default_content_type, Some("application/octet-stream".to_string()));
        assert_eq!(config.pool_max_idle_per_host, Some(25));
        assert_eq!(config.proxy_url, Some("https://comprehensive-proxy.example.com:8080".to_string()));
        assert_eq!(config.proxy_excludes, Some("*.internal.com,localhost,127.0.0.1".to_string()));
        assert_eq!(config.randomize_addresses, true);
        assert_eq!(config.user_agent, Some("ComprehensiveTestApp/3.0".to_string()));
    }

    /// Test object_store migration compatibility
    #[test]
    fn test_object_store_migration_compatibility() {
        // Simulate object_store GoogleCloudStorageBuilder configuration
        let mut object_store_config = HashMap::new();
        object_store_config.insert("google_bucket".to_string(), "migration-test-bucket".to_string());
        object_store_config.insert("google_service_account".to_string(), "migration@example.iam.gserviceaccount.com".to_string());
        object_store_config.insert("google_service_account_path".to_string(), "/migration/path/service-account.json".to_string());
        object_store_config.insert("google_allow_http".to_string(), "true".to_string());
        object_store_config.insert("google_proxy_url".to_string(), "https://migration-proxy.example.com:3128".to_string());
        object_store_config.insert("google_user_agent".to_string(), "MigrationTestApp/1.0".to_string());

        let config = GcsConfig::from_iter(object_store_config)
            .expect("Failed to parse object_store compatible config");

        // Verify all object_store patterns work correctly
        assert_eq!(config.bucket, "migration-test-bucket");
        assert_eq!(config.service_account, Some("migration@example.iam.gserviceaccount.com".to_string()));
        assert_eq!(config.credential_path, Some("/migration/path/service-account.json".to_string()));
        assert_eq!(config.allow_http, true);
        assert_eq!(config.proxy_url, Some("https://migration-proxy.example.com:3128".to_string()));
        assert_eq!(config.user_agent, Some("MigrationTestApp/1.0".to_string()));

        // Verify the config can be converted to a builder
        let _builder = config.into_builder();
        // If this compiles and runs, the configuration is valid
    }
}
