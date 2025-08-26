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

/// Azure Storage Blob services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct AzblobConfig {
    /// The root of Azblob service backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,

    /// The container name of Azblob service backend.
    #[serde(alias = "azure_container_name", alias = "container_name")]
    pub container: String,

    /// The endpoint of Azblob service backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - Azblob: `https://accountname.blob.core.windows.net`
    /// - Azurite: `http://127.0.0.1:10000/devstoreaccount1`
    /// - To use Microsoft Fabric endpoint, there is also shortcut `use_fabric_endpoint`.
    #[serde(alias = "azure_storage_endpoint", alias = "azure_endpoint")]
    pub endpoint: Option<String>,

    /// The account name of Azblob service backend.
    #[serde(alias = "azure_storage_account_name")]
    pub account_name: Option<String>,

    /// The account key of Azblob service backend.
    #[serde(
        alias = "azure_storage_account_key",
        alias = "azure_storage_access_key",
        alias = "azure_storage_master_key",
        alias = "access_key",
        alias = "master_key"
    )]
    pub account_key: Option<String>,

    /// The encryption key of Azblob service backend.
    pub encryption_key: Option<String>,

    /// The encryption key sha256 of Azblob service backend.
    pub encryption_key_sha256: Option<String>,

    /// The encryption algorithm of Azblob service backend.
    pub encryption_algorithm: Option<String>,

    /// The sas token of Azblob service backend.
    #[serde(
        alias = "azure_storage_sas_key",
        alias = "azure_storage_sas_token",
        alias = "sas_key"
    )]
    pub sas_token: Option<String>,

    /// Use Microsoft Fabric url scheme.
    /// When enabled uses https://{account}.dfs.fabric.microsoft.com
    ///
    /// Supported keys:
    /// - `azure_use_fabric_endpoint`
    /// - `use_fabric_endpoint`
    #[serde(alias = "azure_use_fabric_endpoint", default)]
    pub use_fabric_endpoint: bool,

    /// The maximum batch operations of Azblob service backend.
    pub batch_max_operations: Option<usize>,
}

impl Debug for AzblobConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("AzblobConfig");

        ds.field("root", &self.root);
        ds.field("container", &self.container);
        ds.field("endpoint", &self.endpoint);
        if self.use_fabric_endpoint {
            ds.field("use_fabric_endpoint", &self.use_fabric_endpoint);
        }

        if self.account_name.is_some() {
            ds.field("account_name", &"<redacted>");
        }
        if self.account_key.is_some() {
            ds.field("account_key", &"<redacted>");
        }
        if self.sas_token.is_some() {
            ds.field("sas_token", &"<redacted>");
        }

        ds.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_container_name_aliases() {
        // Test original container field
        let json = r#"{"container": "test-container"}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.container, "test-container");

        // Test azure_container_name alias
        let json = r#"{"azure_container_name": "test-container"}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.container, "test-container");

        // Test container_name alias
        let json = r#"{"container_name": "test-container"}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.container, "test-container");
    }

    #[test]
    fn test_account_name_aliases() {
        // Test original account_name field
        let json = r#"{"container": "test", "account_name": "testaccount"}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.account_name, Some("testaccount".to_string()));

        // Test azure_storage_account_name alias
        let json = r#"{"container": "test", "azure_storage_account_name": "testaccount"}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.account_name, Some("testaccount".to_string()));
    }

    #[test]
    fn test_account_key_aliases() {
        // Test original account_key field
        let json = r#"{"container": "test", "account_key": "dGVzdGtleQ=="}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.account_key, Some("dGVzdGtleQ==".to_string()));

        // Test azure_storage_account_key alias
        let json = r#"{"container": "test", "azure_storage_account_key": "dGVzdGtleQ=="}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.account_key, Some("dGVzdGtleQ==".to_string()));

        // Test azure_storage_access_key alias
        let json = r#"{"container": "test", "azure_storage_access_key": "dGVzdGtleQ=="}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.account_key, Some("dGVzdGtleQ==".to_string()));

        // Test azure_storage_master_key alias
        let json = r#"{"container": "test", "azure_storage_master_key": "dGVzdGtleQ=="}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.account_key, Some("dGVzdGtleQ==".to_string()));

        // Test access_key alias
        let json = r#"{"container": "test", "access_key": "dGVzdGtleQ=="}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.account_key, Some("dGVzdGtleQ==".to_string()));

        // Test master_key alias
        let json = r#"{"container": "test", "master_key": "dGVzdGtleQ=="}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.account_key, Some("dGVzdGtleQ==".to_string()));
    }

    #[test]
    fn test_sas_token_aliases() {
        // Test original sas_token field
        let json = r#"{"container": "test", "sas_token": "test-token"}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.sas_token, Some("test-token".to_string()));

        // Test azure_storage_sas_key alias
        let json = r#"{"container": "test", "azure_storage_sas_key": "test-token"}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.sas_token, Some("test-token".to_string()));

        // Test azure_storage_sas_token alias
        let json = r#"{"container": "test", "azure_storage_sas_token": "test-token"}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.sas_token, Some("test-token".to_string()));

        // Test sas_key alias
        let json = r#"{"container": "test", "sas_key": "test-token"}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.sas_token, Some("test-token".to_string()));
    }

    #[test]
    fn test_endpoint_aliases() {
        // Test original endpoint field
        let json = r#"{"container": "test", "endpoint": "https://test.blob.core.windows.net"}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.endpoint,
            Some("https://test.blob.core.windows.net".to_string())
        );

        // Test azure_storage_endpoint alias
        let json = r#"{"container": "test", "azure_storage_endpoint": "https://test.blob.core.windows.net"}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.endpoint,
            Some("https://test.blob.core.windows.net".to_string())
        );

        // Test azure_endpoint alias
        let json =
            r#"{"container": "test", "azure_endpoint": "https://test.blob.core.windows.net"}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.endpoint,
            Some("https://test.blob.core.windows.net".to_string())
        );
    }

    #[test]
    fn test_use_fabric_endpoint_aliases() {
        // Test original use_fabric_endpoint field
        let json = r#"{"container": "test", "use_fabric_endpoint": true}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert!(config.use_fabric_endpoint);

        // Test azure_use_fabric_endpoint alias
        let json = r#"{"container": "test", "azure_use_fabric_endpoint": true}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert!(config.use_fabric_endpoint);

        // Test default value (should be false)
        let json = r#"{"container": "test"}"#;
        let config: AzblobConfig = serde_json::from_str(json).unwrap();
        assert!(!config.use_fabric_endpoint);
    }
}
