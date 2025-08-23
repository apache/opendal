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

    /// The maximum batch operations of Azblob service backend.
    pub batch_max_operations: Option<usize>,
}

impl Debug for AzblobConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("AzblobConfig");

        ds.field("root", &self.root);
        ds.field("container", &self.container);
        ds.field("endpoint", &self.endpoint);

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
    fn test_config_aliases() {
        // Test container name aliases
        let json1 = r#"{"container": "test-container"}"#;
        let config1: AzblobConfig = serde_json::from_str(json1).unwrap();
        assert_eq!(config1.container, "test-container");

        let json2 = r#"{"azure_container_name": "test-container"}"#;
        let config2: AzblobConfig = serde_json::from_str(json2).unwrap();
        assert_eq!(config2.container, "test-container");

        let json3 = r#"{"container_name": "test-container"}"#;
        let config3: AzblobConfig = serde_json::from_str(json3).unwrap();
        assert_eq!(config3.container, "test-container");

        // Test account name aliases
        let json4 = r#"{"container": "test", "account_name": "testaccount"}"#;
        let config4: AzblobConfig = serde_json::from_str(json4).unwrap();
        assert_eq!(config4.account_name, Some("testaccount".to_string()));

        let json5 = r#"{"container": "test", "azure_storage_account_name": "testaccount"}"#;
        let config5: AzblobConfig = serde_json::from_str(json5).unwrap();
        assert_eq!(config5.account_name, Some("testaccount".to_string()));

        // Test account key aliases
        let json6 = r#"{"container": "test", "account_key": "dGVzdGtleQ=="}"#;
        let config6: AzblobConfig = serde_json::from_str(json6).unwrap();
        assert_eq!(config6.account_key, Some("dGVzdGtleQ==".to_string()));

        let json7 = r#"{"container": "test", "azure_storage_account_key": "dGVzdGtleQ=="}"#;
        let config7: AzblobConfig = serde_json::from_str(json7).unwrap();
        assert_eq!(config7.account_key, Some("dGVzdGtleQ==".to_string()));

        let json8 = r#"{"container": "test", "access_key": "dGVzdGtleQ=="}"#;
        let config8: AzblobConfig = serde_json::from_str(json8).unwrap();
        assert_eq!(config8.account_key, Some("dGVzdGtleQ==".to_string()));

        // Test SAS token aliases
        let json9 = r#"{"container": "test", "sas_token": "test-token"}"#;
        let config9: AzblobConfig = serde_json::from_str(json9).unwrap();
        assert_eq!(config9.sas_token, Some("test-token".to_string()));

        let json10 = r#"{"container": "test", "azure_storage_sas_key": "test-token"}"#;
        let config10: AzblobConfig = serde_json::from_str(json10).unwrap();
        assert_eq!(config10.sas_token, Some("test-token".to_string()));

        let json11 = r#"{"container": "test", "sas_key": "test-token"}"#;
        let config11: AzblobConfig = serde_json::from_str(json11).unwrap();
        assert_eq!(config11.sas_token, Some("test-token".to_string()));

        // Test endpoint aliases
        let json12 = r#"{"container": "test", "endpoint": "https://test.blob.core.windows.net"}"#;
        let config12: AzblobConfig = serde_json::from_str(json12).unwrap();
        assert_eq!(
            config12.endpoint,
            Some("https://test.blob.core.windows.net".to_string())
        );

        let json13 = r#"{"container": "test", "azure_storage_endpoint": "https://test.blob.core.windows.net"}"#;
        let config13: AzblobConfig = serde_json::from_str(json13).unwrap();
        assert_eq!(
            config13.endpoint,
            Some("https://test.blob.core.windows.net".to_string())
        );
    }
}
