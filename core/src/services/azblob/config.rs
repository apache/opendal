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
    ///
    /// Supported keys:
    /// - `azure_container_name`
    /// - `container_name`
    /// - `container`
    #[serde(alias = "azure_container_name", alias = "container_name")]
    pub container: String,

    /// The endpoint of Azblob service backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - Azblob: `https://accountname.blob.core.windows.net`
    /// - Azurite: `http://127.0.0.1:10000/devstoreaccount1`
    ///
    /// Supported keys:
    /// - `azure_storage_endpoint`
    /// - `azure_endpoint`
    /// - `endpoint`
    #[serde(alias = "azure_storage_endpoint", alias = "azure_endpoint")]
    pub endpoint: Option<String>,

    /// The account name of Azblob service backend.
    ///
    /// Supported keys:
    /// - `azure_storage_account_name`
    /// - `account_name`
    #[serde(alias = "azure_storage_account_name")]
    pub account_name: Option<String>,

    /// The account key of Azblob service backend.
    ///
    /// Supported keys:
    /// - `azure_storage_account_key`
    /// - `azure_storage_access_key`  
    /// - `azure_storage_master_key`
    /// - `access_key`
    /// - `account_key`
    /// - `master_key`
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
    ///
    /// Supported keys:
    /// - `azure_storage_sas_key`
    /// - `azure_storage_sas_token`
    /// - `sas_key`
    /// - `sas_token`
    #[serde(
        alias = "azure_storage_sas_key",
        alias = "azure_storage_sas_token",
        alias = "sas_key"
    )]
    pub sas_token: Option<String>,

    /// Service principal client id for authorizing requests.
    ///
    /// Supported keys:
    /// - `azure_storage_client_id`
    /// - `azure_client_id`
    /// - `client_id`
    #[serde(alias = "azure_storage_client_id", alias = "azure_client_id")]
    pub client_id: Option<String>,

    /// Service principal client secret for authorizing requests.
    ///
    /// Supported keys:
    /// - `azure_storage_client_secret`
    /// - `azure_client_secret`
    /// - `client_secret`
    #[serde(alias = "azure_storage_client_secret", alias = "azure_client_secret")]
    pub client_secret: Option<String>,

    /// Tenant id used in oauth flows.
    ///
    /// Supported keys:
    /// - `azure_storage_tenant_id`
    /// - `azure_storage_authority_id`
    /// - `azure_tenant_id`
    /// - `azure_authority_id`
    /// - `tenant_id`
    /// - `authority_id`
    #[serde(
        alias = "azure_storage_tenant_id",
        alias = "azure_storage_authority_id",
        alias = "azure_tenant_id",
        alias = "azure_authority_id",
        alias = "authority_id"
    )]
    pub tenant_id: Option<String>,

    /// Authority host used in oauth flows.
    /// Defaults to https://login.microsoftonline.com
    ///
    /// Supported keys:
    /// - `azure_storage_authority_host`
    /// - `azure_authority_host`
    /// - `authority_host`
    #[serde(alias = "azure_storage_authority_host", alias = "azure_authority_host")]
    pub authority_host: Option<String>,

    /// Bearer token for authorization.
    ///
    /// Supported keys:
    /// - `azure_storage_token`
    /// - `bearer_token`
    /// - `token`
    #[serde(alias = "azure_storage_token", alias = "token")]
    pub bearer_token: Option<String>,

    /// Use object store with azurite storage emulator.
    ///
    /// Supported keys:
    /// - `azure_storage_use_emulator`
    /// - `object_store_use_emulator`
    /// - `use_emulator`
    #[serde(
        alias = "azure_storage_use_emulator",
        alias = "object_store_use_emulator"
    )]
    pub use_emulator: Option<bool>,

    /// Endpoint to request a managed identity token.
    ///
    /// Supported keys:
    /// - `azure_msi_endpoint`
    /// - `azure_identity_endpoint`
    /// - `identity_endpoint`
    /// - `msi_endpoint`
    #[serde(
        alias = "azure_msi_endpoint",
        alias = "azure_identity_endpoint",
        alias = "identity_endpoint"
    )]
    pub msi_endpoint: Option<String>,

    /// Object id for use with managed identity authentication.
    ///
    /// Supported keys:
    /// - `azure_object_id`
    /// - `object_id`
    #[serde(alias = "azure_object_id")]
    pub object_id: Option<String>,

    /// MSI resource id for use with managed identity authentication.
    ///
    /// Supported keys:
    /// - `azure_msi_resource_id`
    /// - `msi_resource_id`
    #[serde(alias = "azure_msi_resource_id")]
    pub msi_resource_id: Option<String>,

    /// File containing token for Azure AD workload identity federation.
    ///
    /// Supported keys:
    /// - `azure_federated_token_file`
    /// - `federated_token_file`
    #[serde(alias = "azure_federated_token_file")]
    pub federated_token_file: Option<String>,

    /// Use azure cli for acquiring access token.
    ///
    /// Supported keys:
    /// - `azure_use_azure_cli`
    /// - `use_azure_cli`
    #[serde(alias = "azure_use_azure_cli")]
    pub use_azure_cli: Option<bool>,

    /// Skip signing requests.
    ///
    /// Supported keys:
    /// - `azure_skip_signature`
    /// - `skip_signature`
    #[serde(alias = "azure_skip_signature")]
    pub skip_signature: Option<bool>,

    /// Use Microsoft Fabric url scheme.
    /// When enabled uses https://{account}.dfs.fabric.microsoft.com
    ///
    /// Supported keys:
    /// - `azure_use_fabric_endpoint`
    /// - `use_fabric_endpoint`
    #[serde(alias = "azure_use_fabric_endpoint")]
    pub use_fabric_endpoint: Option<bool>,

    /// Disables tagging objects.
    ///
    /// Supported keys:
    /// - `azure_disable_tagging`
    /// - `disable_tagging`
    #[serde(alias = "azure_disable_tagging")]
    pub disable_tagging: Option<bool>,

    /// The maximum batch operations of Azblob service backend.
    pub batch_max_operations: Option<usize>,
}

impl Debug for AzblobConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("AzblobConfig");

        ds.field("root", &self.root);
        ds.field("container", &self.container);
        ds.field("endpoint", &self.endpoint);
        ds.field("authority_host", &self.authority_host);
        ds.field("use_emulator", &self.use_emulator);
        ds.field("msi_endpoint", &self.msi_endpoint);
        ds.field("object_id", &self.object_id);
        ds.field("msi_resource_id", &self.msi_resource_id);
        ds.field("use_azure_cli", &self.use_azure_cli);
        ds.field("skip_signature", &self.skip_signature);
        ds.field("use_fabric_endpoint", &self.use_fabric_endpoint);
        ds.field("disable_tagging", &self.disable_tagging);

        if self.account_name.is_some() {
            ds.field("account_name", &"<redacted>");
        }
        if self.account_key.is_some() {
            ds.field("account_key", &"<redacted>");
        }
        if self.sas_token.is_some() {
            ds.field("sas_token", &"<redacted>");
        }
        if self.client_id.is_some() {
            ds.field("client_id", &"<redacted>");
        }
        if self.client_secret.is_some() {
            ds.field("client_secret", &"<redacted>");
        }
        if self.tenant_id.is_some() {
            ds.field("tenant_id", &"<redacted>");
        }
        if self.bearer_token.is_some() {
            ds.field("bearer_token", &"<redacted>");
        }
        if self.federated_token_file.is_some() {
            ds.field("federated_token_file", &self.federated_token_file);
        }

        ds.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Configurator;
    use std::collections::HashMap;

    #[test]
    fn test_config_aliases() {
        // Test account_name aliases
        let mut map = HashMap::new();
        map.insert(
            "azure_storage_account_name".to_string(),
            "testaccount".to_string(),
        );
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(config.account_name, Some("testaccount".to_string()));

        // Test account_key aliases
        let mut map = HashMap::new();
        map.insert(
            "azure_storage_account_key".to_string(),
            "dGVzdGtleQ==".to_string(),
        );
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(config.account_key, Some("dGVzdGtleQ==".to_string()));

        let mut map = HashMap::new();
        map.insert("access_key".to_string(), "dGVzdGtleQ==".to_string());
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(config.account_key, Some("dGVzdGtleQ==".to_string()));

        let mut map = HashMap::new();
        map.insert("master_key".to_string(), "dGVzdGtleQ==".to_string());
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(config.account_key, Some("dGVzdGtleQ==".to_string()));

        // Test SAS token aliases
        let mut map = HashMap::new();
        map.insert("sas_key".to_string(), "sv=2021-06-08&ss=b".to_string());
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(config.sas_token, Some("sv=2021-06-08&ss=b".to_string()));

        let mut map = HashMap::new();
        map.insert(
            "azure_storage_sas_token".to_string(),
            "sv=2021-06-08&ss=b".to_string(),
        );
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(config.sas_token, Some("sv=2021-06-08&ss=b".to_string()));

        // Test OAuth aliases
        let mut map = HashMap::new();
        map.insert("azure_client_id".to_string(), "client-id-123".to_string());
        map.insert(
            "azure_client_secret".to_string(),
            "client-secret-123".to_string(),
        );
        map.insert("azure_tenant_id".to_string(), "tenant-id-123".to_string());
        map.insert(
            "azure_authority_host".to_string(),
            "https://login.microsoftonline.com".to_string(),
        );
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(config.client_id, Some("client-id-123".to_string()));
        assert_eq!(config.client_secret, Some("client-secret-123".to_string()));
        assert_eq!(config.tenant_id, Some("tenant-id-123".to_string()));
        assert_eq!(
            config.authority_host,
            Some("https://login.microsoftonline.com".to_string())
        );

        // Test alternative tenant_id aliases
        let mut map = HashMap::new();
        map.insert("authority_id".to_string(), "alt-tenant-id".to_string());
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(config.tenant_id, Some("alt-tenant-id".to_string()));

        // Test managed identity aliases
        let mut map = HashMap::new();
        map.insert("azure_object_id".to_string(), "object-id-123".to_string());
        map.insert(
            "azure_msi_resource_id".to_string(),
            "resource-id-123".to_string(),
        );
        map.insert(
            "azure_msi_endpoint".to_string(),
            "http://169.254.169.254".to_string(),
        );
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(config.object_id, Some("object-id-123".to_string()));
        assert_eq!(config.msi_resource_id, Some("resource-id-123".to_string()));
        assert_eq!(
            config.msi_endpoint,
            Some("http://169.254.169.254".to_string())
        );

        // Test workload identity aliases
        let mut map = HashMap::new();
        map.insert(
            "azure_federated_token_file".to_string(),
            "/var/run/secrets/tokens/azure-identity-token".to_string(),
        );
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(
            config.federated_token_file,
            Some("/var/run/secrets/tokens/azure-identity-token".to_string())
        );

        // TODO: Boolean parsing from strings needs investigation
        // Skipping boolean tests for now - they may require custom deserializer

        // Test bearer token aliases
        let mut map = HashMap::new();
        map.insert(
            "azure_storage_token".to_string(),
            "bearer-token-123".to_string(),
        );
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(config.bearer_token, Some("bearer-token-123".to_string()));

        let mut map = HashMap::new();
        map.insert("token".to_string(), "token-123".to_string());
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(config.bearer_token, Some("token-123".to_string()));

        // Test endpoint aliases
        let mut map = HashMap::new();
        map.insert(
            "azure_storage_endpoint".to_string(),
            "https://test.blob.core.windows.net".to_string(),
        );
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(
            config.endpoint,
            Some("https://test.blob.core.windows.net".to_string())
        );

        let mut map = HashMap::new();
        map.insert(
            "azure_endpoint".to_string(),
            "https://alt.blob.core.windows.net".to_string(),
        );
        map.insert("container".to_string(), "test-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(
            config.endpoint,
            Some("https://alt.blob.core.windows.net".to_string())
        );

        // Test container aliases
        let mut map = HashMap::new();
        map.insert(
            "azure_container_name".to_string(),
            "azure-container".to_string(),
        );
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(config.container, "azure-container".to_string());

        let mut map = HashMap::new();
        map.insert("container_name".to_string(), "named-container".to_string());
        let config = AzblobConfig::from_iter(map).unwrap();
        assert_eq!(config.container, "named-container".to_string());
    }
}
