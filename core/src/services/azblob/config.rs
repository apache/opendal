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
    pub container: String,

    /// The endpoint of Azblob service backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - Azblob: `https://accountname.blob.core.windows.net`
    /// - Azurite: `http://127.0.0.1:10000/devstoreaccount1`
    pub endpoint: Option<String>,

    /// The account name of Azblob service backend.
    pub account_name: Option<String>,

    /// The account key of Azblob service backend.
    pub account_key: Option<String>,

    /// The encryption key of Azblob service backend.
    pub encryption_key: Option<String>,

    /// The encryption key sha256 of Azblob service backend.
    pub encryption_key_sha256: Option<String>,

    /// The encryption algorithm of Azblob service backend.
    pub encryption_algorithm: Option<String>,

    /// The sas token of Azblob service backend.
    pub sas_token: Option<String>,

    /// Service principal client id for authorizing requests.
    pub client_id: Option<String>,

    /// Service principal client secret for authorizing requests.
    pub client_secret: Option<String>,

    /// Tenant id used in oauth flows.
    pub tenant_id: Option<String>,

    /// Authority host used in oauth flows.
    /// Defaults to https://login.microsoftonline.com
    pub authority_host: Option<String>,

    /// Bearer token for authorization.
    pub bearer_token: Option<String>,

    /// Use object store with azurite storage emulator.
    pub use_emulator: Option<bool>,

    /// Endpoint to request a managed identity token.
    pub msi_endpoint: Option<String>,

    /// Object id for use with managed identity authentication.
    pub object_id: Option<String>,

    /// MSI resource id for use with managed identity authentication.
    pub msi_resource_id: Option<String>,

    /// File containing token for Azure AD workload identity federation.
    pub federated_token_file: Option<String>,

    /// Use azure cli for acquiring access token.
    pub use_azure_cli: Option<bool>,

    /// Skip signing requests.
    pub skip_signature: Option<bool>,

    /// Use Microsoft Fabric url scheme.
    /// When enabled uses https://{account}.dfs.fabric.microsoft.com
    pub use_fabric_endpoint: Option<bool>,

    /// Disables tagging objects.
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
