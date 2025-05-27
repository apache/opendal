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

/// Azure Data Lake Storage Gen2 Support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct AzdlsConfig {
    /// Root of this backend.
    pub root: Option<String>,
    /// Filesystem name of this backend.
    pub filesystem: String,
    /// Endpoint of this backend.
    pub endpoint: Option<String>,
    /// Account name of this backend.
    pub account_name: Option<String>,
    /// Account key of this backend.
    /// - required for shared_key authentication
    pub account_key: Option<String>,
    /// client_secret
    /// The client secret of the service principal.
    /// - required for client_credentials authentication
    pub client_secret: Option<String>,
    /// tenant_id
    /// The tenant id of the service principal.
    /// - required for client_credentials authentication
    pub tenant_id: Option<String>,
    /// client_id
    /// The client id of the service principal.
    /// - required for client_credentials authentication
    pub client_id: Option<String>,
    /// sas_token
    /// The shared access signature token.
    /// - required for sas authentication
    pub sas_token: Option<String>,
    /// authority_host
    /// The authority host of the service principal.
    /// - required for client_credentials authentication
    /// - default value: `https://login.microsoftonline.com`
    pub authority_host: Option<String>,
}

impl Debug for AzdlsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("AzdlsConfig");

        ds.field("root", &self.root);
        ds.field("filesystem", &self.filesystem);
        ds.field("endpoint", &self.endpoint);

        if self.account_name.is_some() {
            ds.field("account_name", &"<redacted>");
        }
        if self.account_key.is_some() {
            ds.field("account_key", &"<redacted>");
        }
        if self.client_secret.is_some() {
            ds.field("client_secret", &"<redacted>");
        }
        if self.tenant_id.is_some() {
            ds.field("tenant_id", &"<redacted>");
        }
        if self.client_id.is_some() {
            ds.field("client_id", &"<redacted>");
        }
        if self.sas_token.is_some() {
            ds.field("sas_token", &"<redacted>");
        }
        ds.finish()
    }
}
