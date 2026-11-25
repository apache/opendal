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

use opendal_core::Result;
use serde::{Deserialize, Serialize};

use crate::preset::{self, Connection};

/// Configuration for Tigris.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct TigrisConfig {
    /// Root within the bucket. The default is `/`.
    ///
    /// <!-- @group General -->
    /// <!-- @default / -->
    pub root: Option<String>,
    /// Bucket name.
    ///
    /// <!-- @group General -->
    /// <!-- @example example-bucket -->
    pub bucket: String,
    /// Signing region.
    ///
    /// <!-- @group General -->
    /// <!-- @default auto -->
    pub region: Option<String>,
    /// Explicit HTTP(S) base endpoint. The default is `https://t3.storage.dev`.
    ///
    /// <!-- @group General -->
    /// <!-- @default https://t3.storage.dev -->
    pub endpoint: Option<String>,
    /// Use virtual-hosted-style bucket addressing.
    ///
    /// <!-- @group General -->
    /// <!-- @default true -->
    pub enable_virtual_host_style: Option<bool>,
    /// Access key ID. Set this field together with `secret_access_key`.
    ///
    /// <!-- @group Credentials -->
    pub access_key_id: Option<String>,
    /// Secret access key. Set this field together with `access_key_id`.
    ///
    /// <!-- @group Credentials -->
    pub secret_access_key: Option<String>,
    /// Session token for temporary credentials. Requires both access keys.
    ///
    /// <!-- @group Credentials -->
    pub session_token: Option<String>,
}

impl TigrisConfig {
    fn connection(&self) -> Result<Connection> {
        let endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| "https://t3.storage.dev".to_owned());

        Ok(Connection {
            endpoint,
            region: self.region.clone().unwrap_or_else(|| "auto".to_owned()),
            virtual_host_style: self.enable_virtual_host_style.unwrap_or(true),
        })
    }
}

preset::provider_preset! {
    TigrisConfig, Tigris, TIGRIS_SCHEME = "tigris", register_tigris_service, "tigris.md";
    required {
        bucket: "Set the bucket name.",
    }
    optional {
        root: "Set the root within the bucket.",
        region: "Set the signing region.",
        endpoint: "Set an explicit base endpoint.",
        access_key_id: "Set the access key ID.",
        secret_access_key: "Set the secret access key.",
        session_token: "Set the session token for temporary credentials.",
    }
    flags {
        enable_virtual_host_style: "Set whether to use virtual-hosted-style addressing.",
    }
    token: session_token;
}
