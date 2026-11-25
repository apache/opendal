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

/// Configuration for DigitalOcean Spaces.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct SpacesConfig {
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
    /// Spaces datacenter slug. Required unless `endpoint` is set.
    ///
    /// <!-- @group General -->
    /// <!-- @example nyc3 -->
    /// <!-- @minimal true -->
    pub datacenter: Option<String>,
    /// Signing region, independent of the Spaces datacenter.
    ///
    /// <!-- @group General -->
    /// <!-- @default us-east-1 -->
    pub region: Option<String>,
    /// Explicit HTTP(S) base endpoint. The default is
    /// `https://<datacenter>.digitaloceanspaces.com`.
    ///
    /// <!-- @group General -->
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
}

impl SpacesConfig {
    fn connection(&self) -> Result<Connection> {
        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => {
                let datacenter = self.datacenter.as_deref().ok_or_else(|| {
                    preset::config_error(SPACES_SCHEME, "datacenter is required without endpoint")
                })?;
                let datacenter = preset::dns_label(datacenter, SPACES_SCHEME, "datacenter")?;
                format!("https://{datacenter}.digitaloceanspaces.com")
            }
        };

        Ok(Connection {
            endpoint,
            region: self
                .region
                .clone()
                .unwrap_or_else(|| "us-east-1".to_owned()),
            virtual_host_style: self.enable_virtual_host_style.unwrap_or(true),
        })
    }
}

preset::provider_preset! {
    SpacesConfig, Spaces, SPACES_SCHEME = "spaces", register_spaces_service, "spaces.md";
    required {
        bucket: "Set the bucket name.",
    }
    optional {
        root: "Set the root within the bucket.",
        datacenter: "Set the Spaces datacenter slug.",
        region: "Set the signing region.",
        endpoint: "Set an explicit base endpoint.",
        access_key_id: "Set the access key ID.",
        secret_access_key: "Set the secret access key.",
    }
    flags {
        enable_virtual_host_style: "Set whether to use virtual-hosted-style addressing.",
    }
}
