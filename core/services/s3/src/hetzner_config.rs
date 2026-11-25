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

/// Configuration for Hetzner Object Storage.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct HetznerConfig {
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
    /// Bucket location. Required unless `endpoint` is set.
    ///
    /// <!-- @group General -->
    /// <!-- @example fsn1 -->
    /// <!-- @minimal true -->
    pub location: Option<String>,
    /// Signing region. Defaults to `location`.
    ///
    /// <!-- @group General -->
    pub region: Option<String>,
    /// Explicit HTTP(S) base endpoint. The default is `https://<location>.your-objectstorage.com`.
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

impl HetznerConfig {
    fn connection(&self) -> Result<Connection> {
        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => {
                let location = self.location.as_deref().ok_or_else(|| {
                    preset::config_error(HETZNER_SCHEME, "location is required without endpoint")
                })?;
                let location = preset::dns_label(location, HETZNER_SCHEME, "location")?;
                format!("https://{location}.your-objectstorage.com")
            }
        };
        let region = self
            .region
            .as_ref()
            .or(self.location.as_ref())
            .ok_or_else(|| {
                preset::config_error(HETZNER_SCHEME, "region or location is required for signing")
            })?;

        Ok(Connection {
            endpoint,
            region: region.clone(),
            virtual_host_style: self.enable_virtual_host_style.unwrap_or(true),
        })
    }
}

preset::provider_preset! {
    HetznerConfig, Hetzner, HETZNER_SCHEME = "hetzner", register_hetzner_service, "hetzner.md";
    required {
        bucket: "Set the bucket name.",
    }
    optional {
        root: "Set the root within the bucket.",
        location: "Set the bucket location.",
        region: "Set an explicit signing region.",
        endpoint: "Set an explicit base endpoint.",
        access_key_id: "Set the access key ID.",
        secret_access_key: "Set the secret access key.",
    }
    flags {
        enable_virtual_host_style: "Set whether to use virtual-hosted-style addressing.",
    }
}
