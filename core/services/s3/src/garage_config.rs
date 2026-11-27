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

/// Configuration for a Garage deployment.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct GarageConfig {
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
    /// HTTP(S) base URL of the cluster’s S3 API.
    ///
    /// <!-- @group General -->
    /// <!-- @example http://127.0.0.1:3900 -->
    pub endpoint: String,
    /// Signing region from `s3_region` in the cluster’s `[s3_api]` configuration.
    ///
    /// <!-- @group General -->
    /// <!-- @example garage -->
    pub region: String,
    /// Use virtual-hosted-style addressing when the cluster has a matching `root_domain`.
    ///
    /// <!-- @group General -->
    /// <!-- @default false -->
    pub enable_virtual_host_style: Option<bool>,
    /// Opt in to native checksum verification for signed, non-append writes. Defaults to false.
    /// Verified with signed path-style requests to Garage 2.1.0.
    ///
    /// <!-- @group Write -->
    /// <!-- @default false -->
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_write_checksum: Option<bool>,
    /// Access key ID. Set this field together with `secret_access_key`.
    ///
    /// <!-- @group Credentials -->
    pub access_key_id: Option<String>,
    /// Secret access key. Set this field together with `access_key_id`.
    ///
    /// <!-- @group Credentials -->
    pub secret_access_key: Option<String>,
    /// Disable loading credentials from AWS environment variables and shared profile files.
    ///
    /// Set both `access_key_id` and `secret_access_key` when enabled.
    ///
    /// <!-- @group Credentials -->
    /// <!-- @default false -->
    pub disable_config_load: bool,
}

impl GarageConfig {
    fn connection(&self) -> Result<Connection> {
        Ok(Connection {
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            virtual_host_style: self.enable_virtual_host_style.unwrap_or(false),
        })
    }
}

preset::provider_preset! {
    GarageConfig, Garage, GARAGE_SCHEME = "garage", register_garage_service, "garage.md";
    required {
        bucket: "Set the bucket name.",
        endpoint: "Set the HTTP(S) base endpoint.",
        region: "Set the signing region.",
    }
    optional {
        root: "Set the root within the bucket.",
        access_key_id: "Set the access key ID.",
        secret_access_key: "Set the secret access key.",
    }
    flags {
        enable_virtual_host_style: "Set whether to use virtual-hosted-style addressing.",
        enable_write_checksum: "Opt in to native checksum verification for signed, non-append writes.",
    }
    write_checksum: enable_write_checksum;
}

#[cfg(test)]
mod tests {
    use super::GarageConfig;

    #[test]
    fn write_checksum_config_round_trip() {
        for enabled in [None, Some(false), Some(true)] {
            let config = GarageConfig {
                enable_write_checksum: enabled,
                ..Default::default()
            };
            let serialized = serde_json::to_value(&config).unwrap();
            assert_eq!(
                serialized.get("enable_write_checksum"),
                enabled.map(serde_json::Value::Bool).as_ref()
            );
            let decoded: GarageConfig = serde_json::from_value(serialized).unwrap();
            assert_eq!(decoded, config);
        }
    }
}
