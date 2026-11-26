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

/// Configuration for Ceph RADOS Gateway.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct CephRgwConfig {
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
    /// HTTP(S) base URL of the RADOS Gateway S3 API.
    ///
    /// <!-- @group General -->
    /// <!-- @example https://rgw.example.com -->
    pub endpoint: String,
    /// Signing region. Defaults to `us-east-1`.
    ///
    /// <!-- @group General -->
    /// <!-- @default us-east-1 -->
    pub region: Option<String>,
    /// Use virtual-hosted-style addressing when the gateway has a matching `rgw_dns_name` or zonegroup hostname.
    ///
    /// <!-- @group General -->
    /// <!-- @default false -->
    pub enable_virtual_host_style: Option<bool>,
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

impl CephRgwConfig {
    fn connection(&self) -> Result<Connection> {
        Ok(Connection {
            endpoint: self.endpoint.clone(),
            region: self
                .region
                .clone()
                .unwrap_or_else(|| "us-east-1".to_owned()),
            virtual_host_style: self.enable_virtual_host_style.unwrap_or(false),
        })
    }
}

preset::provider_preset! {
    CephRgwConfig, CephRgw, CEPH_RGW_SCHEME = "ceph-rgw", register_ceph_rgw_service, "ceph-rgw.md";
    required {
        bucket: "Set the bucket name.",
        endpoint: "Set the HTTP(S) base endpoint.",
    }
    optional {
        root: "Set the root within the bucket.",
        region: "Set the signing region.",
        access_key_id: "Set the access key ID.",
        secret_access_key: "Set the secret access key.",
    }
    flags {
        enable_virtual_host_style: "Set whether to use virtual-hosted-style addressing.",
    }
}
