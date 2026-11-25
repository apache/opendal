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

/// Configuration for Neon Object Storage.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct NeonConfig {
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
    /// HTTPS S3 endpoint of the Neon branch, or an HTTP(S) custom endpoint.
    ///
    /// <!-- @group General -->
    /// <!-- @example https://br-example.storage.c-6.us-east-2.aws.neon.tech -->
    pub endpoint: String,
    /// Signing region. Defaults to the project region in a standard Neon branch URL.
    /// Required with a custom endpoint.
    ///
    /// <!-- @group General -->
    pub region: Option<String>,
    /// Access key ID. Set this field together with `secret_access_key`.
    ///
    /// <!-- @group Credentials -->
    pub access_key_id: Option<String>,
    /// Secret access key. Set this field together with `access_key_id`.
    ///
    /// <!-- @group Credentials -->
    pub secret_access_key: Option<String>,
}

impl NeonConfig {
    fn connection(&self) -> Result<Connection> {
        preset::validate_required(&self.endpoint, NEON_SCHEME, "endpoint is required")?;
        let region = self
            .region
            .clone()
            .or_else(|| {
                let endpoint = url::Url::parse(&self.endpoint).ok()?;
                if endpoint.scheme() != "https" {
                    return None;
                }
                region_from_branch_host(endpoint.host_str()?).map(str::to_owned)
            })
            .ok_or_else(|| {
                preset::config_error(NEON_SCHEME, "region is required for a custom endpoint")
            })?;
        Ok(Connection {
            endpoint: self.endpoint.clone(),
            region,
            virtual_host_style: false,
        })
    }
}

/// Extract the project region from a Neon branch hostname.
pub(crate) fn region_from_branch_host(host: &str) -> Option<&str> {
    host.strip_suffix(".aws.neon.tech")
        .and_then(|value| value.split_once(".storage."))
        .and_then(|(_, value)| value.split_once('.'))
        .map(|(_, region)| region)
}

preset::provider_preset! {
    NeonConfig, Neon, NEON_SCHEME = "neon", register_neon_service, "neon.md";
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
    flags {}
}
