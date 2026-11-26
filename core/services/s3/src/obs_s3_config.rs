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

/// Configuration for Huawei Cloud OBS access through its S3-compatible API.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct ObsS3Config {
    /// Root within the bucket. The default is `/`.
    ///
    /// <!-- @group General -->
    /// <!-- @default / -->
    pub root: Option<String>,
    /// OBS bucket name as a single lowercase DNS label.
    ///
    /// <!-- @group General -->
    /// <!-- @example example-bucket -->
    pub bucket: String,
    /// OBS bucket region. Also used to sign requests.
    ///
    /// <!-- @group General -->
    /// <!-- @example cn-north-1 -->
    pub region: String,
    /// HTTP(S) base endpoint override. OpenDAL prepends the bucket to the host.
    ///
    /// <!-- @group General -->
    /// <!-- @example https://obs.cn-north-1.myhuaweicloud.com -->
    pub endpoint: Option<String>,
    /// Huawei Cloud access key ID (AK).
    ///
    /// <!-- @group Credentials -->
    pub access_key_id: Option<String>,
    /// Huawei Cloud secret access key (SK).
    ///
    /// <!-- @group Credentials -->
    pub secret_access_key: Option<String>,
    /// Session token for temporary Huawei Cloud credentials. Requires both keys.
    ///
    /// <!-- @group Credentials -->
    pub session_token: Option<String>,
    /// Disable loading credentials from AWS environment variables and shared profile files.
    ///
    /// Set both `access_key_id` and `secret_access_key` when enabled.
    ///
    /// <!-- @group Credentials -->
    /// <!-- @default false -->
    pub disable_config_load: bool,
}

impl ObsS3Config {
    fn connection(&self) -> Result<Connection> {
        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => {
                let region = preset::dns_label(&self.region, OBS_S3_SCHEME, "region")?;
                format!("https://obs.{region}.myhuaweicloud.com")
            }
        };
        Ok(Connection {
            endpoint,
            region: self.region.clone(),
            virtual_host_style: true,
        })
    }
}

preset::provider_preset! {
    ObsS3Config, ObsS3, OBS_S3_SCHEME = "obs-s3", register_obs_s3_service, "obs-s3.md";
    required {
        bucket: "Set the OBS bucket name.",
        region: "Set the OBS bucket region used to sign requests.",
    }
    optional {
        root: "Set the root within the bucket.",
        endpoint: "Set the HTTP(S) base endpoint.",
        access_key_id: "Set the Huawei Cloud access key ID.",
        secret_access_key: "Set the Huawei Cloud secret access key.",
        session_token: "Set the session token for temporary credentials.",
    }
    flags {}
    token: session_token;
}
