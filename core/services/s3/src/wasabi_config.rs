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

/// Configuration for Wasabi Object Storage.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct WasabiConfig {
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
    /// Bucket region, used as the signing region.
    ///
    /// <!-- @group General -->
    /// <!-- @example eu-west-2 -->
    pub region: String,
    /// Explicit HTTP(S) base endpoint. The default is
    /// the published service URL for the bucket region.
    ///
    /// <!-- @group General -->
    pub endpoint: Option<String>,
    /// Use virtual-hosted-style bucket addressing.
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
    /// Session token for temporary credentials. Requires both access keys.
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

impl WasabiConfig {
    fn connection(&self) -> Result<Connection> {
        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => match self.region.as_str() {
                "us-east-1" | "us-east-2" | "us-central-1" | "us-west-1" | "us-west-2"
                | "ca-central-1" | "eu-central-1" | "eu-central-2" | "eu-west-1" | "eu-west-2"
                | "eu-west-3" | "eu-south-1" | "ap-northeast-1" | "ap-northeast-2"
                | "ap-southeast-1" | "ap-southeast-2" => {
                    format!("https://s3.{}.wasabisys.com", self.region)
                }
                _ => {
                    return Err(preset::config_error(
                        WASABI_SCHEME,
                        "region has no documented default endpoint; set endpoint explicitly",
                    ));
                }
            },
        };

        Ok(Connection {
            endpoint,
            region: self.region.clone(),
            virtual_host_style: self.enable_virtual_host_style.unwrap_or(false),
        })
    }
}

preset::provider_preset! {
    WasabiConfig, Wasabi, WASABI_SCHEME = "wasabi", register_wasabi_service, "wasabi.md";
    required {
        bucket: "Set the bucket name.",
        region: "Set the bucket region.",
    }
    optional {
        root: "Set the root within the bucket.",
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
