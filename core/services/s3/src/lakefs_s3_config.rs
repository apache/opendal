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

/// Configuration for the lakeFS S3 Gateway.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct LakefsS3Config {
    /// S3 key prefix starting with a branch or other lakeFS reference.
    ///
    /// <!-- @group General -->
    /// <!-- @required true -->
    /// <!-- @example dev/data -->
    pub root: Option<String>,
    /// lakeFS repository name as exposed by the S3 Gateway.
    ///
    /// <!-- @group General -->
    /// <!-- @example example-repo -->
    pub bucket: String,
    /// HTTP(S) base URL of the lakeFS S3 Gateway.
    ///
    /// <!-- @group General -->
    /// <!-- @example https://lakefs.example.com -->
    pub endpoint: String,
    /// Signing region. Defaults to `us-east-1`.
    ///
    /// <!-- @group General -->
    /// <!-- @default us-east-1 -->
    pub region: Option<String>,
    /// Use virtual-hosted-style addressing when the gateway is configured for it.
    ///
    /// <!-- @group General -->
    /// <!-- @default false -->
    pub enable_virtual_host_style: Option<bool>,
    /// lakeFS access key ID.
    ///
    /// <!-- @group Credentials -->
    pub access_key_id: Option<String>,
    /// lakeFS secret access key.
    ///
    /// <!-- @group Credentials -->
    pub secret_access_key: Option<String>,
    /// Session token from issued lakeFS Enterprise temporary credentials. Requires both keys.
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

impl LakefsS3Config {
    fn connection(&self) -> Result<Connection> {
        let root = opendal_core::raw::normalize_root(self.root.as_deref().unwrap_or_default());
        let reference = root
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or_default();
        if reference.trim().is_empty() || reference == ".." {
            return Err(preset::config_error(
                LAKEFS_S3_SCHEME,
                "root must start with a lakeFS branch or reference",
            ));
        }
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
    LakefsS3Config, LakefsS3, LAKEFS_S3_SCHEME = "lakefs-s3", register_lakefs_s3_service, "lakefs-s3.md";
    required {
        bucket: "Set the lakeFS repository name.",
        endpoint: "Set the HTTP(S) base endpoint.",
    }
    optional {
        root: "Set the S3 key prefix starting with a branch or reference.",
        region: "Set the signing region.",
        access_key_id: "Set the access key ID.",
        secret_access_key: "Set the secret access key.",
        session_token: "Set the session token for temporary credentials.",
    }
    flags {
        enable_virtual_host_style: "Set whether to use virtual-hosted-style addressing.",
    }
    token: session_token;
}
