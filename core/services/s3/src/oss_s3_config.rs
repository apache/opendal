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

/// Configuration for Alibaba Cloud OSS access through its S3-compatible API.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct OssS3Config {
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
    /// OSS bucket region. Also used to sign requests.
    ///
    /// <!-- @group General -->
    /// <!-- @example cn-hangzhou -->
    pub region: String,
    /// HTTP(S) base endpoint override.
    ///
    /// <!-- @group General -->
    /// <!-- @example https://s3.oss-cn-hangzhou-internal.aliyuncs.com -->
    pub endpoint: Option<String>,
    /// Alibaba Cloud RAM AccessKey ID.
    ///
    /// <!-- @group Credentials -->
    pub access_key_id: Option<String>,
    /// Alibaba Cloud RAM AccessKey secret.
    ///
    /// <!-- @group Credentials -->
    pub secret_access_key: Option<String>,
    /// Session token for temporary credentials. Requires both keys.
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

impl OssS3Config {
    fn connection(&self) -> Result<Connection> {
        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => {
                let region = preset::dns_label(&self.region, OSS_S3_SCHEME, "region")?;
                format!("https://s3.oss-{region}.aliyuncs.com")
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
    OssS3Config, OssS3, OSS_S3_SCHEME = "oss-s3", register_oss_s3_service, "oss-s3.md";
    required {
        bucket: "Set the bucket name.",
        region: "Set the signing region.",
    }
    optional {
        root: "Set the root within the bucket.",
        endpoint: "Set the HTTP(S) base endpoint.",
        access_key_id: "Set the access key ID.",
        secret_access_key: "Set the secret access key.",
        session_token: "Set the session token for temporary credentials.",
    }
    flags {}
    token: session_token;
}
