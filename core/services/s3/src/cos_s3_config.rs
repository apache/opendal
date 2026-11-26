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

/// Configuration for Tencent Cloud COS access through its S3-compatible API.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct CosS3Config {
    /// Root within the bucket. The default is `/`.
    ///
    /// <!-- @group General -->
    /// <!-- @default / -->
    pub root: Option<String>,
    /// Full COS bucket name including the numeric `APPID` suffix.
    ///
    /// <!-- @group General -->
    /// <!-- @example example-bucket-1250000000 -->
    pub bucket: String,
    /// COS bucket region. Also used to sign requests.
    ///
    /// <!-- @group General -->
    /// <!-- @example ap-guangzhou -->
    pub region: String,
    /// HTTP(S) base endpoint override.
    ///
    /// <!-- @group General -->
    /// <!-- @example https://cos.ap-guangzhou.myqcloud.com -->
    pub endpoint: Option<String>,
    /// Tencent Cloud `SecretId`.
    ///
    /// <!-- @group Credentials -->
    pub access_key_id: Option<String>,
    /// Tencent Cloud `SecretKey`.
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

impl CosS3Config {
    fn connection(&self) -> Result<Connection> {
        let has_appid = self.bucket.rsplit_once('-').is_some_and(|(name, appid)| {
            !name.is_empty() && !appid.is_empty() && appid.bytes().all(|byte| byte.is_ascii_digit())
        });
        if !has_appid {
            return Err(preset::config_error(
                COS_S3_SCHEME,
                "bucket must include its numeric APPID suffix",
            ));
        }
        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => {
                let region = preset::dns_label(&self.region, COS_S3_SCHEME, "region")?;
                format!("https://cos.{region}.myqcloud.com")
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
    CosS3Config, CosS3, COS_S3_SCHEME = "cos-s3", register_cos_s3_service, "cos-s3.md";
    required {
        bucket: "Set the full bucket name including the APPID suffix.",
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
