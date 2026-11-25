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

/// Configuration for Volcengine or BytePlus TOS access through its S3-compatible API.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct TosS3Config {
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
    /// HTTP(S) S3-compatible base endpoint for the selected TOS product and region.
    ///
    /// <!-- @group General -->
    /// <!-- @example https://tos-s3-cn-beijing.volces.com -->
    pub endpoint: String,
    /// Signing region for the selected TOS S3 endpoint.
    ///
    /// <!-- @group General -->
    /// <!-- @example cn-beijing -->
    pub region: String,
    /// TOS access key ID.
    ///
    /// <!-- @group Credentials -->
    pub access_key_id: Option<String>,
    /// TOS secret access key.
    ///
    /// <!-- @group Credentials -->
    pub secret_access_key: Option<String>,
}

impl TosS3Config {
    fn connection(&self) -> Result<Connection> {
        Ok(Connection {
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            virtual_host_style: true,
        })
    }
}

preset::provider_preset! {
    TosS3Config, TosS3, TOS_S3_SCHEME = "tos-s3", register_tos_s3_service, "tos-s3.md";
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
    flags {}
}
