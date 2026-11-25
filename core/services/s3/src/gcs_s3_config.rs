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

/// Configuration for Google Cloud Storage XML API access with S3-compatible HMAC signing.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct GcsS3Config {
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
    /// HTTP(S) XML API base endpoint. Defaults to `https://storage.googleapis.com`.
    ///
    /// <!-- @group General -->
    /// <!-- @default https://storage.googleapis.com -->
    pub endpoint: Option<String>,
    /// Signing region. Defaults to `auto`.
    ///
    /// <!-- @group General -->
    /// <!-- @default auto -->
    pub region: Option<String>,
    /// Use virtual-hosted-style addressing for DNS-compatible buckets without dots.
    ///
    /// <!-- @group General -->
    /// <!-- @default false -->
    pub enable_virtual_host_style: Option<bool>,
    /// Cloud Storage HMAC access ID.
    ///
    /// <!-- @group Credentials -->
    pub access_key_id: Option<String>,
    /// Cloud Storage HMAC secret.
    ///
    /// <!-- @group Credentials -->
    pub secret_access_key: Option<String>,
}

impl GcsS3Config {
    fn connection(&self) -> Result<Connection> {
        Ok(Connection {
            endpoint: self
                .endpoint
                .clone()
                .unwrap_or_else(|| "https://storage.googleapis.com".to_owned()),
            region: self.region.clone().unwrap_or_else(|| "auto".to_owned()),
            virtual_host_style: self.enable_virtual_host_style.unwrap_or(false),
        })
    }
}

preset::provider_preset! {
    GcsS3Config, GcsS3, GCS_S3_SCHEME = "gcs-s3", register_gcs_s3_service, "gcs-s3.md";
    required {
        bucket: "Set the bucket name.",
    }
    optional {
        root: "Set the root within the bucket.",
        endpoint: "Set the HTTP(S) base endpoint.",
        region: "Set the signing region.",
        access_key_id: "Set the access key ID.",
        secret_access_key: "Set the secret access key.",
    }
    flags {
        enable_virtual_host_style: "Set whether to use virtual-hosted-style addressing.",
    }
}
