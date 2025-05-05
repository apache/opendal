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

use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

/// Config for Aliyun Object Storage Service (OSS) support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct OssConfig {
    /// Root for oss.
    pub root: Option<String>,

    /// Endpoint for oss.
    pub endpoint: Option<String>,
    /// Presign endpoint for oss.
    pub presign_endpoint: Option<String>,
    /// Bucket for oss.
    pub bucket: String,

    /// is bucket versioning enabled for this bucket
    pub enable_versioning: bool,

    // OSS features
    /// Server side encryption for oss.
    pub server_side_encryption: Option<String>,
    /// Server side encryption key id for oss.
    pub server_side_encryption_key_id: Option<String>,
    /// Allow anonymous for oss.
    pub allow_anonymous: bool,

    // authenticate options
    /// Access key id for oss.
    pub access_key_id: Option<String>,
    /// Access key secret for oss.
    pub access_key_secret: Option<String>,
    /// The size of max batch operations.
    #[deprecated(
        since = "0.52.0",
        note = "Please use `delete_max_size` instead of `batch_max_operations`"
    )]
    pub batch_max_operations: Option<usize>,
    /// The size of max delete operations.
    pub delete_max_size: Option<usize>,
    /// If `role_arn` is set, we will use already known config as source
    /// credential to assume role with `role_arn`.
    pub role_arn: Option<String>,
    /// role_session_name for this backend.
    pub role_session_name: Option<String>,
    /// `oidc_provider_arn` will be loaded from
    ///
    /// - this field if it's `is_some`
    /// - env value: [`ALIBABA_CLOUD_OIDC_PROVIDER_ARN`]
    pub oidc_provider_arn: Option<String>,
    /// `oidc_token_file` will be loaded from
    ///
    /// - this field if it's `is_some`
    /// - env value: [`ALIBABA_CLOUD_OIDC_TOKEN_FILE`]
    pub oidc_token_file: Option<String>,
    /// `sts_endpoint` will be loaded from
    ///
    /// - this field if it's `is_some`
    /// - env value: [`ALIBABA_CLOUD_STS_ENDPOINT`]
    pub sts_endpoint: Option<String>,
}

impl Debug for OssConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Builder");
        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("allow_anonymous", &self.allow_anonymous);

        d.finish_non_exhaustive()
    }
}
