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

/// Configuration for Supabase Storage.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct SupabaseConfig {
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
    /// Supabase project reference. Required unless `endpoint` is set.
    ///
    /// <!-- @group General -->
    /// <!-- @example example-project -->
    /// <!-- @minimal true -->
    pub project_ref: Option<String>,
    /// Region from the project’s S3 settings, used for signing.
    ///
    /// <!-- @group General -->
    /// <!-- @example us-west-1 -->
    pub region: String,
    /// Explicit HTTP(S) base endpoint. The default is
    /// `https://<project_ref>.storage.supabase.co/storage/v1/s3`.
    ///
    /// <!-- @group General -->
    pub endpoint: Option<String>,
    /// Access key ID. Set this field together with `secret_access_key`.
    ///
    /// <!-- @group Credentials -->
    pub access_key_id: Option<String>,
    /// Secret access key. Set this field together with `access_key_id`.
    ///
    /// <!-- @group Credentials -->
    pub secret_access_key: Option<String>,
    /// User JWT for Supabase session authentication. Requires `access_key_id`
    /// and `secret_access_key`.
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

impl SupabaseConfig {
    fn connection(&self) -> Result<Connection> {
        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => {
                let project_ref = self.project_ref.as_deref().ok_or_else(|| {
                    preset::config_error(
                        SUPABASE_SCHEME,
                        "project_ref is required without endpoint",
                    )
                })?;
                let project_ref = preset::dns_label(project_ref, SUPABASE_SCHEME, "project_ref")?;
                format!("https://{project_ref}.storage.supabase.co/storage/v1/s3")
            }
        };

        Ok(Connection {
            endpoint,
            region: self.region.clone(),
            virtual_host_style: false,
        })
    }
}

preset::provider_preset! {
    SupabaseConfig, Supabase, SUPABASE_SCHEME = "supabase", register_supabase_service, "supabase.md";
    required {
        bucket: "Set the bucket name.",
        region: "Set the project region.",
    }
    optional {
        root: "Set the root within the bucket.",
        project_ref: "Set the Supabase project reference.",
        endpoint: "Set an explicit base endpoint.",
        access_key_id: "Set the access key ID.",
        secret_access_key: "Set the secret access key.",
        session_token: "Set the user JWT for session authentication.",
    }
    flags {}
    token: session_token;
}
