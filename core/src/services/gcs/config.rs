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
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

/// [Google Cloud Storage](https://cloud.google.com/storage) services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct GcsConfig {
    /// root URI, all operations happens under `root`
    pub root: Option<String>,
    /// bucket name
    #[serde(alias = "google_bucket", alias = "google_bucket_name", alias = "bucket_name")]
    pub bucket: String,
    /// endpoint URI of GCS service,
    /// default is `https://storage.googleapis.com`
    pub endpoint: Option<String>,
    /// Scope for gcs.
    pub scope: Option<String>,
    /// Service Account for gcs.
    #[serde(alias = "google_service_account", alias = "service_account")]
    pub service_account: Option<String>,
    /// Credentials string for GCS service OAuth2 authentication.
    #[serde(alias = "google_service_account_key", alias = "service_account_key")]
    pub credential: Option<String>,
    /// Local path to credentials file for GCS service OAuth2 authentication.
    #[serde(alias = "google_service_account_path", alias = "service_account_path")]
    pub credential_path: Option<String>,
    /// The predefined acl for GCS.
    pub predefined_acl: Option<String>,
    /// The default storage class used by gcs.
    pub default_storage_class: Option<String>,
    /// Allow opendal to send requests without signing when credentials are not
    /// loaded.
    pub allow_anonymous: bool,
    /// Disable attempting to load credentials from the GCE metadata server when
    /// running within Google Cloud.
    pub disable_vm_metadata: bool,
    /// Disable loading configuration from the environment.
    pub disable_config_load: bool,
    /// A Google Cloud OAuth2 token.
    ///
    /// Takes precedence over `credential` and `credential_path`.
    pub token: Option<String>,

    // HTTP client configuration options
    /// Allow HTTP connections (default: false, only HTTPS allowed)
    #[serde(alias = "google_allow_http")]
    pub allow_http: bool,

    /// Allow invalid/self-signed certificates (default: false)
    #[serde(alias = "google_allow_invalid_certificates")]
    pub allow_invalid_certificates: bool,

    /// Connection timeout duration
    #[serde(alias = "google_connect_timeout")]
    pub connect_timeout: Option<Duration>,

    /// Default content type for uploads
    #[serde(alias = "google_default_content_type")]
    pub default_content_type: Option<String>,

    /// Pool idle timeout duration
    #[serde(alias = "google_pool_idle_timeout")]
    pub pool_idle_timeout: Option<Duration>,

    /// Maximum number of idle connections per host
    #[serde(alias = "google_pool_max_idle_per_host")]
    pub pool_max_idle_per_host: Option<usize>,

    /// HTTP proxy URL
    #[serde(alias = "google_proxy_url")]
    pub proxy_url: Option<String>,

    /// PEM-formatted CA certificate for proxy connections
    #[serde(alias = "google_proxy_ca_certificate")]
    pub proxy_ca_certificate: Option<String>,

    /// List of hosts that bypass proxy (comma-separated)
    #[serde(alias = "google_proxy_excludes")]
    pub proxy_excludes: Option<String>,

    /// Randomize DNS resolution order (default: true)
    #[serde(alias = "google_randomize_addresses")]
    pub randomize_addresses: bool,

    /// Request timeout duration
    #[serde(alias = "google_timeout")]
    pub timeout: Option<Duration>,

    /// Custom User-Agent header
    #[serde(alias = "google_user_agent")]
    pub user_agent: Option<String>,
}

impl Debug for GcsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsConfig")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}
