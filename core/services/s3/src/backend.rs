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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Write;
use std::sync::Arc;
use std::sync::LazyLock;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Buf;
use http::HeaderValue;
use http::StatusCode;
use log::debug;
use log::warn;
use md5::Digest;
use md5::Md5;
use reqsign_aws_v4::Credential;
use reqsign_aws_v4::DefaultCredentialProvider;
use reqsign_aws_v4::ECSCredentialProvider;
use reqsign_aws_v4::S3ExpressSessionConfig;
use reqsign_aws_v4::StaticCredentialProvider;
#[cfg(not(target_arch = "wasm32"))]
use reqsign_command_execute_tokio::TokioCommandExecute;
use reqsign_core::Context;
use reqsign_core::OsEnv;
use reqsign_core::ProvideCredentialChain;
use reqsign_core::ProvideCredentialDyn;
use reqsign_file_read_tokio::TokioFileRead;
use url::Url;

use crate::S3_SCHEME;
use crate::config::S3Config;
use crate::copier::S3Copiers;
use crate::copier::new_s3_copier;
use crate::core::parse_error;
use crate::core::*;
use crate::deleter::S3Deleter;
use crate::lister::S3ListerV1;
use crate::lister::S3ListerV2;
use crate::lister::S3Listers;
use crate::lister::S3ObjectVersionsLister;
use crate::reader::*;
use crate::writer::S3Writer;
use crate::writer::S3Writers;
use opendal_core::raw::oio::Copy;
use opendal_core::raw::*;
use opendal_core::*;

/// Allow constructing correct region endpoint if user gives a global endpoint.
static ENDPOINT_TEMPLATES: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    // AWS S3 Service.
    m.insert(
        "https://s3.amazonaws.com",
        "https://s3.{region}.amazonaws.com",
    );
    m
});

const DEFAULT_BATCH_MAX_OPERATIONS: usize = 1000;

/// Aws S3 and compatible services (including minio, digitalocean space, Tencent Cloud Object Storage(COS) and so on) support.
/// For more information about s3-compatible services, refer to [Compatible Services](#compatible-services).
#[doc = include_str!("docs.md")]
#[doc = include_str!("compatible_services.md")]
#[derive(Default)]
pub struct S3Builder {
    pub(super) config: S3Config,
    pub(super) credential_providers: Option<ProvideCredentialChain<Credential>>,
}

impl Debug for S3Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Builder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl S3Builder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set bucket name of this backend.
    pub fn bucket(mut self, bucket: &str) -> Self {
        self.config.bucket = bucket.to_string();

        self
    }

    /// Set endpoint of this backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - AWS S3: `https://s3.amazonaws.com` or `https://s3.{region}.amazonaws.com`
    /// - Cloudflare R2: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`
    /// - Aliyun OSS: `https://{region}.aliyuncs.com`
    /// - Tencent COS: `https://cos.{region}.myqcloud.com`
    /// - Minio: `http://127.0.0.1:9000`
    ///
    /// If user inputs endpoint without scheme like "s3.amazonaws.com", we
    /// will prepend "https://" before it.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string())
        }

        self
    }

    /// Region represent the signing region of this endpoint. This is required
    /// if you are using the default AWS S3 endpoint.
    ///
    /// If using a custom endpoint,
    /// - If region is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn region(mut self, region: &str) -> Self {
        if !region.is_empty() {
            self.config.region = Some(region.to_string())
        }

        self
    }

    /// Set the fallback signing region.
    ///
    /// OpenDAL uses this value only when `region`, `AWS_REGION`, and
    /// `AWS_DEFAULT_REGION` do not supply a region. Without any region,
    /// building the service returns [`ErrorKind::ConfigInvalid`].
    pub fn default_region(mut self, region: &str) -> Self {
        if !region.is_empty() {
            self.config.default_region = Some(region.to_string());
        }
        self
    }

    /// Set the AWS profile used by the default credential provider chain.
    ///
    /// The configured profile takes precedence over the `AWS_PROFILE`
    /// environment variable and applies to shared AWS config and credentials
    /// files, SSO, and external process credentials.
    ///
    /// This setting has no effect when [`Self::disable_config_load`] is set or
    /// when [`Self::credential_provider_chain`] replaces the default chain.
    pub fn profile(mut self, profile: &str) -> Self {
        if !profile.is_empty() {
            self.config.profile = Some(profile.to_string())
        }

        self
    }

    /// Set access_key_id of this backend.
    ///
    /// - If access_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn access_key_id(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.access_key_id = Some(v.to_string())
        }

        self
    }

    /// Set secret_access_key of this backend.
    ///
    /// - If secret_access_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn secret_access_key(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.secret_access_key = Some(v.to_string())
        }

        self
    }

    /// Set role_arn for this backend.
    ///
    /// If `role_arn` is set, we will use already known config as source
    /// credential to assume role with `role_arn`.
    pub fn role_arn(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.role_arn = Some(v.to_string())
        }

        self
    }

    /// Set external_id for this backend.
    pub fn external_id(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.external_id = Some(v.to_string())
        }

        self
    }

    /// Set role_session_name for this backend.
    pub fn role_session_name(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.role_session_name = Some(v.to_string())
        }

        self
    }

    /// Set assume_role_duration_seconds for this backend.
    pub fn assume_role_duration_seconds(mut self, v: u32) -> Self {
        self.config.assume_role_duration_seconds = Some(v);
        self
    }

    /// Set assume_role_session_tags for this backend.
    pub fn assume_role_session_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.config.assume_role_session_tags = Some(tags);
        self
    }

    /// Set default storage_class for this backend.
    ///
    /// Available values:
    /// - `DEEP_ARCHIVE`
    /// - `GLACIER`
    /// - `GLACIER_IR`
    /// - `INTELLIGENT_TIERING`
    /// - `ONEZONE_IA`
    /// - `OUTPOSTS`
    /// - `REDUCED_REDUNDANCY`
    /// - `STANDARD`
    /// - `STANDARD_IA`
    pub fn default_storage_class(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.default_storage_class = Some(v.to_string())
        }

        self
    }

    /// Set server_side_encryption for this backend.
    ///
    /// Available values: `AES256`, `aws:kms`.
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn server_side_encryption(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.server_side_encryption = Some(v.to_string())
        }

        self
    }

    /// Set server_side_encryption_aws_kms_key_id for this backend
    ///
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
    ///   is not set, S3 will use aws managed kms key to encrypt data.
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
    ///   is a valid kms key id, S3 will use the provided kms key to encrypt data.
    /// - If the `server_side_encryption_aws_kms_key_id` is invalid or not found, an error will be
    ///   returned.
    /// - If `server_side_encryption` is not `aws:kms`, setting `server_side_encryption_aws_kms_key_id` is a noop.
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn server_side_encryption_aws_kms_key_id(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.server_side_encryption_aws_kms_key_id = Some(v.to_string())
        }

        self
    }

    /// Control S3 Bucket Keys for SSE-KMS writes and copies.
    ///
    /// `true` enables Bucket Keys for destination objects; `false` explicitly
    /// disables them. If this method is not called, objects inherit the bucket
    /// setting. This option does not change the bucket's configuration.
    pub fn server_side_encryption_bucket_key_enabled(mut self, enabled: bool) -> Self {
        self.config.server_side_encryption_bucket_key_enabled = Some(enabled);
        self
    }

    /// Set server_side_encryption_customer_algorithm for this backend.
    ///
    /// Available values: `AES256`.
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn server_side_encryption_customer_algorithm(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.server_side_encryption_customer_algorithm = Some(v.to_string())
        }

        self
    }

    /// Set server_side_encryption_customer_key for this backend.
    ///
    /// # Args
    ///
    /// `v`: base64 encoded key that matches algorithm specified in
    /// `server_side_encryption_customer_algorithm`.
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn server_side_encryption_customer_key(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.server_side_encryption_customer_key = Some(v.to_string())
        }

        self
    }

    /// Set server_side_encryption_customer_key_md5 for this backend.
    ///
    /// # Args
    ///
    /// `v`: MD5 digest of key specified in `server_side_encryption_customer_key`.
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn server_side_encryption_customer_key_md5(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.server_side_encryption_customer_key_md5 = Some(v.to_string())
        }

        self
    }

    /// Enable server side encryption with aws managed kms key
    ///
    /// As known as: SSE-KMS
    ///
    /// NOTE: This function should not be used along with other `server_side_encryption_with_` functions.
    pub fn server_side_encryption_with_aws_managed_kms_key(mut self) -> Self {
        self.config.server_side_encryption = Some("aws:kms".to_string());
        self
    }

    /// Enable server side encryption with customer managed kms key
    ///
    /// As known as: SSE-KMS
    ///
    /// NOTE: This function should not be used along with other `server_side_encryption_with_` functions.
    pub fn server_side_encryption_with_customer_managed_kms_key(
        mut self,
        aws_kms_key_id: &str,
    ) -> Self {
        self.config.server_side_encryption = Some("aws:kms".to_string());
        self.config.server_side_encryption_aws_kms_key_id = Some(aws_kms_key_id.to_string());
        self
    }

    /// Enable server side encryption with s3 managed key
    ///
    /// As known as: SSE-S3
    ///
    /// NOTE: This function should not be used along with other `server_side_encryption_with_` functions.
    pub fn server_side_encryption_with_s3_key(mut self) -> Self {
        self.config.server_side_encryption = Some("AES256".to_string());
        self
    }

    /// Enable server side encryption with customer key.
    ///
    /// As known as: SSE-C
    ///
    /// NOTE: This function should not be used along with other `server_side_encryption_with_` functions.
    pub fn server_side_encryption_with_customer_key(mut self, algorithm: &str, key: &[u8]) -> Self {
        self.config.server_side_encryption_customer_algorithm = Some(algorithm.to_string());
        self.config.server_side_encryption_customer_key = Some(BASE64_STANDARD.encode(key));
        let key_md5 = Md5::digest(key);
        self.config.server_side_encryption_customer_key_md5 = Some(BASE64_STANDARD.encode(key_md5));
        self
    }

    /// Set temporary credential used in AWS S3 connections
    ///
    /// # Warning
    ///
    /// session token's lifetime is short and requires users to refresh in time.
    pub fn session_token(mut self, token: &str) -> Self {
        if !token.is_empty() {
            self.config.session_token = Some(token.to_string());
        }
        self
    }

    /// Set the relative URI for AWS container credential metadata.
    ///
    /// An explicit full endpoint takes precedence. The relative URI takes
    /// precedence over ambient full and relative container credential URIs and
    /// is joined to [`Self::container_metadata_uri_override`] when set.
    ///
    /// This setting configures the ECS slot in the default credential chain. It
    /// does not move that slot ahead of earlier default providers.
    pub fn container_credentials_relative_uri(mut self, uri: &str) -> Self {
        if !uri.is_empty() {
            self.config.container_credentials_relative_uri = Some(uri.to_string());
        }
        self
    }

    /// Set the full URI for AWS container credential metadata.
    ///
    /// This endpoint takes precedence over the relative URI and both ambient
    /// container credential URI forms.
    pub fn container_credentials_endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.container_credentials_endpoint = Some(endpoint.to_string());
        }
        self
    }

    /// Set the authorization token for AWS container credential requests.
    ///
    /// This value takes precedence over the configured token file and both
    /// ambient token forms.
    pub fn container_authorization_token(mut self, token: &str) -> Self {
        if !token.is_empty() {
            self.config.container_authorization_token = Some(token.to_string());
        }
        self
    }

    /// Set the file containing the authorization token for AWS container
    /// credential requests.
    ///
    /// This file takes precedence over both ambient token forms. The credential
    /// provider trims surrounding whitespace after reading the file.
    pub fn container_authorization_token_file(mut self, file_path: &str) -> Self {
        if !file_path.is_empty() {
            self.config.container_authorization_token_file = Some(file_path.to_string());
        }
        self
    }

    /// Set the base URI used to resolve a relative AWS container credential
    /// URI.
    ///
    /// The override affects relative URIs only and does not alter a configured
    /// or ambient full URI. The override alone does not select an endpoint.
    pub fn container_metadata_uri_override(mut self, uri: &str) -> Self {
        if !uri.is_empty() {
            self.config.container_metadata_uri_override = Some(uri.to_string());
        }
        self
    }

    /// Disable automatic endpoint loading and ambient environment/profile
    /// credential providers.
    ///
    /// This skips endpoint environment variables and removes the static
    /// environment, shared-profile, SSO, web-identity, and external process
    /// credential slots. Region environment lookup and compute metadata
    /// credential providers, including AWS container credentials and EC2
    /// instance metadata, remain enabled.
    pub fn disable_config_load(mut self) -> Self {
        self.config.disable_config_load = true;
        self
    }

    /// Disable list objects v2 so that opendal will fall back to the older
    /// List Objects V1 to list objects.
    ///
    /// By default, OpenDAL uses List Objects V2 to list objects. However,
    /// some legacy services do not yet support V2.
    pub fn disable_list_objects_v2(mut self) -> Self {
        self.config.disable_list_objects_v2 = true;
        self
    }

    /// Enable request payer so that OpenDAL will send requests with `x-amz-request-payer` header.
    ///
    /// With this option the client accepts to pay for the request and data transfer costs.
    pub fn enable_request_payer(mut self) -> Self {
        self.config.enable_request_payer = true;
        self
    }

    /// Disable load credential from ec2 metadata.
    ///
    /// This option is used to disable the default behavior of opendal
    /// to load credential from ec2 metadata, a.k.a, IMDSv2
    pub fn disable_ec2_metadata(mut self) -> Self {
        self.config.disable_ec2_metadata = true;
        self
    }

    /// Skip signature will skip loading credentials and signing requests.
    pub fn skip_signature(mut self) -> Self {
        self.config.skip_signature = true;
        self
    }

    /// Allow anonymous will allow opendal to send request without signing
    /// when credential is not loaded.
    #[deprecated(
        since = "0.57.0",
        note = "Please use `skip_signature` instead of `allow_anonymous`"
    )]
    pub fn allow_anonymous(self) -> Self {
        self.skip_signature()
    }

    /// Enable virtual host style so that opendal will send API requests
    /// in virtual host style instead of path style.
    ///
    /// - By default, opendal will send API to `https://s3.us-east-1.amazonaws.com/bucket_name`
    /// - Enabled, opendal will send API to `https://bucket_name.s3.us-east-1.amazonaws.com`
    pub fn enable_virtual_host_style(mut self) -> Self {
        self.config.enable_virtual_host_style = true;
        self
    }

    /// Deprecated: S3 stat override capabilities are enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "S3 stat override capabilities are enabled by default and this option is no longer needed."
    )]
    pub fn disable_stat_with_override(self) -> Self {
        self
    }

    /// Deprecated: S3 versioning capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "S3 versioning capability is enabled by default and this option is no longer needed."
    )]
    pub fn enable_versioning(self, _enabled: bool) -> Self {
        self
    }

    /// Replace the credential providers with a custom chain.
    ///
    /// Build the chain from [`opendal_core::reqsign_core`] and
    /// [`crate::reqsign_aws_v4`] so that its types match the ones this service is
    /// built with.
    pub fn credential_provider_chain(mut self, chain: ProvideCredentialChain<Credential>) -> Self {
        self.credential_providers = Some(chain);
        self
    }

    /// Check if `bucket` is valid.
    /// `bucket` must be not empty and if `enable_virtual_host_style` is true
    /// it could not contain dot (.) character.
    fn is_bucket_valid(config: &S3Config) -> bool {
        if config.bucket.is_empty() {
            return false;
        }
        // If enable virtual host style, `bucket` will reside in domain part,
        // for example `https://bucket_name.s3.us-east-1.amazonaws.com`,
        // so `bucket` with dot can't be recognized correctly for this format.
        if config.enable_virtual_host_style && config.bucket.contains('.') {
            return false;
        }
        true
    }

    /// Normalize an endpoint by ensuring it has a lowercase `http://` or
    /// `https://` scheme prefix without misclassifying scheme-less hostnames
    /// that begin with `"http"` (for example `http-proxy.internal:9000`).
    fn normalize_endpoint_scheme(endpoint: &str) -> String {
        if let Some(prefix) = endpoint.get(..7)
            && prefix.eq_ignore_ascii_case("http://")
        {
            format!("http://{}", &endpoint[7..])
        } else if let Some(prefix) = endpoint.get(..8)
            && prefix.eq_ignore_ascii_case("https://")
        {
            format!("https://{}", &endpoint[8..])
        } else {
            // Prefix https if endpoint doesn't start with scheme.
            format!("https://{endpoint}")
        }
    }

    /// Build endpoint with given region.
    fn build_endpoint(config: &S3Config, region: &str) -> String {
        let bucket = {
            debug_assert!(Self::is_bucket_valid(config), "bucket must be valid");

            config.bucket.as_str()
        };

        let mut endpoint = match &config.endpoint {
            Some(endpoint) => Self::normalize_endpoint_scheme(endpoint),
            None => "https://s3.amazonaws.com".to_string(),
        };

        // If endpoint contains bucket name, we should trim them.
        endpoint = endpoint.replace(&format!("//{bucket}."), "//");

        // Omit default ports if specified.
        if let Ok(url) = Url::parse(&endpoint) {
            // Remove the trailing `/` of root path.
            endpoint = url.to_string().trim_end_matches('/').to_string();
        }

        // Update with endpoint templates.
        endpoint = if let Some(template) = ENDPOINT_TEMPLATES.get(endpoint.as_str()) {
            template.replace("{region}", region)
        } else {
            // If we don't know where about this endpoint, just leave
            // them as it.
            endpoint.to_string()
        };

        // Apply virtual host style.
        if config.enable_virtual_host_style {
            endpoint = endpoint.replace("//", &format!("//{bucket}."))
        } else {
            write!(endpoint, "/{bucket}").expect("write into string must succeed");
        };

        endpoint
    }

    fn is_standard_aws_s3_endpoint(config: &S3Config, region: &str, endpoint: &str) -> bool {
        let Ok(endpoint) = Url::parse(endpoint) else {
            return false;
        };
        if endpoint.scheme() != "https"
            || endpoint.port().is_some()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return false;
        }

        let Some(host) = endpoint.host_str() else {
            return false;
        };
        let host = if config.enable_virtual_host_style {
            if endpoint.path() != "/" {
                return false;
            }
            let Some(host) = host
                .strip_prefix(config.bucket.as_str())
                .and_then(|host| host.strip_prefix('.'))
            else {
                return false;
            };
            host
        } else {
            if endpoint.path().strip_prefix('/') != Some(config.bucket.as_str()) {
                return false;
            }
            host
        };

        host.strip_suffix(".amazonaws.com").and_then(|host| {
            host.strip_prefix("s3.")
                .or_else(|| host.strip_prefix("s3-"))
        }) == Some(region)
    }

    fn invalid_s3_express_config(message: &'static str) -> Error {
        Error::new(ErrorKind::ConfigInvalid, message)
            .with_operation("Builder::build")
            .with_context("service", S3_SCHEME)
    }

    fn is_aws_host(host: &str) -> bool {
        host == "s3.amazonaws.com"
            || host.ends_with(".amazonaws.com")
            || host.ends_with(".amazonaws.com.cn")
    }

    fn resolve_s3_express_config(
        config: &S3Config,
        region: &str,
    ) -> Result<Option<S3ExpressSessionConfig>> {
        if !config.bucket.ends_with("--x-s3") {
            return Ok(None);
        }

        let endpoint = match &config.endpoint {
            Some(endpoint) => {
                let endpoint = Self::normalize_endpoint_scheme(endpoint);
                let endpoint = Url::parse(&endpoint).map_err(|err| {
                    Self::invalid_s3_express_config("invalid S3 endpoint").set_source(err)
                })?;
                let Some(host) = endpoint.host_str() else {
                    return Ok(None);
                };
                let host = host
                    .strip_prefix(&format!("{}.", config.bucket))
                    .unwrap_or(host);
                if !Self::is_aws_host(host) {
                    return Ok(None);
                }
                if endpoint.scheme() != "https"
                    || endpoint.path() != "/"
                    || endpoint.query().is_some()
                    || endpoint.fragment().is_some()
                    || endpoint.port().is_some_and(|port| port != 443)
                    || !endpoint.username().is_empty()
                    || endpoint.password().is_some()
                {
                    return Err(Self::invalid_s3_express_config(
                        "unsupported AWS endpoint for an S3 directory bucket",
                    ));
                }
                Some(host.to_string())
            }
            None => None,
        };

        let express_config =
            S3ExpressSessionConfig::from_bucket(&config.bucket, region).map_err(|err| {
                Self::invalid_s3_express_config("invalid AWS S3 directory bucket configuration")
                    .set_source(err)
            })?;

        if let Some(endpoint) = endpoint {
            let dns_suffix = express_config.partition().dns_suffix();
            let region = express_config.region();
            let regional = format!("s3.{region}.{dns_suffix}");
            let zonal = format!(
                "s3express-{}.{region}.{dns_suffix}",
                express_config.zone_id()
            );
            let global = (dns_suffix == "amazonaws.com").then_some("s3.amazonaws.com");
            if endpoint != regional && Some(endpoint.as_str()) != global && endpoint != zonal {
                return Err(Self::invalid_s3_express_config(
                    "AWS endpoint does not match the S3 directory bucket",
                ));
            }
        }

        Ok(Some(express_config))
    }

    /// Deprecated: S3 delete batch capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "S3 delete batch capability is enabled by default and this option is no longer needed."
    )]
    pub fn batch_max_operations(self, _batch_max_operations: usize) -> Self {
        self
    }

    /// Deprecated: S3 delete batch capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "S3 delete batch capability is enabled by default and this option is no longer needed."
    )]
    pub fn delete_max_size(self, _delete_max_size: usize) -> Self {
        self
    }

    /// Set checksum algorithm of this backend.
    /// This is necessary when writing to AWS S3 Buckets with Object Lock enabled for example.
    ///
    /// Available options:
    /// - "crc32c"
    /// - "md5"
    pub fn checksum_algorithm(mut self, checksum_algorithm: &str) -> Self {
        self.config.checksum_algorithm = Some(checksum_algorithm.to_string());

        self
    }

    /// Deprecated: S3 write with If-Match capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "S3 write with If-Match capability is enabled by default and this option is no longer needed."
    )]
    pub fn disable_write_with_if_match(self) -> Self {
        self
    }

    /// Deprecated: S3 append capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "S3 append capability is enabled by default and this option is no longer needed."
    )]
    pub fn enable_write_with_append(self) -> Self {
        self
    }

    /// Detect region of S3 bucket.
    ///
    /// # Args
    ///
    /// - endpoint: the endpoint of S3 service
    /// - bucket: the bucket of S3 service
    ///
    /// # Return
    ///
    /// - `Some(region)` means we detect the region successfully
    /// - `None` means we can't detect the region or meeting errors.
    ///
    /// # Notes
    ///
    /// We will try to detect region by the following methods.
    ///
    /// - Match endpoint with given rules to get region
    ///   - Cloudflare R2
    ///   - AWS S3
    ///   - Aliyun OSS
    ///   - Neon Object Storage
    /// - Send a `HEAD` request to endpoint with bucket name to get `x-amz-bucket-region`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use opendal_service_s3::S3;
    ///
    /// # async fn example() {
    /// let region: Option<String> = S3::detect_region("https://s3.amazonaws.com", "example").await;
    /// # }
    /// ```
    ///
    /// # Reference
    ///
    /// - [Amazon S3 HeadBucket API](https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_HeadBucket.html)
    pub async fn detect_region(endpoint: &str, bucket: &str) -> Option<String> {
        // Remove the possible trailing `/` in endpoint.
        let endpoint = endpoint.trim_end_matches('/');

        // Make sure the endpoint contains the scheme.
        let mut endpoint = Self::normalize_endpoint_scheme(endpoint);

        // Remove bucket name from endpoint.
        endpoint = endpoint.replace(&format!("//{bucket}."), "//");
        let url = format!("{endpoint}/{bucket}");

        debug!("detect region with url: {url}");

        // Try to detect region by endpoint.

        // If this bucket is R2, we can return auto directly.
        //
        // Reference: <https://developers.cloudflare.com/r2/api/s3/api/>
        if endpoint.ends_with("r2.cloudflarestorage.com") {
            return Some("auto".to_string());
        }

        // If this bucket is AWS, we can try to match the endpoint.
        if endpoint == "https://s3.amazonaws.com" {
            return Some("us-east-1".to_string());
        }

        if let Some(region) = endpoint
            .strip_prefix("https://s3.")
            .and_then(|v| v.strip_suffix(".amazonaws.com"))
        {
            return Some(region.to_string());
        }

        // If this bucket is OSS, we can try to match the endpoint.
        //
        // - `oss-ap-southeast-1.aliyuncs.com` => `oss-ap-southeast-1`
        // - `oss-cn-hangzhou-internal.aliyuncs.com` => `oss-cn-hangzhou`
        if let Some(v) = endpoint.strip_prefix("https://") {
            if let Some(region) = v.strip_suffix("-internal.aliyuncs.com") {
                return Some(region.to_string());
            }
            if let Some(region) = v.strip_suffix(".aliyuncs.com") {
                return Some(region.to_string());
            }
        }

        // If this bucket is Neon Object Storage, the region is part of the branch endpoint.
        // Neon doesn't return `x-amz-bucket-region` for HeadBucket.
        //
        // - `br-xxx.storage.c-6.us-east-2.aws.neon.tech` => `us-east-2`
        //
        // Reference: <https://neon.com/docs/storage/s3-compatibility>
        if let Some(region) = endpoint
            .strip_prefix("https://")
            .and_then(crate::neon_config::region_from_branch_host)
        {
            return Some(region.to_string());
        }

        // Try to detect region by HeadBucket.
        let req = http::Request::head(&url).body(Buffer::new()).ok()?;

        let client = HttpTransporter::default();
        let res = client
            .send(req)
            .await
            .map_err(|err| warn!("detect region failed for: {err:?}"))
            .ok()?;

        debug!(
            "auto detect region got response: status {:?}, header: {:?}",
            res.status(),
            res.headers()
        );

        // Get region from response header no matter status code.
        if let Some(region) = res
            .headers()
            .get("x-amz-bucket-region")
            .and_then(|header| header.to_str().ok())
        {
            return Some(region.to_string());
        }

        // Status code is 403 or 200 means we already visit the correct
        // region, we can use the default region directly.
        if res.status() == StatusCode::FORBIDDEN || res.status() == StatusCode::OK {
            return Some("us-east-1".to_string());
        }

        None
    }

    /// Set default ACL for new objects.
    pub fn default_acl(mut self, acl: &str) -> Self {
        self.config.default_acl = Some(acl.to_string());
        self
    }
}

impl Builder for S3Builder {
    type Config = S3Config;

    fn build(self) -> Result<impl Service> {
        self.build_inner(S3_SCHEME)
    }
}

impl S3Builder {
    pub(crate) fn from_provider_config(
        config: S3Config,
        credential_providers: ProvideCredentialChain<Credential>,
    ) -> Self {
        Self {
            config,
            credential_providers: Some(credential_providers),
        }
    }

    pub(crate) fn build_with_scheme(self, scheme: &'static str) -> Result<S3Backend> {
        self.build_inner(scheme)
    }

    pub(crate) fn build_with_resolved_endpoint(
        self,
        scheme: &'static str,
        endpoint: String,
    ) -> Result<S3Backend> {
        self.build_backend(scheme, Some(endpoint))
    }

    fn build_inner(self, scheme: &'static str) -> Result<S3Backend> {
        self.build_backend(scheme, None)
    }

    fn build_backend(
        self,
        scheme: &'static str,
        resolved_endpoint: Option<String>,
    ) -> Result<S3Backend> {
        debug!("backend build started: {:?}", self);

        let S3Builder {
            mut config,
            credential_providers,
        } = self;

        #[allow(deprecated)]
        if config.allow_anonymous {
            config.skip_signature = true;
        }

        let root = normalize_root(&config.root.clone().unwrap_or_default());
        debug!("backend use root {}", root);

        // Handle bucket name.
        let bucket = if Self::is_bucket_valid(&config) {
            Ok(&config.bucket)
        } else {
            Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", scheme),
            )
        }?;
        debug!("backend use bucket {}", bucket);

        let default_storage_class = match &config.default_storage_class {
            None => None,
            Some(v) => Some(
                build_header_value(v).map_err(|err| err.with_context("key", "storage_class"))?,
            ),
        };

        let server_side_encryption = match &config.server_side_encryption {
            None => None,
            Some(v) => Some(
                build_header_value(v)
                    .map_err(|err| err.with_context("key", "server_side_encryption"))?,
            ),
        };

        let server_side_encryption_aws_kms_key_id =
            match &config.server_side_encryption_aws_kms_key_id {
                None => None,
                Some(v) => Some(build_header_value(v).map_err(|err| {
                    err.with_context("key", "server_side_encryption_aws_kms_key_id")
                })?),
            };

        let server_side_encryption_customer_algorithm =
            match &config.server_side_encryption_customer_algorithm {
                None => None,
                Some(v) => Some(build_header_value(v).map_err(|err| {
                    err.with_context("key", "server_side_encryption_customer_algorithm")
                })?),
            };

        let server_side_encryption_customer_key =
            match &config.server_side_encryption_customer_key {
                None => None,
                Some(v) => Some(build_header_value(v).map_err(|err| {
                    err.with_context("key", "server_side_encryption_customer_key")
                })?),
            };

        let server_side_encryption_customer_key_md5 =
            match &config.server_side_encryption_customer_key_md5 {
                None => None,
                Some(v) => Some(build_header_value(v).map_err(|err| {
                    err.with_context("key", "server_side_encryption_customer_key_md5")
                })?),
            };

        let checksum_algorithm = match config.checksum_algorithm.as_deref() {
            Some("crc32c") => Some(ChecksumAlgorithm::Crc32c),
            Some("md5") => Some(ChecksumAlgorithm::Md5),
            None => None,
            v => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    format!("{v:?} is not a supported checksum_algorithm."),
                ));
            }
        };

        // Determine the region
        let region = if let Some(ref v) = config.region {
            v.to_string()
        } else {
            std::env::var("AWS_REGION")
                .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                .ok()
                .or_else(|| config.default_region.clone().filter(|v| !v.is_empty()))
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "region is missing. Set region, an AWS region environment variable, or default_region.",
                    )
                    .with_operation("Builder::build")
                    .with_context("service", scheme)
                })?
        };
        debug!("backend use region: {region}");

        let (endpoint, s3_express_config) = if let Some(endpoint) = resolved_endpoint {
            (endpoint, None)
        } else {
            if config.endpoint.is_none() && !config.disable_config_load {
                let endpoint_from_env = std::env::var("AWS_ENDPOINT_URL")
                    .or_else(|_| std::env::var("AWS_ENDPOINT"))
                    .or_else(|_| std::env::var("AWS_S3_ENDPOINT"))
                    .ok();
                if let Some(endpoint) = endpoint_from_env {
                    let normalized = endpoint.trim_end_matches('/').to_string();
                    config.endpoint = Some(normalized);
                }
            }

            let s3_express_config = Self::resolve_s3_express_config(&config, &region)?;
            let endpoint = match &s3_express_config {
                Some(config) => config.endpoint().to_string(),
                None => Self::build_endpoint(&config, &region),
            };
            (endpoint, s3_express_config)
        };
        let is_s3_express = s3_express_config.is_some();
        let read_checksum_mode = scheme == S3_SCHEME
            && !config.skip_signature
            && !is_s3_express
            && Self::is_standard_aws_s3_endpoint(&config, &region, &endpoint);
        debug!("backend use endpoint: {endpoint}");

        // The base signer context carries local config readers and command
        // execution. HTTP sending is injected from OperationContext when S3Core
        // signs each operation.
        let ctx = Context::new().with_file_read(TokioFileRead).with_env(OsEnv);
        #[cfg(not(target_arch = "wasm32"))]
        let ctx = ctx.with_command_execute(TokioCommandExecute);

        let mut default_provider = {
            let mut builder = DefaultCredentialProvider::builder();

            if config.disable_config_load {
                builder = builder.no_env().no_profile().no_web_identity();
                #[cfg(not(target_arch = "wasm32"))]
                {
                    builder = builder.no_sso().no_process();
                }
            } else if let Some(profile) = config
                .profile
                .as_deref()
                .filter(|profile| !profile.is_empty())
            {
                builder = builder.with_profile(profile);
            }

            if config.disable_ec2_metadata {
                builder = builder.no_imds();
            }

            if config.container_credentials_endpoint.is_some()
                || config.container_credentials_relative_uri.is_some()
                || config.container_authorization_token.is_some()
                || config.container_authorization_token_file.is_some()
                || config.container_metadata_uri_override.is_some()
            {
                let mut ecs = ECSCredentialProvider::new();

                if let Some(endpoint) = config.container_credentials_endpoint.as_deref() {
                    ecs = ecs.with_endpoint(endpoint);
                }
                if let Some(relative_uri) = config.container_credentials_relative_uri.as_deref() {
                    ecs = ecs.with_relative_uri(relative_uri);
                }
                if let Some(token) = config.container_authorization_token.as_deref() {
                    ecs = ecs.with_auth_token(token);
                }
                if let Some(token_file) = config.container_authorization_token_file.as_deref() {
                    ecs = ecs.with_auth_token_file(token_file);
                }
                if let Some(uri_override) = config.container_metadata_uri_override.as_deref() {
                    ecs = ecs.with_metadata_uri_override(uri_override);
                }

                builder = builder.ecs(ecs);
            }

            builder.build()
        };

        // Insert static key if user provided.
        if let (Some(ak), Some(sk)) = (&config.access_key_id, &config.secret_access_key) {
            let static_provider = if let Some(token) = config.session_token.as_deref() {
                StaticCredentialProvider::new(ak, sk).with_session_token(token)
            } else {
                StaticCredentialProvider::new(ak, sk)
            };
            default_provider = default_provider.push_front(static_provider);
        }

        let (base_provider, assume_role): (
            Arc<dyn ProvideCredentialDyn<Credential = Credential>>,
            Option<S3AssumeRoleConfig>,
        ) = if let Some(chain) = credential_providers {
            (Arc::new(chain), None)
        } else {
            let assume_role = config.role_arn.map(|role_arn| S3AssumeRoleConfig {
                role_arn,
                external_id: config.external_id,
                role_session_name: config.role_session_name,
                duration_seconds: config.assume_role_duration_seconds,
                tags: config
                    .assume_role_session_tags
                    .map(|tags| tags.into_iter().collect()),
            });
            (Arc::new(default_provider), assume_role)
        };

        let signer_spec = S3SignerSpec {
            sign_ctx: ctx,
            base_provider,
            assume_role,
            region,
            bucket: bucket.to_string(),
            is_express: is_s3_express,
        };

        // S3 CreateMultipartUpload does not support MD5 checksum_algorithm;
        // only single-shot PutObject supports Content-MD5.
        let s3_write_can_multi = !matches!(checksum_algorithm, Some(ChecksumAlgorithm::Md5));
        // General-purpose S3 verifies full-object CRCs on PutObject and multipart uploads.
        // Provider presets may accept checksum headers without verifying them.
        //
        // ref: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html>
        let s3_can_checksum = scheme == S3_SCHEME && !is_s3_express;

        let mut capability = Capability {
            stat: true,
            stat_with_if_match: true,
            stat_with_if_none_match: true,
            stat_with_if_modified_since: true,
            stat_with_if_unmodified_since: true,
            stat_with_override_cache_control: true,
            stat_with_override_content_disposition: true,
            stat_with_override_content_type: true,
            stat_with_version: !is_s3_express,

            read: true,
            read_with_if_match: true,
            read_with_if_none_match: true,
            read_with_if_modified_since: true,
            read_with_if_unmodified_since: true,
            read_with_override_cache_control: true,
            read_with_override_content_disposition: true,
            read_with_override_content_type: true,
            read_with_version: !is_s3_express,
            read_with_suffix: true,

            write: true,
            write_can_empty: true,
            write_can_multi: s3_write_can_multi,
            write_can_append: true,
            write_can_copy_from: checksum_algorithm.is_none(),

            write_with_cache_control: true,
            write_with_content_type: true,
            write_with_content_disposition: true,
            write_with_content_encoding: !is_s3_express,
            write_with_content_language: true,
            write_with_if_match: true,
            write_with_if_not_exists: true,
            write_with_user_metadata: true,

            write_with_checksum: s3_can_checksum,

            // The min multipart size of S3 is 5 MiB.
            //
            // ref: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
            write_multi_min_size: Some(5 * 1024 * 1024),
            // The max multipart size of S3 is 5 GiB.
            //
            // ref: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
            write_multi_max_size: if cfg!(target_pointer_width = "64") {
                Some(5 * 1024 * 1024 * 1024)
            } else {
                Some(usize::MAX)
            },
            // S3 allows at most 10,000 parts and 5 GiB for each part.
            //
            // ref: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
            write_total_max_size: if cfg!(target_pointer_width = "64") {
                Some(10_000 * 5 * 1024 * 1024 * 1024)
            } else {
                None
            },

            delete: true,
            delete_max_size: Some(DEFAULT_BATCH_MAX_OPERATIONS),
            delete_with_version: !is_s3_express,
            delete_with_if_match: true,

            copy: true,
            copy_can_multi: true,
            copy_with_if_not_exists: true,
            copy_with_if_match: true,
            copy_with_source_version: !is_s3_express,
            copy_with_user_metadata: true,
            copy_with_content_type: true,
            copy_with_content_language: true,
            // The min multipart size of S3 is 5 MiB.
            //
            // ref: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
            copy_multi_min_size: Some(5 * 1024 * 1024),
            // The max multipart size of S3 is 5 GiB.
            //
            // ref: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
            copy_multi_max_size: if cfg!(target_pointer_width = "64") {
                Some(5 * 1024 * 1024 * 1024)
            } else {
                Some(usize::MAX)
            },

            restore: !is_s3_express,
            restore_with_version: !is_s3_express,
            restore_with_if_not_exists: !is_s3_express,

            list: true,
            list_with_limit: true,
            list_with_start_after: !is_s3_express,
            list_with_recursive: !is_s3_express,
            list_with_versions: !is_s3_express,
            list_with_deleted: !is_s3_express,

            presign: true,
            presign_stat: true,
            presign_read: true,
            presign_write: true,
            presign_delete: true,

            shared: true,

            ..Default::default()
        };

        if !s3_write_can_multi {
            capability.write_multi_min_size = None;
            capability.write_multi_max_size = None;
            #[cfg(target_pointer_width = "64")]
            {
                capability.write_total_max_size = Some(5 * 1024 * 1024 * 1024);
            }
        }

        Ok(S3Backend {
            core: Arc::new(S3Core {
                info: ServiceInfo::new(scheme, &root, bucket),
                capability,
                bucket: bucket.to_string(),
                endpoint,
                root,
                server_side_encryption,
                server_side_encryption_aws_kms_key_id,
                server_side_encryption_bucket_key_enabled: config
                    .server_side_encryption_bucket_key_enabled
                    .map(|enabled| {
                        HeaderValue::from_static(if enabled { "true" } else { "false" })
                    }),
                server_side_encryption_customer_algorithm,
                server_side_encryption_customer_key,
                server_side_encryption_customer_key_md5,
                default_storage_class,
                skip_signature: config.skip_signature,
                read_checksum_mode,
                disable_list_objects_v2: config.disable_list_objects_v2,
                enable_request_payer: config.enable_request_payer,
                signer_spec,
                signers: HttpTransportCache::new(),
                checksum_algorithm,
                default_acl: config.default_acl,
            }),
        })
    }
}

/// Backend for s3 services.
#[derive(Debug, Clone)]
pub struct S3Backend {
    pub(crate) core: Arc<S3Core>,
}

impl Service for S3Backend {
    type Reader = oio::StreamReader<S3Reader>;
    type Writer = S3Writers;
    type Lister = S3Listers;
    type Deleter = oio::BatchDeleter<S3Deleter>;
    type Copier = S3Copiers;
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let error_ctx = ErrorContext::new(ServiceOperation("HeadObject"))
            .with_caller_condition(args.is_conditional());
        let checksum = args.checksum() && self.core.read_checksum_mode;
        let resp = self.core.s3_head_object(ctx, path, args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpStat::new(parse_into_s3_metadata(
                path,
                resp.headers(),
                checksum,
            )?)),
            _ => Err(parse_error(error_ctx, resp)),
        }
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<S3Reader> = {
            Ok(oio::StreamReader::new(S3Reader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let append = args.append();
        let concurrent = args.concurrent();
        let writer = S3Writer::new(self.core.clone(), ctx.clone(), path, args)?;
        if append {
            Ok(S3Writers::Two(oio::AppendWriter::new(writer)))
        } else {
            Ok(S3Writers::One(oio::MultipartWriter::new(
                ctx.executor().clone(),
                writer,
                concurrent,
            )))
        }
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::BatchDeleter<S3Deleter> = {
            Ok(oio::BatchDeleter::new(
                S3Deleter::new(self.core.clone(), ctx.clone()),
                self.core.capability.delete_max_size,
            ))
        }?;

        Ok(output)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let output: S3Listers = {
            let l = if args.versions() || args.deleted() {
                ThreeWays::Three(oio::PageLister::new(S3ObjectVersionsLister::new(
                    self.core.clone(),
                    ctx.clone(),
                    path,
                    args,
                )))
            } else if self.core.disable_list_objects_v2 {
                ThreeWays::One(oio::PageLister::new(S3ListerV1::new(
                    self.core.clone(),
                    ctx.clone(),
                    path,
                    args,
                )))
            } else {
                ThreeWays::Two(oio::PageLister::new(S3ListerV2::new(
                    self.core.clone(),
                    ctx.clone(),
                    path,
                    args,
                )))
            };

            Ok(l)
        }?;

        Ok(output)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        let output: S3Copiers = {
            let copier = new_s3_copier(self.core.clone(), ctx, from, to, args)?;
            Ok(copier)
        }?;

        Ok(output)
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        if let Some(version) = args.version() {
            let copy_args = OpCopy::from_options(
                &self.capability(),
                options::CopyOptions {
                    source_version: Some(version.to_owned()),
                    if_not_exists: args.if_not_exists(),
                    ..Default::default()
                },
            )?;
            let mut copier = new_s3_copier(self.core.clone(), ctx, path, path, copy_args)?;

            return match copier.close().await {
                Ok(_) => Ok(RpRestore::new()),
                Err(err) => {
                    let _ = copier.abort().await;
                    Err(err)
                }
            };
        }

        if args.if_not_exists() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "if_not_exists requires a restore version",
            ));
        }

        let resp = self
            .core
            .s3_list_object_versions(ctx, path, "", Some(1), "", "")
            .await?;
        if resp.status() != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("ListObjectVersions")),
                resp,
            ));
        }

        let output: ListObjectVersionsOutput =
            quick_xml::de::from_reader(resp.into_body().reader())
                .map_err(new_xml_deserialize_error)
                .map_err(Error::set_temporary)?;
        let abs_path = build_absolute_path(&self.core.root, path);

        if output
            .version
            .iter()
            .any(|version| version.key == abs_path && version.is_latest)
        {
            return Ok(RpRestore::new());
        }

        let Some(marker) = output
            .delete_marker
            .into_iter()
            .find(|marker| marker.key == abs_path && marker.is_latest)
        else {
            return Err(Error::new(
                ErrorKind::NotFound,
                "no live object or current delete marker exists",
            ));
        };

        let delete_args = OpDelete::from_options(
            &self.capability(),
            options::DeleteOptions {
                version: Some(marker.version_id.clone()),
                ..Default::default()
            },
        )?;
        let resp = self.core.s3_delete_object(ctx, path, &delete_args).await?;
        match resp.status() {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(RpRestore::new()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("DeleteObject")),
                resp,
            )),
        }
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        let (expire, op) = args.into_parts();
        // We will not send this request out, just for signing.
        let req = match op {
            PresignOperation::Stat(v) => self.core.s3_head_object_request(path, v),
            PresignOperation::Read(range, v) => self.core.s3_get_object_request(path, range, &v),
            PresignOperation::Write(v) if v.append() => Err(Error::new(
                ErrorKind::Unsupported,
                "S3 presigned writes do not support append",
            )),
            PresignOperation::Write(v) => {
                let checksum = match v.checksum() {
                    None => None,
                    Some(WriteChecksum::Expected(checksum)) => Some((
                        s3_full_object_checksum_algorithm(checksum.algo)?,
                        BASE64_STANDARD.encode(checksum.value),
                    )),
                    Some(_) => {
                        return Err(Error::new(
                            ErrorKind::Unsupported,
                            "S3 presigned writes require a precomputed checksum when specifying a checksum",
                        ));
                    }
                };
                self.core.s3_put_object_request(
                    path,
                    None,
                    &v,
                    checksum
                        .as_ref()
                        .map(|(algorithm, value)| (*algorithm, value.as_str())),
                    Buffer::new(),
                )
            }
            PresignOperation::Delete(v) => self.core.s3_delete_object_request(path, &v),
            _ => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        };
        let req = req?;

        let req = self.core.sign_query(ctx, req, expire).await?;

        // We don't need this request anymore, consume it directly.
        let (parts, _) = req.into_parts();

        Ok(RpPresign::new(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        )))
    }
}
#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use bytes::Bytes;
    use http::HeaderMap;
    use http::HeaderValue;
    use http::Method;
    use http::Request;
    use http::Response;
    use reqsign_core::ProvideCredential;

    use super::*;

    struct ReadChecksumTransport {
        requests: Arc<Mutex<Vec<(Method, bool)>>>,
    }

    impl HttpTransport for ReadChecksumTransport {
        async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
            let mode = request.headers().get(constants::X_AMZ_CHECKSUM_MODE);
            assert!(mode.is_none_or(|value| value == "ENABLED"));
            self.requests
                .lock()
                .unwrap()
                .push((request.method().clone(), mode.is_some()));
            let mut response = Response::builder()
                .header(constants::X_AMZ_CHECKSUM_TYPE, "FULL_OBJECT")
                .header("x-amz-checksum-crc32c", "4waSgw==");
            let range = request
                .headers()
                .get(http::header::RANGE)
                .map(|v| v.to_str().unwrap());
            let (payload, length, content_range): (&'static [u8], _, _) =
                match (request.method(), range) {
                    (&Method::HEAD, None) => (b"", 9, None),
                    (&Method::GET, None) => (b"123456789", 9, None),
                    (&Method::GET, Some("bytes=2-4")) => (b"345", 3, Some("bytes 2-4/9")),
                    (&Method::GET, Some("bytes=0-8")) => (b"123456789", 9, Some("bytes 0-8/9")),
                    _ => panic!("unexpected request"),
                };
            if let Some(content_range) = content_range {
                response = response
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(http::header::CONTENT_RANGE, content_range);
            }
            let body = Buffer::from(Bytes::from_static(payload));
            Ok(response
                .header(http::header::CONTENT_LENGTH, length)
                .body(HttpBody::new(
                    futures::stream::iter([Ok(body)]),
                    Some(payload.len() as u64),
                ))
                .unwrap())
        }
    }

    #[tokio::test]
    async fn test_read_checksum_mode_and_metadata() -> Result<()> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let transport = HttpTransporter::new(ReadChecksumTransport {
            requests: requests.clone(),
        });
        let op = Operator::new(
            S3Builder::default()
                .bucket("example")
                .region("us-west-2")
                .access_key_id("test-access-key")
                .secret_access_key("test-secret-key")
                .disable_config_load()
                .disable_ec2_metadata(),
        )?
        .with_context(OperationContext::new().with_http_transport(transport.clone()));
        let full_checksum = Checksum::new(ChecksumAlgo::Crc32c, [0xe3, 0x06, 0x92, 0x83]);
        for hint in [false, true] {
            let expected = hint.then_some(full_checksum.clone());
            assert_eq!(
                op.stat_with("object").checksum(hint).await?.checksum(),
                expected
            );
            let reader = op.reader_with("object").checksum(hint).await?;
            assert_eq!(reader.read(..).await?.to_bytes(), "123456789");
            assert_eq!(reader.metadata().unwrap().checksum(), expected);
        }

        for (range, bytes, checksum) in [
            (2..5, "345", None),
            (0..9, "123456789", Some(full_checksum)),
        ] {
            let reader = op.reader_with("object").checksum(true).await?;
            assert_eq!(reader.read(range).await?.to_bytes(), bytes);
            let metadata = reader.metadata().unwrap();
            assert_eq!(metadata.content_length(), 9);
            assert_eq!(metadata.checksum(), checksum);
        }
        assert_eq!(
            op.read_with("object").checksum(true).await?.to_bytes(),
            "123456789"
        );
        let preset = Operator::new(
            crate::Minio::default()
                .endpoint("https://s3.us-west-2.amazonaws.com")
                .bucket("example")
                .region("us-west-2")
                .access_key_id("test-access-key")
                .secret_access_key("test-secret-key")
                .disable_config_load(),
        )?
        .with_context(OperationContext::new().with_http_transport(transport));
        assert_eq!(
            preset.stat_with("object").checksum(true).await?.checksum(),
            None
        );
        assert_eq!(
            *requests.lock().unwrap(),
            [
                (Method::HEAD, false),
                (Method::GET, false),
                (Method::HEAD, true),
                (Method::GET, true),
                (Method::GET, true),
                (Method::GET, true),
                (Method::GET, true),
                (Method::HEAD, false),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_read_checksum_mode_endpoint_scope() -> Result<()> {
        let builder = || {
            S3Builder::default()
                .bucket("example")
                .region("us-west-2")
                .disable_config_load()
                .disable_ec2_metadata()
        };
        assert!(builder().build_inner(S3_SCHEME)?.core.read_checksum_mode);
        for endpoint in [
            "s3.amazonaws.com",
            "https://s3.us-west-2.amazonaws.com",
            "https://s3-us-west-2.amazonaws.com",
        ] {
            let path_style = builder().endpoint(endpoint).build_inner(S3_SCHEME)?;
            let virtual_style = builder()
                .endpoint(endpoint)
                .enable_virtual_host_style()
                .build_inner(S3_SCHEME)?;
            assert!(path_style.core.read_checksum_mode);
            assert!(virtual_style.core.read_checksum_mode);
        }
        for endpoint in [
            "https://s3.us-west-2.amazonaws.com.example.test",
            "https://other.us-west-2.amazonaws.com",
            "https://s3.us-west-2.amazonaws.com/base-path",
            "https://s3.us-west-2.amazonaws.com:8443",
            "http://s3.us-west-2.amazonaws.com",
        ] {
            let backend = builder().endpoint(endpoint).build_inner(S3_SCHEME)?;
            assert!(!backend.core.read_checksum_mode);
        }
        let anonymous = builder().skip_signature().build_inner(S3_SCHEME)?;
        let express = builder()
            .bucket("example--usw2-az1--x-s3")
            .build_inner(S3_SCHEME)?;
        assert!(!anonymous.core.read_checksum_mode);
        assert!(!express.core.read_checksum_mode);
        Ok(())
    }

    #[tokio::test]
    async fn test_presign_ignores_read_checksum_hint() -> Result<()> {
        let backend = S3Builder::default()
            .bucket("example")
            .region("us-west-2")
            .access_key_id("test-access-key")
            .secret_access_key("test-secret-key")
            .disable_config_load()
            .disable_ec2_metadata()
            .build()?;
        let stat = options::StatOptions {
            checksum: true,
            ..Default::default()
        };
        let stat = OpStat::from_options(&backend.capability(), stat)?;
        let read = OpRead::new().with_checksum(true);
        for operation in [
            PresignOperation::Stat(stat),
            PresignOperation::Read(BytesRange::default(), read),
        ] {
            let request = backend
                .presign(
                    &OperationContext::new(),
                    "object",
                    OpPresign::new(operation, Duration::from_secs(60)),
                )
                .await?
                .into_presigned_request();
            assert!(
                !request
                    .header()
                    .contains_key(constants::X_AMZ_CHECKSUM_MODE)
            );
            let query = request.uri().query().unwrap();
            assert!(query.contains("X-Amz-Signature="));
            assert!(!query.contains(constants::X_AMZ_CHECKSUM_MODE));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_bucket_key_presign_headers() {
        let header = "x-amz-server-side-encryption-bucket-key-enabled";
        for (key, value) in [
            ("server_side_encryption_bucket_key_enabled", None),
            ("server_side_encryption_bucket_key_enabled", Some("false")),
            ("server_side_encryption_bucket_key_enabled", Some("true")),
            ("aws_sse_bucket_key_enabled", Some("false")),
        ] {
            let mut config = vec![
                ("bucket".to_string(), "test-bucket".to_string()),
                ("region".to_string(), "us-east-1".to_string()),
                ("access_key_id".to_string(), "test-access-key".to_string()),
                (
                    "secret_access_key".to_string(),
                    "test-secret-key".to_string(),
                ),
                ("disable_config_load".to_string(), "true".to_string()),
                ("server_side_encryption".to_string(), "aws:kms".to_string()),
            ];
            if let Some(value) = value {
                config.push((key.to_string(), value.to_string()));
            }
            let op = opendal_core::Operator::from_iter::<S3Builder>(config).unwrap();
            let write = op
                .presign_write("test", std::time::Duration::from_secs(60))
                .await
                .unwrap();
            assert_eq!(
                write.header().get(header).map(|v| v.to_str().unwrap()),
                value
            );
            let read = op
                .presign_read("test", std::time::Duration::from_secs(60))
                .await
                .unwrap();
            assert!(!read.header().contains_key(header));
        }
    }

    #[tokio::test]
    async fn test_default_region_presign_scope() {
        let environment_region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .ok();
        for (explicit, expected) in [
            (None, environment_region.as_deref().unwrap_or("us-west-2")),
            (Some("us-east-1"), "us-east-1"),
        ] {
            let mut config = vec![
                ("bucket".to_string(), "test-bucket".to_string()),
                ("aws_default_region".to_string(), "us-west-2".to_string()),
                ("access_key_id".to_string(), "test-access-key".to_string()),
                (
                    "secret_access_key".to_string(),
                    "test-secret-key".to_string(),
                ),
                ("disable_config_load".to_string(), "true".to_string()),
            ];
            if let Some(region) = explicit {
                config.push(("region".to_string(), region.to_string()));
            }
            let op = opendal_core::Operator::from_iter::<S3Builder>(config).unwrap();
            let request = op
                .presign_read("test", std::time::Duration::from_secs(60))
                .await
                .unwrap();
            let url = url::Url::parse(&request.uri().to_string()).unwrap();
            let credential = url
                .query_pairs()
                .find_map(|(key, value)| (key == "X-Amz-Credential").then(|| value.into_owned()))
                .unwrap();
            assert_eq!(credential.split('/').nth(2), Some(expected));
        }
    }

    #[tokio::test]
    async fn test_aws_skip_signature_alias() {
        let config = [
            ("bucket", "test-bucket"),
            ("region", "us-east-1"),
            ("aws_skip_signature", "true"),
            ("disable_config_load", "true"),
            ("disable_ec2_metadata", "true"),
        ]
        .map(|(key, value)| (key.to_string(), value.to_string()));
        let op = opendal_core::Operator::from_iter::<S3Builder>(config).unwrap();
        let request = op
            .presign_read("test", std::time::Duration::from_secs(60))
            .await
            .unwrap();
        assert!(request.uri().query().is_none());
        assert!(!request.header().contains_key(http::header::AUTHORIZATION));
    }

    #[derive(Clone, Copy)]
    enum CreateSessionOutcome {
        Success,
        Body(&'static str),
        Status(StatusCode),
        TemporaryTransportError,
    }

    #[derive(Clone)]
    struct CapturedRequest {
        method: Method,
        uri: http::Uri,
        headers: HeaderMap,
    }

    #[derive(Clone)]
    struct S3ExpressMockTransport {
        outcome: CreateSessionOutcome,
        source_content_length: u64,
        source_version: Option<&'static str>,
        head_status: StatusCode,
        put_responses: Arc<Mutex<VecDeque<(StatusCode, Bytes)>>>,
        requests: Arc<Mutex<Vec<CapturedRequest>>>,
    }

    impl S3ExpressMockTransport {
        fn new(outcome: CreateSessionOutcome) -> Self {
            Self {
                outcome,
                source_content_length: 1,
                source_version: None,
                head_status: StatusCode::OK,
                put_responses: Arc::new(Mutex::new(VecDeque::new())),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn with_source_content_length(mut self, source_content_length: u64) -> Self {
            self.source_content_length = source_content_length;
            self
        }

        fn with_source_version(mut self, source_version: &'static str) -> Self {
            self.source_version = Some(source_version);
            self
        }

        fn with_head_status(mut self, head_status: StatusCode) -> Self {
            self.head_status = head_status;
            self
        }

        fn with_put_response(self, status: StatusCode, body: impl Into<Bytes>) -> Self {
            self.put_responses
                .lock()
                .expect("lock poisoned")
                .push_back((status, body.into()));
            self
        }

        fn requests(&self) -> Vec<CapturedRequest> {
            self.requests.lock().expect("lock poisoned").clone()
        }

        fn response(status: StatusCode, body: impl Into<Bytes>) -> Response<HttpBody> {
            let body = Buffer::from(body.into());
            let size = body.len() as u64;
            Response::builder()
                .status(status)
                .body(HttpBody::new(
                    futures::stream::iter(vec![Ok(body)]),
                    Some(size),
                ))
                .expect("mock response must build")
        }
    }

    impl HttpTransport for S3ExpressMockTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let is_create_session = req.uri().query() == Some("session");
            self.requests
                .lock()
                .expect("lock poisoned")
                .push(CapturedRequest {
                    method: req.method().clone(),
                    uri: req.uri().clone(),
                    headers: req.headers().clone(),
                });

            if is_create_session {
                tokio::task::yield_now().await;
                return match self.outcome {
                    CreateSessionOutcome::Success => Ok(Self::response(
                        StatusCode::OK,
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                         <CreateSessionResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                         <Credentials>\
                         <SessionToken>session-token</SessionToken>\
                         <SecretAccessKey>session-secret-key</SecretAccessKey>\
                         <AccessKeyId>session-access-key</AccessKeyId>\
                         <Expiration>2099-01-01T00:05:00Z</Expiration>\
                         </Credentials>\
                         </CreateSessionResult>",
                    )),
                    CreateSessionOutcome::Body(body) => Ok(Self::response(StatusCode::OK, body)),
                    CreateSessionOutcome::Status(status) => {
                        Ok(Self::response(status, Bytes::new()))
                    }
                    CreateSessionOutcome::TemporaryTransportError => Err(Error::new(
                        ErrorKind::Unexpected,
                        "temporary mock transport failure",
                    )
                    .set_temporary()),
                };
            }

            if req.method() == Method::HEAD {
                if self.head_status != StatusCode::OK {
                    return Ok(Self::response(self.head_status, Bytes::new()));
                }
                let mut response = Self::response(StatusCode::OK, Bytes::new());
                response.headers_mut().insert(
                    http::header::CONTENT_LENGTH,
                    self.source_content_length
                        .to_string()
                        .parse()
                        .expect("source content length must be a valid header"),
                );
                response.headers_mut().insert(
                    http::header::ETAG,
                    "\"source-etag\""
                        .parse()
                        .expect("source ETag must be a valid header"),
                );
                if let Some(version) = self.source_version {
                    response.headers_mut().insert(
                        constants::X_AMZ_VERSION_ID,
                        version
                            .parse()
                            .expect("source version must be a valid header"),
                    );
                }
                return Ok(response);
            }

            let query = req.uri().query().unwrap_or_default();
            if req.headers().contains_key(constants::X_AMZ_COPY_SOURCE) {
                return Ok(Self::response(
                    StatusCode::OK,
                    "<CopyObjectResult><ETag>\"etag\"</ETag>\
                     <LastModified>2026-08-24T00:00:00Z</LastModified></CopyObjectResult>",
                ));
            }
            if query == "uploads" {
                return Ok(Self::response(
                    StatusCode::OK,
                    "<InitiateMultipartUploadResult><UploadId>upload-id</UploadId>\
                     </InitiateMultipartUploadResult>",
                ));
            }
            if query.contains("list-type=2") {
                return Ok(Self::response(
                    StatusCode::OK,
                    "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>",
                ));
            }
            if req.method() == Method::POST && query.contains("uploadId=") {
                return Ok(Self::response(
                    StatusCode::OK,
                    "<CompleteMultipartUploadResult><Bucket>example</Bucket><Key>target</Key>\
                     <Location>https://example.invalid/target</Location><ETag>\"etag\"</ETag>\
                     </CompleteMultipartUploadResult>",
                ));
            }

            if req.method() == Method::PUT
                && let Some((status, body)) = self
                    .put_responses
                    .lock()
                    .expect("lock poisoned")
                    .pop_front()
            {
                let mut response = Self::response(status, body);
                if status.is_success() {
                    response
                        .headers_mut()
                        .insert(http::header::ETAG, HeaderValue::from_static("\"etag\""));
                }
                return Ok(response);
            }

            let mut response = Self::response(StatusCode::OK, Bytes::new());
            response
                .headers_mut()
                .insert(http::header::ETAG, HeaderValue::from_static("\"etag\""));
            Ok(response)
        }
    }

    fn header_values<'a>(request: &'a CapturedRequest, name: &str) -> Vec<&'a str> {
        request
            .headers
            .get_all(name)
            .iter()
            .map(|value| value.to_str().expect("header must be text"))
            .collect()
    }

    #[tokio::test]
    async fn test_append_existing_object_with_if_match() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success)
            .with_source_content_length(5);
        let op = s3_express_operator(transport.clone());

        op.write_with("target", "world")
            .append(true)
            .content_type("text/plain")
            .if_match("\"etag-1\"")
            .await
            .expect("conditional append to existing object must succeed");

        let requests = transport.requests();
        let put = requests
            .iter()
            .find(|request| request.method == Method::PUT)
            .expect("PUT request must be captured");
        assert_eq!(header(put, constants::X_AMZ_WRITE_OFFSET_BYTES), "5");
        assert_eq!(header(put, "content-length"), "5");
        assert_eq!(header_values(put, "if-match"), vec!["\"etag-1\""]);
        assert!(!put.headers.contains_key("if-none-match"));
        assert!(!put.headers.contains_key("content-type"));

        let precondition_failed = "<Error><Code>PreconditionFailed</Code>\
            <Message>At least one of the pre-conditions you specified did not hold</Message></Error>";
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success)
            .with_source_content_length(5)
            .with_put_response(StatusCode::PRECONDITION_FAILED, precondition_failed);
        let op = s3_express_operator(transport);

        let err = op
            .write_with("target", "world")
            .append(true)
            .if_match("\"etag-1\"")
            .await
            .expect_err("412 response must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        let no_such_key = "<Error><Code>NoSuchKey</Code>\
            <Message>The specified key does not exist.</Message></Error>";
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success)
            .with_source_content_length(5)
            .with_put_response(StatusCode::NOT_FOUND, no_such_key);
        let op = s3_express_operator(transport);

        let err = op
            .write_with("target", "world")
            .append(true)
            .if_match("\"etag-1\"")
            .await
            .expect_err("404 NoSuchKey with if_match must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
    }

    #[tokio::test]
    async fn test_append_existing_object_with_if_not_exists() {
        let precondition_failed = "<Error><Code>PreconditionFailed</Code>\
            <Message>At least one of the pre-conditions you specified did not hold</Message></Error>";
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success)
            .with_source_content_length(5)
            .with_put_response(StatusCode::PRECONDITION_FAILED, precondition_failed);
        let op = s3_express_operator(transport.clone());

        let err = op
            .write_with("target", "world")
            .append(true)
            .if_not_exists(true)
            .await
            .expect_err("append with if_not_exists on existing object must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        let requests = transport.requests();
        let put = requests
            .iter()
            .find(|request| request.method == Method::PUT)
            .expect("PUT request must be captured");
        assert_eq!(header(put, constants::X_AMZ_WRITE_OFFSET_BYTES), "5");
        assert_eq!(header(put, "content-length"), "5");
        assert_eq!(header_values(put, "if-none-match"), vec!["*"]);
    }

    #[tokio::test]
    async fn test_append_new_object_sends_metadata_and_condition_once() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success)
            .with_head_status(StatusCode::NOT_FOUND);
        let op = s3_express_operator(transport.clone());

        op.write_with("target", "hello")
            .append(true)
            .content_type("text/plain")
            .if_not_exists(true)
            .await
            .expect("append creating new object must succeed");

        let requests = transport.requests();
        let put = requests
            .iter()
            .find(|request| request.method == Method::PUT)
            .expect("PUT request must be captured");
        assert_eq!(header(put, constants::X_AMZ_WRITE_OFFSET_BYTES), "0");
        assert_eq!(header(put, "content-length"), "5");
        assert_eq!(header(put, "content-type"), "text/plain");
        assert_eq!(header_values(put, "if-none-match"), vec!["*"]);

        let no_such_key = "<Error><Code>NoSuchKey</Code>\
            <Message>The specified key does not exist.</Message></Error>";
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success)
            .with_head_status(StatusCode::NOT_FOUND)
            .with_put_response(StatusCode::NOT_FOUND, no_such_key);
        let op = s3_express_operator(transport.clone());

        let err = op
            .write_with("target", "hello")
            .append(true)
            .if_match("\"etag-1\"")
            .await
            .expect_err("append with if_match on missing object must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        let requests = transport.requests();
        let put = requests
            .iter()
            .find(|request| request.method == Method::PUT)
            .expect("PUT request must be captured");
        assert_eq!(header(put, constants::X_AMZ_WRITE_OFFSET_BYTES), "0");
        assert_eq!(header_values(put, "if-match"), vec!["\"etag-1\""]);
    }

    #[tokio::test]
    async fn test_append_multi_chunk_applies_condition_only_to_first_append() {
        const CHUNK_SIZE: usize = 5 * 1024 * 1024;
        let first_chunk = vec![b'a'; CHUNK_SIZE];

        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success)
            .with_head_status(StatusCode::NOT_FOUND);
        let op = s3_express_operator(transport.clone());

        let mut writer = op
            .writer_with("target")
            .append(true)
            .chunk(CHUNK_SIZE)
            .content_type("text/plain")
            .if_not_exists(true)
            .await
            .expect("writer must initialize");
        writer
            .write(first_chunk.clone())
            .await
            .expect("first chunk must succeed");
        writer
            .write("world")
            .await
            .expect("second chunk must succeed");
        writer.close().await.expect("close must succeed");

        let requests = transport.requests();
        let puts: Vec<_> = requests
            .iter()
            .filter(|request| request.method == Method::PUT)
            .collect();
        assert_eq!(puts.len(), 2);
        assert_eq!(header(puts[0], constants::X_AMZ_WRITE_OFFSET_BYTES), "0");
        assert_eq!(header(puts[0], "content-type"), "text/plain");
        assert_eq!(header_values(puts[0], "if-none-match"), vec!["*"]);
        assert_eq!(
            header(puts[1], constants::X_AMZ_WRITE_OFFSET_BYTES),
            CHUNK_SIZE.to_string()
        );
        assert!(!puts[1].headers.contains_key("content-type"));
        assert!(!puts[1].headers.contains_key("if-none-match"));

        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success)
            .with_source_content_length(5);
        let op = s3_express_operator(transport.clone());

        let mut writer = op
            .writer_with("target")
            .append(true)
            .chunk(CHUNK_SIZE)
            .if_match("\"etag-0\"")
            .await
            .expect("writer must initialize");
        writer
            .write(first_chunk.clone())
            .await
            .expect("first chunk must succeed");
        writer
            .write("world")
            .await
            .expect("second chunk must succeed");
        writer.close().await.expect("close must succeed");

        let requests = transport.requests();
        let puts: Vec<_> = requests
            .iter()
            .filter(|request| request.method == Method::PUT)
            .collect();
        assert_eq!(puts.len(), 2);
        assert_eq!(header(puts[0], constants::X_AMZ_WRITE_OFFSET_BYTES), "5");
        assert_eq!(header_values(puts[0], "if-match"), vec!["\"etag-0\""]);
        assert_eq!(
            header(puts[1], constants::X_AMZ_WRITE_OFFSET_BYTES),
            (5 + CHUNK_SIZE).to_string()
        );
        assert!(!puts[1].headers.contains_key("if-match"));

        let precondition_failed = "<Error><Code>PreconditionFailed</Code>\
            <Message>unrelated precondition</Message></Error>";
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success)
            .with_source_content_length(5)
            .with_put_response(StatusCode::OK, Bytes::new())
            .with_put_response(StatusCode::PRECONDITION_FAILED, precondition_failed);
        let op = s3_express_operator(transport);

        let mut writer = op
            .writer_with("target")
            .append(true)
            .chunk(CHUNK_SIZE)
            .if_match("\"etag-0\"")
            .await
            .expect("writer must initialize");
        writer
            .write(first_chunk)
            .await
            .expect("first chunk must succeed");
        writer
            .write("world")
            .await
            .expect("second chunk buffered before close");
        let err = writer
            .close()
            .await
            .expect_err("second chunk 412 without caller condition must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn test_put_object_sends_conditional_headers_once() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success);
        let op = s3_general_operator(transport.clone());

        op.write_with("target", "hello")
            .content_type("text/plain")
            .if_match("\"etag-1\"")
            .if_not_exists(true)
            .await
            .expect("put object must succeed");

        let requests = transport.requests();
        let put = requests
            .iter()
            .find(|request| request.method == Method::PUT)
            .expect("PUT request must be captured");
        assert_eq!(header(put, "content-type"), "text/plain");
        assert_eq!(header_values(put, "if-match"), vec!["\"etag-1\""]);
        assert_eq!(header_values(put, "if-none-match"), vec!["*"]);
    }

    #[tokio::test]
    async fn test_multipart_sse_kms_header_scope() {
        const PART_SIZE: usize = 5 * 1024 * 1024;
        for is_copy in [false, true] {
            let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success);
            let op = Operator::new(
                S3Builder::default()
                    .bucket("example")
                    .region("us-west-2")
                    .server_side_encryption("aws:kms")
                    .server_side_encryption_aws_kms_key_id("example-kms-key")
                    .skip_signature()
                    .disable_config_load()
                    .disable_ec2_metadata(),
            )
            .unwrap()
            .with_context(
                OperationContext::new()
                    .with_http_transport(HttpTransporter::new(transport.clone())),
            );
            if is_copy {
                op.copy_with("source", "target")
                    .source_content_length_hint(PART_SIZE as u64 + 1)
                    .chunk(PART_SIZE)
                    .await
                    .unwrap();
            } else {
                let mut writer = op.writer_with("target").chunk(PART_SIZE).await.unwrap();
                writer.write(vec![0; PART_SIZE]).await.unwrap();
                writer.write("x").await.unwrap();
                writer.close().await.unwrap();
            }
            let requests = transport.requests();
            let initiate = requests
                .iter()
                .find(|request| request.uri.query() == Some("uploads"))
                .unwrap();
            assert_eq!(header(initiate, "x-amz-server-side-encryption"), "aws:kms");
            assert_eq!(
                header(initiate, "x-amz-server-side-encryption-aws-kms-key-id"),
                "example-kms-key"
            );
            let mut parts = 0;
            let mut completions = 0;
            for request in requests.iter().filter(|request| {
                request
                    .uri
                    .query()
                    .is_some_and(|query| query.contains("uploadId="))
            }) {
                if request.method == Method::PUT {
                    parts += 1;
                } else if request.method == Method::POST {
                    completions += 1;
                }
                assert!(!request.headers.contains_key("x-amz-server-side-encryption"));
                assert!(
                    !request
                        .headers
                        .contains_key("x-amz-server-side-encryption-aws-kms-key-id")
                );
            }
            assert_eq!(parts, 2);
            assert_eq!(completions, 1);
        }
    }

    fn s3_express_operator(transport: S3ExpressMockTransport) -> Operator {
        Operator::new(
            S3Builder::default()
                .bucket("example--usw2-az1--x-s3")
                .region("us-west-2")
                .access_key_id("source-access-key")
                .secret_access_key("source-secret-key")
                .session_token("source-session-token")
                .disable_config_load()
                .disable_ec2_metadata(),
        )
        .expect("S3 Express operator must build")
        .with_context(OperationContext::new().with_http_transport(HttpTransporter::new(transport)))
    }

    fn s3_general_operator(transport: S3ExpressMockTransport) -> Operator {
        Operator::new(
            S3Builder::default()
                .bucket("example")
                .region("us-west-2")
                .access_key_id("source-access-key")
                .secret_access_key("source-secret-key")
                .session_token("source-session-token")
                .disable_config_load()
                .disable_ec2_metadata(),
        )
        .expect("S3 operator must build")
        .with_context(OperationContext::new().with_http_transport(HttpTransporter::new(transport)))
    }

    fn header<'a>(request: &'a CapturedRequest, name: &str) -> &'a str {
        request
            .headers
            .get(name)
            .unwrap_or_else(|| panic!("missing {name} header"))
            .to_str()
            .expect("header must be text")
    }

    #[tokio::test]
    async fn test_s3_express_signing_chain_and_session_reuse() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success);
        let op = s3_express_operator(transport.clone());

        let (first, second) = tokio::join!(
            op.write("first", "first-body"),
            op.write("second", "second-body")
        );
        first.expect("first write must succeed");
        second.expect("second write must succeed");

        let requests = transport.requests();
        assert_eq!(
            requests[0].uri.query(),
            Some("session"),
            "CreateSession must complete before object requests are signed"
        );
        let create_sessions = requests
            .iter()
            .filter(|request| request.uri.query() == Some("session"))
            .collect::<Vec<_>>();
        assert_eq!(create_sessions.len(), 1, "session must be reused");

        let create_session = create_sessions[0];
        assert_eq!(create_session.method, Method::GET);
        assert_eq!(
            header(create_session, "x-amz-create-session-mode"),
            "ReadWrite"
        );
        assert_eq!(
            header(create_session, "x-amz-security-token"),
            "source-session-token"
        );
        assert!(!create_session.headers.contains_key("x-amz-s3session-token"));
        assert!(header(create_session, "authorization").contains("source-access-key/"));

        let puts = requests
            .iter()
            .filter(|request| request.method == Method::PUT)
            .collect::<Vec<_>>();
        assert_eq!(puts.len(), 2);
        for request in puts {
            assert_eq!(header(request, "x-amz-s3session-token"), "session-token");
            assert!(!request.headers.contains_key("x-amz-security-token"));
            assert!(header(request, "authorization").contains("session-access-key/"));
        }
    }

    #[tokio::test]
    async fn test_s3_express_list_uses_session_credentials() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success);
        let op = s3_express_operator(transport.clone());

        let entries = op.list("").await.expect("list must succeed");
        assert!(entries.is_empty());

        let requests = transport.requests();
        let list = requests
            .iter()
            .find(|request| {
                request
                    .uri
                    .query()
                    .is_some_and(|query| query.contains("list-type=2"))
            })
            .expect("ListObjectsV2 request must be captured");
        assert_eq!(header(list, "x-amz-s3session-token"), "session-token");
        assert!(!list.headers.contains_key("x-amz-security-token"));
        assert!(header(list, "authorization").contains("session-access-key/"));
    }

    #[tokio::test]
    async fn test_s3_general_bucket_always_uses_iam_credentials() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success);
        let op = s3_general_operator(transport.clone());

        op.write("source", "body")
            .await
            .expect("write must succeed");
        op.copy_with("source", "target")
            .source_content_length_hint(1)
            .await
            .expect("copy must succeed");

        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        for request in requests {
            assert_eq!(
                header(&request, "x-amz-security-token"),
                "source-session-token"
            );
            assert!(!request.headers.contains_key("x-amz-s3session-token"));
            assert!(header(&request, "authorization").contains("source-access-key/"));
        }
    }

    #[tokio::test]
    async fn test_s3_copy_hint_skips_source_snapshot() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success);
        let op = s3_general_operator(transport.clone());

        let metadata = op
            .copy_with("source", "target")
            .source_content_length_hint(7)
            .await
            .expect("copy must succeed");

        assert_eq!(metadata.content_length(), 7);
        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, Method::PUT);
        assert!(
            !requests[0]
                .headers
                .contains_key(constants::X_AMZ_COPY_SOURCE_IF_MATCH)
        );
    }

    #[tokio::test]
    async fn test_s3_copy_hint_preserves_explicit_source_version() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success);
        let op = s3_general_operator(transport.clone());

        op.copy_with("source", "target")
            .source_version("source-version")
            .source_content_length_hint(7)
            .await
            .expect("copy must succeed");

        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert!(
            header(&requests[0], constants::X_AMZ_COPY_SOURCE).contains("versionId=source-version")
        );
    }

    #[tokio::test]
    async fn test_s3_copy_without_hint_pins_source_etag() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success)
            .with_source_content_length(7);
        let op = s3_general_operator(transport.clone());

        let metadata = op
            .copy("source", "target")
            .await
            .expect("copy must succeed");

        assert_eq!(metadata.content_length(), 7);
        let requests = transport.requests();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == Method::HEAD)
                .count(),
            1
        );
        let copy = requests
            .iter()
            .find(|request| request.headers.contains_key(constants::X_AMZ_COPY_SOURCE))
            .expect("CopyObject request must be captured");
        assert_eq!(
            header(copy, constants::X_AMZ_COPY_SOURCE_IF_MATCH),
            "\"source-etag\""
        );
    }

    #[tokio::test]
    async fn test_s3_copy_uses_version_from_source_snapshot() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success)
            .with_source_version("source-version");
        let op = s3_general_operator(transport.clone());

        op.copy_with("source", "target")
            .await
            .expect("copy must succeed");

        let requests = transport.requests();
        let copy = requests
            .iter()
            .find(|request| request.headers.contains_key(constants::X_AMZ_COPY_SOURCE))
            .expect("CopyObject request must be captured");
        assert!(header(copy, constants::X_AMZ_COPY_SOURCE).contains("versionId=source-version"));
        assert_eq!(
            header(copy, constants::X_AMZ_COPY_SOURCE_IF_MATCH),
            "\"source-etag\""
        );
    }

    #[tokio::test]
    async fn test_s3_express_copy_and_presign_use_iam_credentials() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success);
        let op = s3_express_operator(transport.clone());

        op.copy_with("source", "target")
            .source_content_length_hint(1)
            .await
            .expect("copy must succeed");
        let requests = transport.requests();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.uri.query() == Some("session"))
                .count(),
            0,
            "a source length hint must skip HeadObject"
        );
        let copy = requests
            .iter()
            .find(|request| request.headers.contains_key(constants::X_AMZ_COPY_SOURCE))
            .expect("CopyObject request must be captured");
        assert_eq!(header(copy, "x-amz-security-token"), "source-session-token");
        assert!(!copy.headers.contains_key("x-amz-s3session-token"));
        assert!(header(copy, "authorization").contains("source-access-key/"));
        assert!(
            !copy
                .headers
                .contains_key(constants::X_AMZ_COPY_SOURCE_IF_MATCH)
        );

        let presigned = op
            .presign_read("source", Duration::from_secs(60))
            .await
            .expect("presign must succeed");
        let query = presigned.uri().query().expect("presign query must exist");
        assert!(query.contains("X-Amz-Credential=source-access-key%2F"));
        assert!(query.contains("X-Amz-Security-Token=source-session-token"));
        assert!(!query.contains("session-access-key"));
        assert!(!query.contains("x-amz-s3session-token"));
        assert_eq!(transport.requests().len(), requests.len());
    }

    #[test]
    fn test_s3_upload_part_copy_sends_zero_content_length() {
        let backend = S3Builder::default()
            .bucket("test")
            .region("us-east-1")
            .build_inner(S3_SCHEME)
            .unwrap();
        let request = backend
            .core
            .s3_upload_part_copy_request(S3UploadPartCopyRequest {
                from: "source",
                to: "target",
                source_version: None,
                if_match: None,
                if_none_match: None,
                if_modified_since: None,
                if_unmodified_since: None,
                upload_id: "upload",
                part_number: 1,
                range: (0_u64..5 * 1024 * 1024).into(),
                operation: Operation::Copy,
            })
            .unwrap();

        assert_eq!(
            request.headers().get(http::header::CONTENT_LENGTH),
            Some(&HeaderValue::from_static("0"))
        );
        assert!(request.body().is_empty());
    }

    #[tokio::test]
    async fn test_s3_express_upload_part_copy_uses_iam_credentials() {
        let source_content_length = 5 * 1024 * 1024 + 1;
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success)
            .with_source_content_length(source_content_length);
        let op = s3_express_operator(transport.clone());

        op.copy_with("source", "target")
            .chunk(5 * 1024 * 1024)
            .source_content_length_hint(source_content_length)
            .await
            .expect("multipart copy must succeed");

        let requests = transport.requests();
        let part_copies = requests
            .iter()
            .filter(|request| {
                request.headers.contains_key(constants::X_AMZ_COPY_SOURCE)
                    && request
                        .uri
                        .query()
                        .is_some_and(|query| query.contains("partNumber="))
            })
            .collect::<Vec<_>>();
        assert_eq!(part_copies.len(), 2);
        for request in part_copies {
            assert_eq!(
                header(request, "x-amz-security-token"),
                "source-session-token"
            );
            assert!(!request.headers.contains_key("x-amz-s3session-token"));
            assert!(header(request, "authorization").contains("source-access-key/"));
            assert!(
                !request
                    .headers
                    .contains_key(constants::X_AMZ_COPY_SOURCE_IF_MATCH)
            );
        }
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.uri.query() == Some("session"))
                .count(),
            1,
            "session operations in multipart copy must share one session"
        );
    }

    #[tokio::test]
    async fn test_s3_copy_replaces_metadata() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success);
        let op = s3_general_operator(transport.clone());

        op.copy_with("source", "source")
            .user_metadata([("mtime".to_owned(), "42".to_owned())])
            .content_type("text/plain")
            .content_language("fr-CA, en")
            .source_content_length_hint(7)
            .await
            .expect("same-key copy with new metadata must succeed");

        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            header(&requests[0], constants::X_AMZ_METADATA_DIRECTIVE),
            "REPLACE"
        );
        assert_eq!(header(&requests[0], "x-amz-meta-mtime"), "42");
        assert_eq!(header(&requests[0], "content-type"), "text/plain");
        assert_eq!(header(&requests[0], "content-language"), "fr-CA, en");

        op.copy_with("source", "target")
            .source_content_length_hint(7)
            .await
            .expect("copy must succeed");
        let requests = transport.requests();
        assert!(
            !requests[1]
                .headers
                .contains_key(constants::X_AMZ_METADATA_DIRECTIVE)
        );
    }

    #[tokio::test]
    async fn test_s3_multipart_copy_sets_metadata_on_create() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success);
        let op = s3_general_operator(transport.clone());

        op.copy_with("source", "target")
            .user_metadata([("mtime".to_owned(), "42".to_owned())])
            .content_language("es-MX")
            .source_content_length_hint(5 * 1024 * 1024 + 1)
            .chunk(5 * 1024 * 1024)
            .await
            .expect("multipart copy must succeed");

        let requests = transport.requests();
        let create = requests
            .iter()
            .find(|request| request.uri.query() == Some("uploads"))
            .expect("CreateMultipartUpload request must be captured");
        assert_eq!(header(create, "x-amz-meta-mtime"), "42");
        assert_eq!(header(create, "content-language"), "es-MX");
    }

    #[tokio::test]
    async fn test_s3_multipart_copy_without_hint_pins_every_part() {
        let source_content_length = 5 * 1024 * 1024 + 1;
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success)
            .with_source_content_length(source_content_length);
        let op = s3_general_operator(transport.clone());

        op.copy_with("source", "target")
            .chunk(5 * 1024 * 1024)
            .await
            .expect("multipart copy must succeed");

        let requests = transport.requests();
        let part_copies = requests
            .iter()
            .filter(|request| {
                request.headers.contains_key(constants::X_AMZ_COPY_SOURCE)
                    && request
                        .uri
                        .query()
                        .is_some_and(|query| query.contains("partNumber="))
            })
            .collect::<Vec<_>>();
        assert_eq!(part_copies.len(), 2);
        for request in part_copies {
            assert_eq!(
                header(request, constants::X_AMZ_COPY_SOURCE_IF_MATCH),
                "\"source-etag\""
            );
        }
    }

    #[tokio::test]
    async fn test_s3_express_writer_copy_from_uses_iam_credentials() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success);
        let op = s3_express_operator(transport.clone());

        let mut writer = op.writer("target").await.expect("writer must open");
        writer
            .copy_from("source", 0..5 * 1024 * 1024_u64)
            .await
            .expect("writer copy must succeed");
        writer.close().await.expect("writer must close");

        let requests = transport.requests();
        let part_copy = requests
            .iter()
            .find(|request| {
                request.headers.contains_key(constants::X_AMZ_COPY_SOURCE)
                    && request
                        .uri
                        .query()
                        .is_some_and(|query| query.contains("partNumber="))
            })
            .expect("UploadPartCopy request must be captured");
        assert_eq!(
            header(part_copy, "x-amz-security-token"),
            "source-session-token"
        );
        assert!(!part_copy.headers.contains_key("x-amz-s3session-token"));
        assert!(header(part_copy, "authorization").contains("source-access-key/"));
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.uri.query() == Some("session"))
                .count(),
            1,
            "writer multipart operations must share one session"
        );
    }

    #[tokio::test]
    async fn test_s3_express_create_session_error_mapping() {
        for (outcome, kind, temporary) in [
            (
                CreateSessionOutcome::Status(StatusCode::FORBIDDEN),
                ErrorKind::PermissionDenied,
                false,
            ),
            (
                CreateSessionOutcome::Status(StatusCode::TOO_MANY_REQUESTS),
                ErrorKind::RateLimited,
                true,
            ),
            (
                CreateSessionOutcome::Status(StatusCode::INTERNAL_SERVER_ERROR),
                ErrorKind::Unexpected,
                true,
            ),
            (
                CreateSessionOutcome::TemporaryTransportError,
                ErrorKind::Unexpected,
                true,
            ),
            (
                CreateSessionOutcome::Body(
                    "<CreateSessionResult><Credentials>\
                     <SecretAccessKey>session-secret-key</SecretAccessKey>\
                     <AccessKeyId>session-access-key</AccessKeyId>\
                     <Expiration>2099-01-01T00:05:00Z</Expiration>\
                     </Credentials></CreateSessionResult>",
                ),
                ErrorKind::Unexpected,
                false,
            ),
            (
                CreateSessionOutcome::Body(
                    "<CreateSessionResult><Credentials>\
                     <SessionToken>session-token</SessionToken>\
                     <SecretAccessKey>session-secret-key</SecretAccessKey>\
                     <AccessKeyId>session-access-key</AccessKeyId>\
                     <Expiration>2020-01-01T00:05:00Z</Expiration>\
                     </Credentials></CreateSessionResult>",
                ),
                ErrorKind::Unexpected,
                false,
            ),
        ] {
            let transport = S3ExpressMockTransport::new(outcome);
            let op = s3_express_operator(transport);
            let err = op
                .write("test", "body")
                .await
                .expect_err("CreateSession failure must reach the caller");
            assert_eq!(err.kind(), kind);
            assert_eq!(err.is_temporary(), temporary);
        }
    }

    #[derive(Debug)]
    struct FailingOnceCredentialProvider {
        error: Mutex<Option<reqsign_core::Error>>,
        calls: Arc<AtomicUsize>,
    }

    impl ProvideCredential for FailingOnceCredentialProvider {
        type Credential = Credential;

        async fn provide_credential(
            &self,
            ctx: &Context,
        ) -> reqsign_core::Result<Option<Self::Credential>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(err) = self.error.lock().expect("lock poisoned").take() {
                return Err(err);
            }
            StaticCredentialProvider::new("provider-access-key", "provider-secret-key")
                .provide_credential(ctx)
                .await
        }
    }

    fn s3_general_operator_with_provider(
        error: reqsign_core::Error,
        transport: S3ExpressMockTransport,
    ) -> (Operator, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = FailingOnceCredentialProvider {
            error: Mutex::new(Some(error)),
            calls: calls.clone(),
        };
        let backend = S3Builder::default()
            .bucket("example")
            .region("us-west-2")
            .disable_config_load()
            .disable_ec2_metadata()
            .build_inner(S3_SCHEME)
            .expect("S3 backend must build");
        let mut core = Arc::into_inner(backend.core).expect("S3 core must not be shared");
        core.signer_spec.base_provider = Arc::new(provider);
        core.signer_spec.assume_role = None;
        core.signers = HttpTransportCache::new();
        let service = Arc::new(S3Backend {
            core: Arc::new(core),
        }) as Servicer;
        let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
        (Operator::from_parts(ctx, service), calls)
    }

    #[tokio::test]
    async fn test_s3_retryable_credential_error_is_temporary() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success);
        let (op, calls) = s3_general_operator_with_provider(
            reqsign_core::Error::unexpected("failed to send AWS STS AssumeRole request")
                .set_retryable(true),
            transport.clone(),
        );

        let err = op
            .stat("test")
            .await
            .expect_err("credential failure must reach the caller");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.is_temporary());
        assert!(transport.requests().is_empty());

        op.stat("test")
            .await
            .expect("retry must load the credential and sign the request");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(header(&transport.requests()[0], "authorization").contains("provider-access-key/"));
    }

    #[tokio::test]
    async fn test_s3_persistent_credential_error_is_not_temporary() {
        let transport = S3ExpressMockTransport::new(CreateSessionOutcome::Success);
        let (op, calls) = s3_general_operator_with_provider(
            reqsign_core::Error::credential_invalid("failed to load signing credential"),
            transport.clone(),
        );

        let err = op
            .stat("test")
            .await
            .expect_err("credential failure must reach the caller");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(transport.requests().is_empty());
    }

    #[derive(Clone)]
    struct CompleteMultipartErrorTransport {
        complete_body: &'static str,
    }

    impl HttpTransport for CompleteMultipartErrorTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let query = req.uri().query().unwrap_or_default();
            if req.headers().contains_key(constants::X_AMZ_COPY_SOURCE) {
                return Ok(S3ExpressMockTransport::response(
                    StatusCode::OK,
                    "<CopyPartResult><ETag>\"etag\"</ETag>\
                     <LastModified>2026-08-24T00:00:00Z</LastModified></CopyPartResult>",
                ));
            }
            if query == "uploads" {
                return Ok(S3ExpressMockTransport::response(
                    StatusCode::OK,
                    "<InitiateMultipartUploadResult><UploadId>upload-id</UploadId>\
                     </InitiateMultipartUploadResult>",
                ));
            }
            // Abort cleanup must not fall through to the UploadPart 200 OK branch.
            if req.method() == Method::DELETE {
                return Ok(S3ExpressMockTransport::response(
                    StatusCode::NO_CONTENT,
                    Bytes::new(),
                ));
            }
            if req.method() == Method::POST && query.contains("uploadId=") {
                assert_eq!(
                    req.headers()
                        .get(http::header::IF_MATCH)
                        .and_then(|v| v.to_str().ok()),
                    Some("\"expected\""),
                    "CompleteMultipartUpload must carry If-Match"
                );
                return Ok(S3ExpressMockTransport::response(
                    StatusCode::NOT_FOUND,
                    self.complete_body,
                ));
            }

            let mut response = S3ExpressMockTransport::response(StatusCode::OK, Bytes::new());
            response
                .headers_mut()
                .insert(http::header::ETAG, HeaderValue::from_static("\"etag\""));
            Ok(response)
        }
    }

    #[tokio::test]
    async fn test_complete_multipart_upload_error_classification_with_if_match() {
        const PART_SIZE: usize = 5 * 1024 * 1024;
        let part_payload = Bytes::from(vec![0u8; PART_SIZE]);
        let cases = [
            (
                "NoSuchUpload 404",
                "<Error><Code>NoSuchUpload</Code><Message>The specified upload does not exist.</Message></Error>",
                ErrorKind::Unexpected,
                Some("NoSuchUpload"),
            ),
            (
                "NoSuchKey 404",
                "<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>",
                ErrorKind::ConditionNotMatch,
                None,
            ),
        ];

        for (label, complete_body, expected_kind, expected_msg_substr) in cases {
            let transport = CompleteMultipartErrorTransport { complete_body };
            let op = Operator::new(
                S3Builder::default()
                    .bucket("example")
                    .region("us-west-2")
                    .skip_signature()
                    .disable_config_load()
                    .disable_ec2_metadata(),
            )
            .unwrap()
            .with_context(
                OperationContext::new()
                    .with_http_transport(HttpTransporter::new(transport.clone())),
            );

            let mut writer = op
                .writer_with("target")
                .if_match("\"expected\"")
                .chunk(PART_SIZE)
                .await
                .unwrap();
            writer.write(part_payload.clone()).await.unwrap();
            writer.write("x").await.unwrap();
            let err = writer
                .close()
                .await
                .expect_err("multipart write close must fail");
            assert_eq!(err.kind(), expected_kind, "write {label}");
            assert!(!err.is_temporary(), "write {label}");
            if let Some(msg) = expected_msg_substr {
                assert!(err.to_string().contains(msg), "write {label}: {err}");
            }

            let err = op
                .copy_with("source", "target")
                .if_match("\"expected\"")
                .source_content_length_hint(PART_SIZE as u64 + 1)
                .chunk(PART_SIZE)
                .await
                .expect_err("multipart copy must fail");
            assert_eq!(err.kind(), expected_kind, "copy {label}");
            assert!(!err.is_temporary(), "copy {label}");
            if let Some(msg) = expected_msg_substr {
                assert!(err.to_string().contains(msg), "copy {label}: {err}");
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[derive(Debug)]
    struct ExpiredSsoSession;

    #[cfg(not(target_arch = "wasm32"))]
    impl reqsign_core::FileRead for ExpiredSsoSession {
        async fn file_read(&self, path: &str) -> reqsign_core::Result<Vec<u8>> {
            let path = std::path::Path::new(path);
            let contents = if path == std::path::Path::new("test-home/.aws/config") {
                "[profile sso-test]\n\
                 sso_session = test-session\n\
                 sso_account_id = 000000000000\n\
                 sso_role_name = TestRole\n\
                 [sso-session test-session]\n\
                 sso_start_url = https://example.awsapps.com/start\n\
                 sso_region = us-west-2\n"
            } else if path.starts_with("test-home/.aws/sso/cache") {
                r#"{
                    "accessToken": "expired-token",
                    "expiresAt": "2000-01-01T00:00:00Z",
                    "refreshToken": "test-refresh-token",
                    "clientId": "test-client-id",
                    "clientSecret": "test-client-secret",
                    "registrationExpiresAt": "2099-01-01T00:00:00Z"
                }"#
            } else {
                return Err(reqsign_core::Error::unexpected("file not found"));
            };
            Ok(contents.as_bytes().to_vec())
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl reqsign_core::HttpSend for ExpiredSsoSession {
        async fn http_send(
            &self,
            request: Request<Bytes>,
        ) -> reqsign_core::Result<Response<Bytes>> {
            assert_eq!(request.method(), Method::POST);
            assert_eq!(request.uri(), "https://oidc.us-west-2.amazonaws.com/token");
            Ok(Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(Bytes::new())
                .unwrap())
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn test_default_credentials_preserve_retryable_sso_refresh_error() {
        let backend = S3Builder::default()
            .bucket("example")
            .region("us-west-2")
            .endpoint("https://s3.us-west-2.amazonaws.com")
            .profile("sso-test")
            .disable_ec2_metadata()
            .build_inner(S3_SCHEME)
            .unwrap();
        let ctx = Context::new()
            .with_env(reqsign_core::StaticEnv {
                home_dir: Some("test-home".into()),
                ..Default::default()
            })
            .with_file_read(ExpiredSsoSession)
            .with_http_send(ExpiredSsoSession);
        let (mut parts, ()) = Request::get("https://example.s3.us-west-2.amazonaws.com/test")
            .body(())
            .unwrap()
            .into_parts();

        let err = backend
            .core
            .signer_spec
            .build(ctx)
            .default()
            .sign(&mut parts, None)
            .await
            .unwrap_err();
        assert!(err.is_retryable(), "{err:?}");
        assert!(err.to_string().contains("SSO token refresh returned 503"));
    }

    #[derive(Debug, Clone, Default)]
    struct AmbientCredentialSources {
        file_reads: Arc<AtomicUsize>,
        command_calls: Arc<AtomicUsize>,
        http_calls: Arc<AtomicUsize>,
    }

    impl reqsign_core::FileRead for AmbientCredentialSources {
        async fn file_read(&self, path: &str) -> reqsign_core::Result<Vec<u8>> {
            self.file_reads.fetch_add(1, Ordering::SeqCst);
            let path = std::path::Path::new(path);
            let contents = if path == std::path::Path::new("test-home/.aws/config") {
                "[profile sso-test]\n\
                 sso_session = test-session\n\
                 sso_account_id = 000000000000\n\
                 sso_role_name = TestRole\n\
                 [sso-session test-session]\n\
                 sso_start_url = https://example.awsapps.com/start\n\
                 sso_region = us-east-1\n\
                 [profile process-test]\n\
                 credential_process = fake-cred-helper\n"
            } else if path.starts_with("test-home/.aws/sso/cache") {
                r#"{
                    "accessToken": "valid-sso-token",
                    "expiresAt": "2099-01-01T00:00:00Z"
                }"#
            } else if path == std::path::Path::new("test-home/web-identity-token") {
                "test-web-identity-jwt"
            } else {
                return Err(reqsign_core::Error::unexpected("file not found"));
            };
            Ok(contents.as_bytes().to_vec())
        }
    }

    impl reqsign_core::CommandExecute for AmbientCredentialSources {
        async fn command_execute(
            &self,
            program: &str,
            args: &[&str],
        ) -> reqsign_core::Result<reqsign_core::CommandOutput> {
            self.command_calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(program, "fake-cred-helper");
            assert!(args.is_empty());
            Ok(reqsign_core::CommandOutput {
                status: 0,
                stdout:
                    br#"{"Version":1,"AccessKeyId":"process-ak","SecretAccessKey":"process-sk"}"#
                        .to_vec(),
                stderr: Vec::new(),
            })
        }
    }

    impl reqsign_core::HttpSend for AmbientCredentialSources {
        async fn http_send(
            &self,
            request: Request<Bytes>,
        ) -> reqsign_core::Result<Response<Bytes>> {
            self.http_calls.fetch_add(1, Ordering::SeqCst);
            let host = request.uri().host().unwrap_or_default();
            let body = if host == "portal.sso.us-east-1.amazonaws.com" {
                r#"{"roleCredentials":{"accessKeyId":"sso-ak","secretAccessKey":"sso-sk","sessionToken":"sso-token","expiration":4102444800000}}"#
            } else if host.starts_with("sts.") {
                "<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
                    <AssumeRoleWithWebIdentityResult>\
                        <Credentials>\
                            <AccessKeyId>web-identity-ak</AccessKeyId>\
                            <SecretAccessKey>web-identity-sk</SecretAccessKey>\
                            <SessionToken>web-identity-token</SessionToken>\
                            <Expiration>2099-01-01T00:00:00Z</Expiration>\
                        </Credentials>\
                    </AssumeRoleWithWebIdentityResult>\
                 </AssumeRoleWithWebIdentityResponse>"
            } else if host == "169.254.170.2" {
                r#"{"AccessKeyId":"ecs-ak","SecretAccessKey":"ecs-sk","Token":"ecs-token","Expiration":"2099-01-01T00:00:00Z"}"#
            } else {
                panic!("unexpected HTTP credential request: {}", request.uri());
            };
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Bytes::from_static(body.as_bytes()))
                .unwrap())
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn test_disable_config_load_skips_sso_and_process_credentials() {
        for (profile, expected_ak, expect_command, expect_http) in [
            ("sso-test", "sso-ak", 0, 1),
            ("process-test", "process-ak", 1, 0),
        ] {
            let sources = AmbientCredentialSources::default();
            let enabled_backend = S3Builder::default()
                .bucket("example")
                .region("us-east-1")
                .endpoint("https://s3.us-east-1.amazonaws.com")
                .profile(profile)
                .disable_ec2_metadata()
                .build_inner(S3_SCHEME)
                .unwrap();
            let ctx = Context::new()
                .with_env(reqsign_core::StaticEnv {
                    home_dir: Some("test-home".into()),
                    envs: HashMap::from([("AWS_PROFILE".to_string(), profile.to_string())]),
                })
                .with_file_read(sources.clone())
                .with_command_execute(sources.clone())
                .with_http_send(sources.clone());
            let (mut parts, ()) = Request::get("https://s3.us-east-1.amazonaws.com/example/test")
                .body(())
                .unwrap()
                .into_parts();
            enabled_backend
                .core
                .signer_spec
                .build(ctx)
                .default()
                .sign(&mut parts, None)
                .await
                .unwrap_or_else(|err| {
                    panic!("profile {profile} must load without disable_config_load: {err:?}")
                });
            let auth = parts
                .headers
                .get(http::header::AUTHORIZATION)
                .unwrap()
                .to_str()
                .unwrap();
            assert!(
                auth.contains(&format!("Credential={expected_ak}/")),
                "{auth}"
            );
            assert_eq!(sources.command_calls.load(Ordering::SeqCst), expect_command);
            assert_eq!(sources.http_calls.load(Ordering::SeqCst), expect_http);

            let disabled_sources = AmbientCredentialSources::default();
            let disabled_backend = S3Builder::default()
                .bucket("example")
                .region("us-east-1")
                .endpoint("https://s3.us-east-1.amazonaws.com")
                .profile(profile)
                .disable_config_load()
                .disable_ec2_metadata()
                .build_inner(S3_SCHEME)
                .unwrap();
            let ctx = Context::new()
                .with_env(reqsign_core::StaticEnv {
                    home_dir: Some("test-home".into()),
                    envs: HashMap::from([("AWS_PROFILE".to_string(), profile.to_string())]),
                })
                .with_file_read(disabled_sources.clone())
                .with_command_execute(disabled_sources.clone())
                .with_http_send(disabled_sources.clone());
            let (mut parts, ()) = Request::get("https://s3.us-east-1.amazonaws.com/example/test")
                .body(())
                .unwrap()
                .into_parts();
            let err = disabled_backend
                .core
                .signer_spec
                .build(ctx)
                .default()
                .sign(&mut parts, None)
                .await
                .expect_err("disable_config_load must not load sso or process credentials");
            assert_eq!(err.kind(), reqsign_core::ErrorKind::CredentialInvalid);
            assert_eq!(disabled_sources.file_reads.load(Ordering::SeqCst), 0);
            assert_eq!(disabled_sources.command_calls.load(Ordering::SeqCst), 0);
            assert_eq!(disabled_sources.http_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn test_disable_config_load_skips_web_identity_credentials() {
        let env = HashMap::from([
            (
                "AWS_ROLE_ARN".to_string(),
                "arn:aws:iam::000000000000:role/web-identity".to_string(),
            ),
            (
                "AWS_WEB_IDENTITY_TOKEN_FILE".to_string(),
                "test-home/web-identity-token".to_string(),
            ),
            ("AWS_REGION".to_string(), "us-east-1".to_string()),
        ]);

        let sources = AmbientCredentialSources::default();
        let enabled_backend = S3Builder::default()
            .bucket("example")
            .region("us-east-1")
            .endpoint("https://s3.us-east-1.amazonaws.com")
            .disable_ec2_metadata()
            .build_inner(S3_SCHEME)
            .unwrap();
        let ctx = Context::new()
            .with_env(reqsign_core::StaticEnv {
                home_dir: Some("test-home".into()),
                envs: env.clone(),
            })
            .with_file_read(sources.clone())
            .with_command_execute(sources.clone())
            .with_http_send(sources.clone());
        let (mut parts, ()) = Request::get("https://s3.us-east-1.amazonaws.com/example/test")
            .body(())
            .unwrap()
            .into_parts();
        enabled_backend
            .core
            .signer_spec
            .build(ctx)
            .default()
            .sign(&mut parts, None)
            .await
            .expect("web identity must load without disable_config_load");
        let auth = parts
            .headers
            .get(http::header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth.contains("Credential=web-identity-ak/"), "{auth}");
        assert_eq!(sources.http_calls.load(Ordering::SeqCst), 1);

        let disabled_sources = AmbientCredentialSources::default();
        let disabled_backend = S3Builder::default()
            .bucket("example")
            .region("us-east-1")
            .endpoint("https://s3.us-east-1.amazonaws.com")
            .disable_config_load()
            .disable_ec2_metadata()
            .build_inner(S3_SCHEME)
            .unwrap();
        let ctx = Context::new()
            .with_env(reqsign_core::StaticEnv {
                home_dir: Some("test-home".into()),
                envs: env,
            })
            .with_file_read(disabled_sources.clone())
            .with_command_execute(disabled_sources.clone())
            .with_http_send(disabled_sources.clone());
        let (mut parts, ()) = Request::get("https://s3.us-east-1.amazonaws.com/example/test")
            .body(())
            .unwrap()
            .into_parts();
        let err = disabled_backend
            .core
            .signer_spec
            .build(ctx)
            .default()
            .sign(&mut parts, None)
            .await
            .expect_err("disable_config_load must not load web identity credentials");
        assert_eq!(err.kind(), reqsign_core::ErrorKind::CredentialInvalid);
        assert_eq!(disabled_sources.file_reads.load(Ordering::SeqCst), 0);
        assert_eq!(disabled_sources.http_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_disable_config_load_keeps_container_credentials_enabled() {
        let sources = AmbientCredentialSources::default();
        let backend = S3Builder::default()
            .bucket("example")
            .region("us-east-1")
            .endpoint("https://s3.us-east-1.amazonaws.com")
            .disable_config_load()
            .disable_ec2_metadata()
            .container_credentials_endpoint("http://169.254.170.2/v2/credentials/test")
            .build_inner(S3_SCHEME)
            .unwrap();
        let ctx = Context::new()
            .with_env(reqsign_core::StaticEnv {
                home_dir: Some("test-home".into()),
                envs: HashMap::from([
                    ("AWS_PROFILE".to_string(), "sso-test".to_string()),
                    (
                        "AWS_ROLE_ARN".to_string(),
                        "arn:aws:iam::000000000000:role/web-identity".to_string(),
                    ),
                    (
                        "AWS_WEB_IDENTITY_TOKEN_FILE".to_string(),
                        "test-home/web-identity-token".to_string(),
                    ),
                ]),
            })
            .with_file_read(sources.clone())
            .with_command_execute(sources.clone())
            .with_http_send(sources.clone());
        let (mut parts, ()) = Request::get("https://s3.us-east-1.amazonaws.com/example/test")
            .body(())
            .unwrap()
            .into_parts();

        backend
            .core
            .signer_spec
            .build(ctx)
            .default()
            .sign(&mut parts, None)
            .await
            .expect("container credentials must still load when disable_config_load is set");
        let auth = parts
            .headers
            .get(http::header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth.contains("Credential=ecs-ak/"), "{auth}");
        assert_eq!(sources.file_reads.load(Ordering::SeqCst), 0);
        assert_eq!(sources.command_calls.load(Ordering::SeqCst), 0);
        assert_eq!(sources.http_calls.load(Ordering::SeqCst), 1);
    }

    struct StsErrorResponse {
        status: StatusCode,
        body: &'static str,
    }

    impl HttpTransport for StsErrorResponse {
        async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
            assert_eq!(request.uri().host(), Some("sts.us-west-2.amazonaws.com"));
            assert!(request.uri().query().unwrap().contains("Action=AssumeRole"));
            let body = Buffer::from(self.body);
            let size = body.len() as u64;
            Ok(Response::builder()
                .status(self.status)
                .body(HttpBody::new(futures::stream::iter([Ok(body)]), Some(size)))
                .unwrap())
        }
    }

    #[tokio::test]
    async fn test_assume_role_preserves_sts_error() {
        for (status, code, body, temporary) in [
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailable",
                "<ErrorResponse><Error><Code>ServiceUnavailable</Code>\
                 <Message>Try again</Message></Error></ErrorResponse>",
                true,
            ),
            (
                StatusCode::FORBIDDEN,
                "AccessDenied",
                "<ErrorResponse><Error><Code>AccessDenied</Code>\
                 <Message>Forbidden</Message></Error></ErrorResponse>",
                false,
            ),
        ] {
            let op = Operator::new(
                S3Builder::default()
                    .bucket("example")
                    .region("us-west-2")
                    .disable_config_load()
                    .disable_ec2_metadata()
                    .access_key_id("test-access-key")
                    .secret_access_key("test-secret-key")
                    .role_arn("arn:aws:iam::000000000000:role/example"),
            )
            .unwrap()
            .with_context(
                OperationContext::new()
                    .with_http_transport(HttpTransporter::new(StsErrorResponse { status, body })),
            );

            let err = op.stat("test").await.unwrap_err();
            assert_eq!(err.is_temporary(), temporary, "{err:?}");
            let source = std::error::Error::source(&err).unwrap();
            assert!(source.to_string().contains(code), "{err:?}");
        }
    }

    #[tokio::test]
    async fn test_custom_credential_chain_overrides_assume_role_and_static_keys() {
        let chain = ProvideCredentialChain::new().push(StaticCredentialProvider::new(
            "custom-access-key",
            "custom-secret-key",
        ));
        let op = Operator::new(
            S3Builder::default()
                .bucket("example")
                .region("us-west-2")
                .disable_config_load()
                .disable_ec2_metadata()
                .access_key_id("test-access-key")
                .secret_access_key("test-secret-key")
                .role_arn("arn:aws:iam::000000000000:role/example")
                .credential_provider_chain(chain),
        )
        .unwrap();

        let request = op
            .presign_read("test", std::time::Duration::from_secs(60))
            .await
            .unwrap();
        let url = Url::parse(&request.uri().to_string()).unwrap();
        let credential = url
            .query_pairs()
            .find_map(|(key, value)| (key == "X-Amz-Credential").then_some(value))
            .unwrap();
        assert_eq!(credential.split('/').next(), Some("custom-access-key"));
    }

    #[derive(Debug)]
    struct CountingCredentialProvider {
        calls: Arc<AtomicUsize>,
    }

    impl reqsign_core::ProvideCredential for CountingCredentialProvider {
        type Credential = Credential;

        async fn provide_credential(
            &self,
            ctx: &Context,
        ) -> reqsign_core::Result<Option<Credential>> {
            let id = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            StaticCredentialProvider::new(&format!("ak-{id}"), "secret-key")
                .provide_credential(ctx)
                .await
        }
    }

    struct TrackingHeadTransport {
        auth_headers: Arc<Mutex<Vec<String>>>,
        dropped: Option<Arc<std::sync::atomic::AtomicBool>>,
    }

    impl Drop for TrackingHeadTransport {
        fn drop(&mut self) {
            if let Some(dropped) = &self.dropped {
                dropped.store(true, Ordering::SeqCst);
            }
        }
    }

    impl HttpTransport for TrackingHeadTransport {
        async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
            let auth = request
                .headers()
                .get(http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            self.auth_headers.lock().unwrap().push(auth);
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_LENGTH, "0")
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
                .unwrap())
        }
    }

    #[tokio::test]
    async fn test_s3_transport_scoped_signer_cache_reuse_isolation_and_eviction() {
        let loads = Arc::new(AtomicUsize::new(0));
        let chain = ProvideCredentialChain::new().push(CountingCredentialProvider {
            calls: loads.clone(),
        });

        let auth1 = Arc::new(Mutex::new(Vec::new()));
        let t1 = HttpTransporter::new(TrackingHeadTransport {
            auth_headers: auth1.clone(),
            dropped: None,
        });
        let op = Operator::new(
            S3Builder::default()
                .bucket("example")
                .region("us-west-2")
                .disable_config_load()
                .disable_ec2_metadata()
                .credential_provider_chain(chain),
        )
        .unwrap()
        .with_context(OperationContext::new().with_http_transport(t1));

        // Same transport must reuse the cached signer and its credential cache.
        op.stat("a").await.unwrap();
        op.stat("b").await.unwrap();
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        let h1 = auth1.lock().unwrap().clone();
        assert_eq!(h1.len(), 2);
        assert!(h1[0].contains("Credential=ak-1/"), "{}", h1[0]);
        assert!(h1[1].contains("Credential=ak-1/"), "{}", h1[1]);

        // A derived operator with a distinct transport must get an isolated signer.
        let auth2 = Arc::new(Mutex::new(Vec::new()));
        let t2_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let t2 = HttpTransporter::new(TrackingHeadTransport {
            auth_headers: auth2.clone(),
            dropped: Some(t2_dropped.clone()),
        });
        let op2 = op
            .clone()
            .with_context(OperationContext::new().with_http_transport(t2));
        op2.stat("c").await.unwrap();
        op2.stat("d").await.unwrap();
        assert_eq!(loads.load(Ordering::SeqCst), 2);
        let h2 = auth2.lock().unwrap().clone();
        assert_eq!(h2.len(), 2);
        assert!(h2[0].contains("Credential=ak-2/"), "{}", h2[0]);
        assert!(h2[1].contains("Credential=ak-2/"), "{}", h2[1]);

        // Dropping the derived operator must drop t2 immediately (no strong cycle in cache).
        drop(op2);
        assert!(t2_dropped.load(Ordering::SeqCst));

        // Inserting a third transport prunes t2's dead entry and stays isolated.
        let auth3 = Arc::new(Mutex::new(Vec::new()));
        let t3 = HttpTransporter::new(TrackingHeadTransport {
            auth_headers: auth3.clone(),
            dropped: None,
        });
        let op3 = op
            .clone()
            .with_context(OperationContext::new().with_http_transport(t3));
        op3.stat("e").await.unwrap();
        assert_eq!(loads.load(Ordering::SeqCst), 3);
        let h3 = auth3.lock().unwrap().clone();
        assert_eq!(h3.len(), 1);
        assert!(h3[0].contains("Credential=ak-3/"), "{}", h3[0]);
    }

    #[derive(Clone)]
    struct AssumeRoleFlowTransport {
        sts_access_key: &'static str,
        ecs_calls: Arc<AtomicUsize>,
        sts_calls: Arc<AtomicUsize>,
        s3_auth_headers: Arc<Mutex<Vec<String>>>,
    }

    impl AssumeRoleFlowTransport {
        fn new(sts_access_key: &'static str) -> Self {
            Self {
                sts_access_key,
                ecs_calls: Arc::new(AtomicUsize::new(0)),
                sts_calls: Arc::new(AtomicUsize::new(0)),
                s3_auth_headers: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl HttpTransport for AssumeRoleFlowTransport {
        async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
            let host = request.uri().host().unwrap_or_default();
            if host == "169.254.170.2" {
                self.ecs_calls.fetch_add(1, Ordering::SeqCst);
                let body = Buffer::from(
                    r#"{"AccessKeyId":"ecs-ak","SecretAccessKey":"ecs-sk","Token":"ecs-token","Expiration":"2099-01-01T00:00:00Z"}"#,
                );
                let size = body.len() as u64;
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(HttpBody::new(futures::stream::iter([Ok(body)]), Some(size)))
                    .unwrap());
            }
            if host == "sts.us-west-2.amazonaws.com" {
                assert!(request.uri().query().unwrap().contains("Action=AssumeRole"));
                let auth = request
                    .headers()
                    .get(http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default();
                assert!(auth.contains("Credential=ecs-ak/"), "{auth}");
                self.sts_calls.fetch_add(1, Ordering::SeqCst);
                let xml = format!(
                    "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
                        <AssumeRoleResult>\
                            <Credentials>\
                                <AccessKeyId>{}</AccessKeyId>\
                                <SecretAccessKey>sts-sk</SecretAccessKey>\
                                <SessionToken>sts-token</SessionToken>\
                                <Expiration>2099-01-01T00:00:00Z</Expiration>\
                            </Credentials>\
                        </AssumeRoleResult>\
                    </AssumeRoleResponse>",
                    self.sts_access_key
                );
                let body = Buffer::from(xml);
                let size = body.len() as u64;
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(HttpBody::new(futures::stream::iter([Ok(body)]), Some(size)))
                    .unwrap());
            }

            let auth = request
                .headers()
                .get(http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            self.s3_auth_headers.lock().unwrap().push(auth);
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_LENGTH, "0")
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
                .unwrap())
        }
    }

    #[tokio::test]
    async fn test_s3_assume_role_sts_signer_uses_operation_context_transport() {
        let t1 = AssumeRoleFlowTransport::new("ASIATESTKEY000000001");
        let op = Operator::new(
            S3Builder::default()
                .bucket("example")
                .region("us-west-2")
                .disable_config_load()
                .disable_ec2_metadata()
                .container_credentials_endpoint("http://169.254.170.2/v2/credentials/test")
                .role_arn("arn:aws:iam::000000000000:role/example"),
        )
        .unwrap()
        .with_context(
            OperationContext::new().with_http_transport(HttpTransporter::new(t1.clone())),
        );

        // Both ECS and STS requests must run over the OperationContext transport and be cached.
        op.stat("test").await.unwrap();
        op.stat("test").await.unwrap();
        assert_eq!(t1.ecs_calls.load(Ordering::SeqCst), 1);
        assert_eq!(t1.sts_calls.load(Ordering::SeqCst), 1);
        let h1 = t1.s3_auth_headers.lock().unwrap().clone();
        assert_eq!(h1.len(), 2);
        assert!(
            h1[0].contains("Credential=ASIATESTKEY000000001/"),
            "{}",
            h1[0]
        );
        assert!(
            h1[1].contains("Credential=ASIATESTKEY000000001/"),
            "{}",
            h1[1]
        );

        // A derived operator with another transport must assume role independently over t2.
        let t2 = AssumeRoleFlowTransport::new("ASIATESTKEY000000002");
        let op2 = op.clone().with_context(
            OperationContext::new().with_http_transport(HttpTransporter::new(t2.clone())),
        );
        op2.stat("test").await.unwrap();
        op2.stat("test").await.unwrap();
        assert_eq!(t2.ecs_calls.load(Ordering::SeqCst), 1);
        assert_eq!(t2.sts_calls.load(Ordering::SeqCst), 1);
        let h2 = t2.s3_auth_headers.lock().unwrap().clone();
        assert_eq!(h2.len(), 2);
        assert!(
            h2[0].contains("Credential=ASIATESTKEY000000002/"),
            "{}",
            h2[0]
        );
        assert!(
            h2[1].contains("Credential=ASIATESTKEY000000002/"),
            "{}",
            h2[1]
        );
    }

    #[test]
    fn test_profile() {
        let builder = S3Builder::default().profile("selected");
        assert_eq!(builder.config.profile.as_deref(), Some("selected"));
    }

    #[tokio::test]
    async fn test_presign_write_rejects_append() {
        let op = Operator::new(
            S3Builder::default()
                .bucket("test")
                .region("us-east-1")
                .endpoint("http://127.0.0.1:9000")
                .access_key_id("test-access-key")
                .secret_access_key("test-secret-key")
                .disable_config_load()
                .disable_ec2_metadata(),
        )
        .unwrap();
        assert!(op.info().capability().write_can_append);

        let result = op
            .presign_write_options(
                "test.txt",
                std::time::Duration::from_secs(60),
                options::WriteOptions {
                    append: true,
                    ..Default::default()
                },
            )
            .await;
        let Err(err) = result else {
            panic!("S3 presign must reject append");
        };
        assert_eq!(err.kind(), ErrorKind::Unsupported);
    }

    #[tokio::test]
    async fn test_presign_write_omits_checksum_headers() {
        for (algorithm, header_name) in
            [("crc32c", "x-amz-checksum-crc32c"), ("md5", "content-md5")]
        {
            let op = Operator::new(
                S3Builder::default()
                    .bucket("test")
                    .region("us-east-1")
                    .endpoint("http://127.0.0.1:9000")
                    .access_key_id("test-access-key")
                    .secret_access_key("test-secret-key")
                    .disable_config_load()
                    .disable_ec2_metadata()
                    .checksum_algorithm(algorithm),
            )
            .unwrap();

            let presigned = op
                .presign_write("test.txt", std::time::Duration::from_secs(60))
                .await
                .unwrap();

            assert!(
                !presigned.header().contains_key(header_name),
                "presign_write with {algorithm} must not attach {header_name}: {:?}",
                presigned.header()
            );

            let url = url::Url::parse(&presigned.uri().to_string()).unwrap();
            let signed_headers = url
                .query_pairs()
                .find_map(|(k, v)| (k == "X-Amz-SignedHeaders").then(|| v.into_owned()))
                .expect("presigned URL must contain X-Amz-SignedHeaders");
            assert!(
                !signed_headers
                    .split(';')
                    .any(|h| h.eq_ignore_ascii_case(header_name)),
                "presign_write with {algorithm} must not sign {header_name}: {signed_headers}"
            );
        }
    }

    #[test]
    fn test_checksum_capability_only_for_s3_scheme() {
        let builder = || {
            S3Builder::default()
                .bucket("test")
                .region("us-east-1")
                .endpoint("http://127.0.0.1:9000")
        };

        let cap = builder().build().unwrap().capability();
        assert!(cap.write_with_checksum);

        let cap = builder()
            .build_with_scheme(crate::MINIO_SCHEME)
            .unwrap()
            .capability();
        assert!(!cap.write_with_checksum);
    }

    #[test]
    fn test_s3_write_rejects_unsupported_checksum_intents_synchronously() {
        let backend = S3Builder::default()
            .bucket("test")
            .region("us-west-2")
            .build()
            .unwrap();
        let capability = backend.capability();
        for (append, intent) in [
            (true, WriteChecksum::Any),
            (true, WriteChecksum::Fast),
            (false, WriteChecksum::Algo(ChecksumAlgo::Sha256)),
            (
                false,
                WriteChecksum::Expected(Checksum::new(ChecksumAlgo::Md5, [0; 16])),
            ),
        ] {
            let (args, _) = OpWrite::from_options(
                &capability,
                options::WriteOptions {
                    append,
                    checksum: Some(intent),
                    ..Default::default()
                },
            )
            .expect("core leaves these intents for the service to validate");
            let result = backend.write(&OperationContext::new(), "file", args);
            assert!(matches!(result, Err(error) if error.kind() == ErrorKind::Unsupported));
        }
    }

    #[test]
    fn test_write_can_copy_from_capability() {
        let backend = S3Builder::default()
            .bucket("test")
            .region("us-east-1")
            .endpoint("http://127.0.0.1:9000")
            .build()
            .unwrap();
        assert!(backend.capability().write_can_copy_from);
        assert!(backend.capability().write_can_multi);
        assert_eq!(
            backend.capability().write_multi_min_size,
            Some(5 * 1024 * 1024)
        );
        assert!(backend.capability().write_multi_max_size.is_some());

        let backend = S3Builder::default()
            .bucket("test")
            .region("us-east-1")
            .endpoint("http://127.0.0.1:9000")
            .checksum_algorithm("crc32c")
            .build()
            .unwrap();
        assert!(!backend.capability().write_can_copy_from);
        assert!(backend.capability().write_can_multi);
        assert_eq!(
            backend.capability().write_multi_min_size,
            Some(5 * 1024 * 1024)
        );

        let backend = S3Builder::default()
            .bucket("test")
            .region("us-east-1")
            .endpoint("http://127.0.0.1:9000")
            .checksum_algorithm("md5")
            .build()
            .unwrap();
        assert!(!backend.capability().write_can_copy_from);
        assert!(!backend.capability().write_can_multi);
        assert_eq!(backend.capability().write_multi_min_size, None);
        assert_eq!(backend.capability().write_multi_max_size, None);
        #[cfg(target_pointer_width = "64")]
        assert_eq!(
            backend.capability().write_total_max_size,
            Some(5 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn test_is_valid_bucket() {
        let bucket_cases = vec![
            ("", false, false),
            ("test", false, true),
            ("test.xyz", false, true),
            ("", true, false),
            ("test", true, true),
            ("test.xyz", true, false),
        ];

        for (bucket, enable_virtual_host_style, expected) in bucket_cases {
            let mut b = S3Builder::default();
            b = b.bucket(bucket);
            if enable_virtual_host_style {
                b = b.enable_virtual_host_style();
            }
            assert_eq!(S3Builder::is_bucket_valid(&b.config), expected)
        }
    }

    #[test]
    fn test_build_endpoint() {
        let endpoint_cases = vec![
            Some("s3.amazonaws.com"),
            Some("https://s3.amazonaws.com"),
            Some("https://s3.us-east-2.amazonaws.com"),
            None,
        ];

        for endpoint in &endpoint_cases {
            let mut b = S3Builder::default().bucket("test");
            if let Some(endpoint) = endpoint {
                b = b.endpoint(endpoint);
            }

            let endpoint = S3Builder::build_endpoint(&b.config, "us-east-2");
            assert_eq!(endpoint, "https://s3.us-east-2.amazonaws.com/test");
        }

        for endpoint in &endpoint_cases {
            let mut b = S3Builder::default()
                .bucket("test")
                .enable_virtual_host_style();
            if let Some(endpoint) = endpoint {
                b = b.endpoint(endpoint);
            }

            let endpoint = S3Builder::build_endpoint(&b.config, "us-east-2");
            assert_eq!(endpoint, "https://test.s3.us-east-2.amazonaws.com");
        }
    }

    #[tokio::test]
    async fn test_build_endpoint_scheme_prefix() {
        let cases = [
            (
                "http-proxy.internal:9000",
                "https://http-proxy.internal:9000/test",
            ),
            ("httpbin.org", "https://httpbin.org/test"),
            ("https-proxy.internal", "https://https-proxy.internal/test"),
            ("http://127.0.0.1:9000", "http://127.0.0.1:9000/test"),
            ("HTTP://127.0.0.1:9000", "http://127.0.0.1:9000/test"),
            (
                "HTTPS://s3.amazonaws.com",
                "https://s3.us-east-2.amazonaws.com/test",
            ),
        ];

        for (input, expected) in cases {
            let builder = S3Builder::default().bucket("test").endpoint(input);
            assert_eq!(
                S3Builder::build_endpoint(&builder.config, "us-east-2"),
                expected,
                "endpoint={input}"
            );
        }

        assert_eq!(
            S3Builder::detect_region("HTTPS://s3.amazonaws.com", "test").await,
            Some("us-east-1".to_string())
        );

        let express_builder = S3Builder::default()
            .bucket("example--usw2-az1--x-s3")
            .endpoint("http-proxy.internal:9000");
        assert!(
            S3Builder::resolve_s3_express_config(&express_builder.config, "us-west-2")
                .expect("non-AWS schemeless http-prefixed host must parse cleanly")
                .is_none()
        );
    }

    #[test]
    fn test_resolve_s3_express_config() {
        let directory_bucket = "example--usw2-az1--x-s3";

        for endpoint in [
            None,
            Some("s3.amazonaws.com"),
            Some("https://s3.us-west-2.amazonaws.com"),
            Some("https://s3.us-west-2.amazonaws.com:443"),
            Some("https://example--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com"),
        ] {
            let mut builder = S3Builder::default().bucket(directory_bucket);
            if let Some(endpoint) = endpoint {
                builder = builder.endpoint(endpoint);
            }

            let config = S3Builder::resolve_s3_express_config(&builder.config, "us-west-2")
                .expect("valid directory bucket must resolve")
                .expect("AWS directory bucket must enable session authentication");
            assert_eq!(
                config.endpoint(),
                "https://example--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com"
            );
        }
    }

    #[test]
    fn test_s3_express_classification_does_not_change_compatible_services() {
        let builder = S3Builder::default()
            .bucket("example--usw2-az1--x-s3")
            .endpoint("https://s3.example.com");

        assert!(
            S3Builder::resolve_s3_express_config(&builder.config, "us-west-2")
                .expect("custom endpoint classification must succeed")
                .is_none()
        );
    }

    #[test]
    fn test_s3_express_capabilities() {
        let backend = S3Builder::default()
            .bucket("example--usw2-az1--x-s3")
            .region("us-west-2")
            .disable_config_load()
            .disable_ec2_metadata()
            .build()
            .expect("S3 Express backend must build");
        let capability = backend.capability();

        assert!(!capability.stat_with_version);
        assert!(!capability.read_with_version);
        assert!(!capability.write_with_content_encoding);
        assert!(!capability.delete_with_version);
        assert!(!capability.copy_with_source_version);
        assert!(!capability.restore);
        assert!(!capability.restore_with_version);
        assert!(!capability.restore_with_if_not_exists);
        assert!(!capability.list_with_start_after);
        assert!(!capability.list_with_recursive);
        assert!(!capability.list_with_versions);
        assert!(!capability.list_with_deleted);
    }

    #[test]
    fn test_invalid_aws_directory_bucket_configuration_is_rejected() {
        let builder = S3Builder::default().bucket("example--usw2-az1--x-s3");

        let err = S3Builder::resolve_s3_express_config(&builder.config, "us-east-1")
            .expect_err("zone and region mismatch must be rejected");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn test_unsupported_aws_directory_bucket_endpoints_are_rejected() {
        for endpoint in [
            "https://s3.dualstack.us-west-2.amazonaws.com",
            "https://example--usw2-az1--x-s3.s3express-usw2-az1.dualstack.us-west-2.amazonaws.com",
            "https://example--usw2-az1--x-s3.s3express-use1-az1.us-west-2.amazonaws.com",
            "https://s3.us-west-2.amazonaws.com/path",
            "https://s3.us-west-2.amazonaws.com.cn",
        ] {
            let builder = S3Builder::default()
                .bucket("example--usw2-az1--x-s3")
                .endpoint(endpoint);

            let err = S3Builder::resolve_s3_express_config(&builder.config, "us-west-2")
                .expect_err("unsupported AWS directory bucket endpoint must be rejected");
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid, "endpoint: {endpoint}");
        }
    }

    #[tokio::test]
    async fn test_detect_region() {
        let cases = vec![
            (
                "aws s3 without region in endpoint",
                "https://s3.amazonaws.com",
                "example",
                Some("us-east-1"),
            ),
            (
                "aws s3 with region in endpoint",
                "https://s3.us-east-1.amazonaws.com",
                "example",
                Some("us-east-1"),
            ),
            (
                "oss with public endpoint",
                "https://oss-ap-southeast-1.aliyuncs.com",
                "example",
                Some("oss-ap-southeast-1"),
            ),
            (
                "oss with internal endpoint",
                "https://oss-cn-hangzhou-internal.aliyuncs.com",
                "example",
                Some("oss-cn-hangzhou"),
            ),
            (
                "r2",
                "https://abc.xxxxx.r2.cloudflarestorage.com",
                "example",
                Some("auto"),
            ),
            (
                "neon",
                "https://br-example-a1b2c3d4.storage.c-6.us-east-2.aws.neon.tech",
                "example",
                Some("us-east-2"),
            ),
            (
                "invalid service",
                "https://opendal.apache.org",
                "example",
                None,
            ),
        ];

        for (name, endpoint, bucket, expected) in cases {
            let region = S3Builder::detect_region(endpoint, bucket).await;
            assert_eq!(region.as_deref(), expected, "{name}");
        }
    }

    #[tokio::test]
    async fn test_presign_write_preserves_content_headers() {
        let backend = S3Builder::default()
            .bucket("test")
            .region("us-east-1")
            .skip_signature()
            .disable_config_load()
            .disable_ec2_metadata()
            .build()
            .expect("build");

        let (op, _) = OpWrite::from_options(
            &backend.capability(),
            options::WriteOptions {
                content_type: Some("application/json".to_owned()),
                content_language: Some("fr-CA".to_owned()),
                ..Default::default()
            },
        )
        .unwrap();
        let args = OpPresign::new(op, Duration::from_secs(3600));
        let ctx = OperationContext::new();
        let presigned = backend
            .presign(&ctx, "test.txt", args)
            .await
            .expect("presign")
            .into_presigned_request();

        assert_eq!(
            presigned.header().get(http::header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        assert_eq!(
            presigned
                .header()
                .get(http::header::CONTENT_LANGUAGE)
                .unwrap(),
            "fr-CA"
        );
    }

    #[tokio::test]
    async fn test_presign_stat_encodes_version_id() {
        let backend = S3Builder::default()
            .bucket("test")
            .region("us-east-1")
            .skip_signature()
            .disable_config_load()
            .disable_ec2_metadata()
            .build()
            .expect("build");

        let op: OpStat = options::StatOptions {
            version: Some("a+b/c=d%25&e".to_owned()),
            ..Default::default()
        }
        .into();
        let args = OpPresign::new(op, Duration::from_secs(3600));
        let ctx = OperationContext::new();
        let presigned = backend
            .presign(&ctx, "test.txt", args)
            .await
            .expect("presign")
            .into_presigned_request();

        assert_eq!(
            presigned.uri().to_string(),
            "https://s3.us-east-1.amazonaws.com/test/test.txt?versionId=a%2Bb/c%3Dd%2525%26e"
        );
    }

    #[tokio::test]
    async fn test_presign_read_encodes_version_id() {
        let backend = S3Builder::default()
            .bucket("test")
            .region("us-east-1")
            .skip_signature()
            .disable_config_load()
            .disable_ec2_metadata()
            .build()
            .expect("build");

        let (_, op, _) = options::ReadOptions {
            version: Some("a+b/c=d%25&e".to_owned()),
            ..Default::default()
        }
        .into();
        let args = OpPresign::new(
            PresignOperation::Read(BytesRange::default(), op),
            Duration::from_secs(3600),
        );
        let ctx = OperationContext::new();
        let presigned = backend
            .presign(&ctx, "test.txt", args)
            .await
            .expect("presign")
            .into_presigned_request();

        assert_eq!(
            presigned.uri().to_string(),
            "https://s3.us-east-1.amazonaws.com/test/test.txt?versionId=a%2Bb/c%3Dd%2525%26e"
        );
    }

    #[cfg(unix)]
    struct TempScript {
        dir: std::path::PathBuf,
        script_path: std::path::PathBuf,
        marker_path: std::path::PathBuf,
    }

    #[cfg(unix)]
    impl TempScript {
        fn new(body: &str) -> Self {
            use std::os::unix::fs::PermissionsExt;
            use std::sync::atomic::AtomicU64;

            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "opendal-s3-cmd-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let marker_path = dir.join("executed");
            let script_path = dir.join("cred.sh");
            let script = format!("#!/bin/sh\ntouch \"{}\"\n{}\n", marker_path.display(), body);
            std::fs::write(&script_path, script).unwrap();
            std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755)).unwrap();
            Self {
                dir,
                script_path,
                marker_path,
            }
        }
    }

    #[cfg(unix)]
    impl Drop for TempScript {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[cfg(unix)]
    #[derive(Debug, Clone)]
    struct MockConfigFileRead {
        config_path: String,
        contents: String,
    }

    #[cfg(unix)]
    impl reqsign_core::FileRead for MockConfigFileRead {
        async fn file_read(&self, path: &str) -> reqsign_core::Result<Vec<u8>> {
            if path == self.config_path {
                Ok(self.contents.as_bytes().to_vec())
            } else {
                Err(reqsign_core::Error::unexpected(format!(
                    "file not found: {path}"
                )))
            }
        }
    }

    #[cfg(unix)]
    #[derive(Debug, Clone, Default)]
    struct MockStsHttpSend {
        auth_headers: Arc<Mutex<Vec<String>>>,
    }

    #[cfg(unix)]
    impl reqsign_core::HttpSend for MockStsHttpSend {
        async fn http_send(
            &self,
            request: Request<Bytes>,
        ) -> reqsign_core::Result<Response<Bytes>> {
            let auth = request
                .headers()
                .get(http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            self.auth_headers.lock().unwrap().push(auth);
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Bytes::from_static(
                    br#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
                        <AssumeRoleResult>
                            <Credentials>
                                <AccessKeyId>ASIAEXAMPLEASSUMED01</AccessKeyId>
                                <SecretAccessKey>assumed-sk</SecretAccessKey>
                                <SessionToken>assumed-token</SessionToken>
                                <Expiration>2099-01-01T00:00:00Z</Expiration>
                            </Credentials>
                        </AssumeRoleResult>
                    </AssumeRoleResponse>"#,
                ))
                .unwrap())
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_process_credential_executes_via_wired_sign_ctx() {
        let script = TempScript::new(
            r#"printf '%s' '{"Version":1,"AccessKeyId":"proc-wired-ak","SecretAccessKey":"proc-wired-sk","SessionToken":"proc-wired-token"}'"#,
        );
        let backend = S3Builder::default()
            .bucket("example")
            .region("us-east-1")
            .endpoint("https://s3.us-east-1.amazonaws.com")
            .profile("process-wired")
            .disable_ec2_metadata()
            .build_inner(S3_SCHEME)
            .unwrap();

        let config_contents = format!(
            "[profile process-wired]\ncredential_process = \"{}\"\n",
            script.script_path.display()
        );
        let ctx = backend
            .core
            .signer_spec
            .sign_ctx
            .clone()
            .with_env(reqsign_core::StaticEnv {
                home_dir: None,
                envs: HashMap::from([(
                    "AWS_CONFIG_FILE".to_string(),
                    "/mock/aws/config".to_string(),
                )]),
            })
            .with_file_read(MockConfigFileRead {
                config_path: "/mock/aws/config".to_string(),
                contents: config_contents,
            });
        let (mut parts, ()) = Request::get("https://s3.us-east-1.amazonaws.com/example/test")
            .body(())
            .unwrap()
            .into_parts();

        backend
            .core
            .signer_spec
            .build(ctx)
            .default()
            .sign(&mut parts, None)
            .await
            .unwrap();

        let auth = parts
            .headers
            .get(http::header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth.contains("Credential=proc-wired-ak/"), "{auth}");
        assert_eq!(
            parts
                .headers
                .get("x-amz-security-token")
                .and_then(|v| v.to_str().ok()),
            Some("proc-wired-token")
        );
        assert!(script.marker_path.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_assume_role_sts_signer_inherits_wired_command_execute() {
        let script = TempScript::new(
            r#"printf '%s' '{"Version":1,"AccessKeyId":"proc-wired-ak","SecretAccessKey":"proc-wired-sk","SessionToken":"proc-wired-token"}'"#,
        );
        let backend = S3Builder::default()
            .bucket("example")
            .region("us-east-1")
            .endpoint("https://s3.us-east-1.amazonaws.com")
            .profile("process-wired")
            .role_arn("arn:aws:iam::123456789012:role/test")
            .disable_ec2_metadata()
            .build_inner(S3_SCHEME)
            .unwrap();

        let config_contents = format!(
            "[profile process-wired]\ncredential_process = \"{}\"\n",
            script.script_path.display()
        );
        let sts_http = MockStsHttpSend::default();
        let ctx = backend
            .core
            .signer_spec
            .sign_ctx
            .clone()
            .with_env(reqsign_core::StaticEnv {
                home_dir: None,
                envs: HashMap::from([(
                    "AWS_CONFIG_FILE".to_string(),
                    "/mock/aws/config".to_string(),
                )]),
            })
            .with_file_read(MockConfigFileRead {
                config_path: "/mock/aws/config".to_string(),
                contents: config_contents,
            })
            .with_http_send(sts_http.clone());
        let (mut parts, ()) = Request::get("https://s3.us-east-1.amazonaws.com/example/test")
            .body(())
            .unwrap()
            .into_parts();

        backend
            .core
            .signer_spec
            .build(ctx)
            .default()
            .sign(&mut parts, None)
            .await
            .unwrap();

        let sts_headers = sts_http.auth_headers.lock().unwrap();
        assert_eq!(sts_headers.len(), 1);
        assert!(
            sts_headers[0].contains("Credential=proc-wired-ak/"),
            "{}",
            sts_headers[0]
        );
        let auth = parts
            .headers
            .get(http::header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth.contains("Credential=ASIAEXAMPLEASSUMED01/"), "{auth}");
        assert_eq!(
            parts
                .headers
                .get("x-amz-security-token")
                .and_then(|v| v.to_str().ok()),
            Some("assumed-token")
        );
        assert!(script.marker_path.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_disable_config_load_does_not_execute_process_via_wired_sign_ctx() {
        let script = TempScript::new(
            r#"printf '%s' '{"Version":1,"AccessKeyId":"proc-wired-ak","SecretAccessKey":"proc-wired-sk","SessionToken":"proc-wired-token"}'"#,
        );
        let backend = S3Builder::default()
            .bucket("example")
            .region("us-east-1")
            .endpoint("https://s3.us-east-1.amazonaws.com")
            .disable_config_load()
            .disable_ec2_metadata()
            .build_inner(S3_SCHEME)
            .unwrap();

        let config_contents = format!(
            "[profile process-wired]\ncredential_process = \"{}\"\n",
            script.script_path.display()
        );
        let ctx = backend
            .core
            .signer_spec
            .sign_ctx
            .clone()
            .with_env(reqsign_core::StaticEnv {
                home_dir: None,
                envs: HashMap::from([
                    (
                        "AWS_CONFIG_FILE".to_string(),
                        "/mock/aws/config".to_string(),
                    ),
                    ("AWS_PROFILE".to_string(), "process-wired".to_string()),
                ]),
            })
            .with_file_read(MockConfigFileRead {
                config_path: "/mock/aws/config".to_string(),
                contents: config_contents,
            });
        let (mut parts, ()) = Request::get("https://s3.us-east-1.amazonaws.com/example/test")
            .body(())
            .unwrap()
            .into_parts();

        let err = backend
            .core
            .signer_spec
            .build(ctx)
            .default()
            .sign(&mut parts, None)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), reqsign_core::ErrorKind::CredentialInvalid);
        assert!(!script.marker_path.exists());
    }
}
