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
use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use constants::X_AMZ_META_PREFIX;
use constants::X_AMZ_VERSION_ID;
use http::Response;
use http::StatusCode;
use log::debug;
use log::warn;
use md5::Digest;
use md5::Md5;
use reqsign_aws_v4::AssumeRoleCredentialProvider;
use reqsign_aws_v4::Credential;
use reqsign_aws_v4::DefaultCredentialProvider;
use reqsign_aws_v4::RequestSigner as AwsV4Signer;
use reqsign_aws_v4::StaticCredentialProvider;
use reqsign_core::Context;
use reqsign_core::OsEnv;
use reqsign_core::ProvideCredentialChain;
use reqsign_core::Signer;
use reqsign_file_read_tokio::TokioFileRead;
use reqsign_http_send_reqwest::ReqwestHttpSend;
use reqwest::Url;

use super::S3_SCHEME;
use super::config::S3Config;
use super::core::*;
use super::deleter::S3Deleter;
use super::error::parse_error;
use super::lister::S3ListerV1;
use super::lister::S3ListerV2;
use super::lister::S3Listers;
use super::lister::S3ObjectVersionsLister;
use super::writer::S3Writer;
use super::writer::S3Writers;
use crate::raw::*;
use crate::*;

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

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    pub(super) http_client: Option<HttpClient>,
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

    /// Disable config load so that opendal will not load config from
    /// environment.
    ///
    /// For examples:
    ///
    /// - envs like `AWS_ACCESS_KEY_ID`
    /// - files like `~/.aws/config`
    pub fn disable_config_load(mut self) -> Self {
        self.config.disable_config_load = true;
        self
    }

    /// Disable list objects v2 so that opendal will not use the older
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

    /// Allow anonymous will allow opendal to send request without signing
    /// when credential is not loaded.
    pub fn allow_anonymous(mut self) -> Self {
        self.config.allow_anonymous = true;
        self
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

    /// Disable stat with override so that opendal will not send stat request with override queries.
    ///
    /// For example, R2 doesn't support stat with `response_content_type` query.
    pub fn disable_stat_with_override(mut self) -> Self {
        self.config.disable_stat_with_override = true;
        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    #[allow(deprecated)]
    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Set bucket versioning status for this backend
    pub fn enable_versioning(mut self, enabled: bool) -> Self {
        self.config.enable_versioning = enabled;

        self
    }

    /// Replace the credential providers with a custom chain.
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

    /// Build endpoint with given region.
    fn build_endpoint(config: &S3Config, region: &str) -> String {
        let bucket = {
            debug_assert!(Self::is_bucket_valid(config), "bucket must be valid");

            config.bucket.as_str()
        };

        let mut endpoint = match &config.endpoint {
            Some(endpoint) => {
                if endpoint.starts_with("http") {
                    endpoint.to_string()
                } else {
                    // Prefix https if endpoint doesn't start with scheme.
                    format!("https://{endpoint}")
                }
            }
            None => "https://s3.amazonaws.com".to_string(),
        };

        // If endpoint contains bucket name, we should trim them.
        endpoint = endpoint.replace(&format!("//{bucket}."), "//");

        // Omit default ports if specified.
        if let Ok(url) = Url::from_str(&endpoint) {
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

    /// Set maximum batch operations of this backend.
    #[deprecated(
        since = "0.52.0",
        note = "Please use `delete_max_size` instead of `batch_max_operations`"
    )]
    pub fn batch_max_operations(mut self, batch_max_operations: usize) -> Self {
        self.config.delete_max_size = Some(batch_max_operations);

        self
    }

    /// Set maximum delete operations of this backend.
    pub fn delete_max_size(mut self, delete_max_size: usize) -> Self {
        self.config.delete_max_size = Some(delete_max_size);

        self
    }

    /// Set checksum algorithm of this backend.
    /// This is necessary when writing to AWS S3 Buckets with Object Lock enabled for example.
    ///
    /// Available options:
    /// - "crc32c"
    pub fn checksum_algorithm(mut self, checksum_algorithm: &str) -> Self {
        self.config.checksum_algorithm = Some(checksum_algorithm.to_string());

        self
    }

    /// Disable write with if match so that opendal will not send write request with if match headers.
    pub fn disable_write_with_if_match(mut self) -> Self {
        self.config.disable_write_with_if_match = true;
        self
    }

    /// Enable write with append so that opendal will send write request with append headers.
    pub fn enable_write_with_append(mut self) -> Self {
        self.config.enable_write_with_append = true;
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
    /// - Send a `HEAD` request to endpoint with bucket name to get `x-amz-bucket-region`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use opendal::services::S3;
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
        let mut endpoint = if endpoint.starts_with("http") {
            endpoint.to_string()
        } else {
            // Prefix https if endpoint doesn't start with scheme.
            format!("https://{endpoint}")
        };

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
        if let Some(v) = endpoint.strip_prefix("https://s3.") {
            if let Some(region) = v.strip_suffix(".amazonaws.com") {
                return Some(region.to_string());
            }
        }

        // If this bucket is OSS, we can try to match the endpoint.
        //
        // - `oss-ap-southeast-1.aliyuncs.com` => `oss-ap-southeast-1`
        // - `oss-cn-hangzhou-internal.aliyuncs.com` => `oss-cn-hangzhou`
        if let Some(v) = endpoint.strip_prefix("https://") {
            if let Some(region) = v.strip_suffix(".aliyuncs.com") {
                return Some(region.to_string());
            }

            if let Some(region) = v.strip_suffix("-internal.aliyuncs.com") {
                return Some(region.to_string());
            }
        }

        // Try to detect region by HeadBucket.
        let req = http::Request::head(&url).body(Buffer::new()).ok()?;

        let client = HttpClient::new().ok()?;
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
        if let Some(header) = res.headers().get("x-amz-bucket-region") {
            if let Ok(regin) = header.to_str() {
                return Some(regin.to_string());
            }
        }

        // Status code is 403 or 200 means we already visit the correct
        // region, we can use the default region directly.
        if res.status() == StatusCode::FORBIDDEN || res.status() == StatusCode::OK {
            return Some("us-east-1".to_string());
        }

        None
    }
}

impl Builder for S3Builder {
    type Config = S3Config;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        #[allow(deprecated)]
        let S3Builder {
            mut config,
            http_client,
            credential_providers,
        } = self;

        let root = normalize_root(&config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle bucket name.
        let bucket = if Self::is_bucket_valid(&config) {
            Ok(&config.bucket)
        } else {
            Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", S3_SCHEME),
            )
        }?;
        debug!("backend use bucket {}", &bucket);

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
                .map_err(|_| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "region is missing. Please find it by S3::detect_region() or set them in env.",
                    )
                    .with_operation("Builder::build")
                    .with_context("service", S3_SCHEME)
                })?
        };
        debug!("backend use region: {region}");

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

        // Building endpoint.
        let endpoint = Self::build_endpoint(&config, &region);
        debug!("backend use endpoint: {endpoint}");

        // Create the context for reqsign-core
        let ctx = Context::new()
            .with_file_read(TokioFileRead)
            .with_http_send(ReqwestHttpSend::new(GLOBAL_REQWEST_CLIENT.clone()))
            .with_env(OsEnv);

        let mut provider = {
            let mut builder = DefaultCredentialProvider::builder();

            if config.disable_config_load {
                builder = builder.disable_env(true).disable_profile(true);
            }

            if config.disable_ec2_metadata {
                builder = builder.disable_imds(true);
            }

            ProvideCredentialChain::new().push(builder.build())
        };

        // Insert static key if user provided.
        if let (Some(ak), Some(sk)) = (&config.access_key_id, &config.secret_access_key) {
            let static_provider = if let Some(token) = config.session_token.as_deref() {
                StaticCredentialProvider::new(ak, sk).with_session_token(token)
            } else {
                StaticCredentialProvider::new(ak, sk)
            };
            provider = provider.push_front(static_provider);
        }

        // Insert assume role provider if user provided.
        if let Some(role_arn) = &config.role_arn {
            let sts_ctx = ctx.clone();
            let sts_request_signer = AwsV4Signer::new("sts", &region);
            let sts_signer = Signer::new(sts_ctx, provider, sts_request_signer);
            let mut assume_role_provider =
                AssumeRoleCredentialProvider::new(role_arn.clone(), sts_signer)
                    .with_region(region.clone())
                    .with_regional_sts_endpoint();

            if let Some(external_id) = &config.external_id {
                assume_role_provider = assume_role_provider.with_external_id(external_id.clone());
            }
            if let Some(role_session_name) = &config.role_session_name {
                assume_role_provider =
                    assume_role_provider.with_role_session_name(role_session_name.clone());
            }
            provider = ProvideCredentialChain::new().push(assume_role_provider);
        }

        // Replace provider if user provide their own.
        let provider = if let Some(credential_providers) = credential_providers {
            credential_providers
        } else {
            provider
        };

        // Create request signer for S3
        let request_signer = AwsV4Signer::new("s3", &region);

        // Create the signer
        let signer = Signer::new(ctx, provider, request_signer);

        let delete_max_size = config
            .delete_max_size
            .unwrap_or(DEFAULT_BATCH_MAX_OPERATIONS);

        Ok(S3Backend {
            core: Arc::new(S3Core {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(S3_SCHEME)
                        .set_root(&root)
                        .set_name(bucket)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_with_if_match: true,
                            stat_with_if_none_match: true,
                            stat_with_if_modified_since: true,
                            stat_with_if_unmodified_since: true,
                            stat_with_override_cache_control: !config.disable_stat_with_override,
                            stat_with_override_content_disposition: !config
                                .disable_stat_with_override,
                            stat_with_override_content_type: !config.disable_stat_with_override,
                            stat_with_version: config.enable_versioning,

                            read: true,
                            read_with_if_match: true,
                            read_with_if_none_match: true,
                            read_with_if_modified_since: true,
                            read_with_if_unmodified_since: true,
                            read_with_override_cache_control: true,
                            read_with_override_content_disposition: true,
                            read_with_override_content_type: true,
                            read_with_version: config.enable_versioning,

                            write: true,
                            write_can_empty: true,
                            write_can_multi: true,
                            write_can_append: config.enable_write_with_append,

                            write_with_cache_control: true,
                            write_with_content_type: true,
                            write_with_content_encoding: true,
                            write_with_if_match: !config.disable_write_with_if_match,
                            write_with_if_not_exists: true,
                            write_with_user_metadata: true,

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

                            delete: true,
                            delete_max_size: Some(delete_max_size),
                            delete_with_version: config.enable_versioning,

                            copy: true,

                            list: true,
                            list_with_limit: true,
                            list_with_start_after: true,
                            list_with_recursive: true,
                            list_with_versions: config.enable_versioning,
                            list_with_deleted: config.enable_versioning,

                            presign: true,
                            presign_stat: true,
                            presign_read: true,
                            presign_write: true,

                            shared: true,

                            ..Default::default()
                        });

                    // allow deprecated api here for compatibility
                    #[allow(deprecated)]
                    if let Some(client) = http_client {
                        am.update_http_client(|_| client);
                    }

                    am.into()
                },
                bucket: bucket.to_string(),
                endpoint,
                root,
                server_side_encryption,
                server_side_encryption_aws_kms_key_id,
                server_side_encryption_customer_algorithm,
                server_side_encryption_customer_key,
                server_side_encryption_customer_key_md5,
                default_storage_class,
                allow_anonymous: config.allow_anonymous,
                disable_list_objects_v2: config.disable_list_objects_v2,
                enable_request_payer: config.enable_request_payer,
                signer,
                checksum_algorithm,
            }),
        })
    }
}

/// Backend for s3 services.
#[derive(Debug, Clone)]
pub struct S3Backend {
    core: Arc<S3Core>,
}

impl Access for S3Backend {
    type Reader = HttpBody;
    type Writer = S3Writers;
    type Lister = S3Listers;
    type Deleter = oio::BatchDeleter<S3Deleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.s3_head_object(path, args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let headers = resp.headers();
                let mut meta = parse_into_metadata(path, headers)?;

                let user_meta = parse_prefixed_headers(headers, X_AMZ_META_PREFIX);
                if !user_meta.is_empty() {
                    meta = meta.with_user_metadata(user_meta);
                }

                if let Some(v) = parse_header_to_str(headers, X_AMZ_VERSION_ID)? {
                    meta.set_version(v);
                }

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.s3_get_object(path, args.range(), &args).await?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                Ok((RpRead::default(), resp.into_body()))
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = S3Writer::new(self.core.clone(), path, args.clone());

        let w = if args.append() {
            S3Writers::Two(oio::AppendWriter::new(writer))
        } else {
            S3Writers::One(oio::MultipartWriter::new(
                self.core.info.clone(),
                writer,
                args.concurrent(),
            ))
        };

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::BatchDeleter::new(S3Deleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = if args.versions() || args.deleted() {
            ThreeWays::Three(oio::PageLister::new(S3ObjectVersionsLister::new(
                self.core.clone(),
                path,
                args,
            )))
        } else if self.core.disable_list_objects_v2 {
            ThreeWays::One(oio::PageLister::new(S3ListerV1::new(
                self.core.clone(),
                path,
                args,
            )))
        } else {
            ThreeWays::Two(oio::PageLister::new(S3ListerV2::new(
                self.core.clone(),
                path,
                args,
            )))
        };

        Ok((RpList::default(), l))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.s3_copy_object(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCopy::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let (expire, op) = args.into_parts();
        // We will not send this request out, just for signing.
        let req = match op {
            PresignOperation::Stat(v) => self.core.s3_head_object_request(path, v),
            PresignOperation::Read(v) => {
                self.core
                    .s3_get_object_request(path, BytesRange::default(), &v)
            }
            PresignOperation::Write(_) => {
                self.core
                    .s3_put_object_request(path, None, &OpWrite::default(), Buffer::new())
            }
            PresignOperation::Delete(_) => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        };
        let req = req?;

        let req = self.core.sign_query(req, expire).await?;

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
    use super::*;

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
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

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
                Some("oss-cn-hangzhou-internal"),
            ),
            (
                "r2",
                "https://abc.xxxxx.r2.cloudflarestorage.com",
                "example",
                Some("auto"),
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
}
