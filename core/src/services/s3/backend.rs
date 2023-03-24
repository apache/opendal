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
use std::fmt::Formatter;
use std::fmt::Write;
use std::sync::Arc;

use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Buf;
use bytes::Bytes;
use http::header::HeaderName;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::HeaderValue;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use log::error;
use log::warn;
use md5::Digest;
use md5::Md5;
use once_cell::sync::Lazy;
use reqsign::AwsConfigLoader;
use reqsign::AwsCredentialLoad;
use reqsign::AwsCredentialLoader;
use reqsign::AwsV4Signer;
use serde::Deserialize;
use serde::Serialize;

use super::error::parse_error;
use super::pager::S3Pager;
use super::writer::S3Writer;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Allow constructing correct region endpoint if user gives a global endpoint.
static ENDPOINT_TEMPLATES: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let mut m = HashMap::new();
    // AWS S3 Service.
    m.insert(
        "https://s3.amazonaws.com",
        "https://s3.{region}.amazonaws.com",
    );
    m
});

mod constants {
    pub const X_AMZ_SERVER_SIDE_ENCRYPTION: &str = "x-amz-server-side-encryption";
    pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: &str =
        "x-amz-server-side-encryption-customer-algorithm";
    pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: &str =
        "x-amz-server-side-encryption-customer-key";
    pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: &str =
        "x-amz-server-side-encryption-customer-key-md5";
    pub const X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID: &str =
        "x-amz-server-side-encryption-aws-kms-key-id";
    pub const X_AMZ_BUCKET_REGION: &str = "x-amz-bucket-region";

    pub const RESPONSE_CONTENT_DISPOSITION: &str = "response-content-disposition";
}

/// Aws S3 and compatible services (including minio, digitalocean space and so on) support
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] list
/// - [x] scan
/// - [x] presign
/// - [ ] blocking
///
/// # Configuration
///
/// - `root`: Set the work dir for backend.
/// - `bucket`: Set the container name for backend.
/// - `endpoint`: Set the endpoint for backend.
/// - `region`: Set the region for backend.
/// - `access_key_id`: Set the access_key_id for backend.
/// - `secret_access_key`: Set the secret_access_key for backend.
/// - `security_token`: Set the security_token for backend.
/// - `server_side_encryption`: Set the server_side_encryption for backend.
/// - `server_side_encryption_aws_kms_key_id`: Set the server_side_encryption_aws_kms_key_id for backend.
/// - `server_side_encryption_customer_algorithm`: Set the server_side_encryption_customer_algorithm for backend.
/// - `server_side_encryption_customer_key`: Set the server_side_encryption_customer_key for backend.
/// - `server_side_encryption_customer_key_md5`: Set the server_side_encryption_customer_key_md5 for backend.
/// - `disable_config_load`: Disable aws config load from env
/// - `enable_virtual_host_style`: Enable virtual host style.
///
/// Refer to [`S3Builder`]'s public API docs for more information.
///
/// # Temporary security credentials
///
/// OpenDAL now provides support for S3 temporary security credentials in IAM.
///
/// The way to take advantage of this feature is to build your S3 backend with `Builder::security_token`.
///
/// But OpenDAL will not refresh the temporary security credentials, please keep in mind to refresh those credentials in time.
///
/// # Server Side Encryption
///
/// OpenDAL provides full support of S3 Server Side Encryption(SSE) features.
///
/// The easiest way to configure them is to use helper functions like
///
/// - SSE-KMS: `server_side_encryption_with_aws_managed_kms_key`
/// - SSE-KMS: `server_side_encryption_with_customer_managed_kms_key`
/// - SSE-S3: `server_side_encryption_with_s3_key`
/// - SSE-C: `server_side_encryption_with_customer_key`
///
/// If those functions don't fulfill need, low-level options are also provided:
///
/// - Use service managed kms key
///   - `server_side_encryption="aws:kms"`
/// - Use customer provided kms key
///   - `server_side_encryption="aws:kms"`
///   - `server_side_encryption_aws_kms_key_id="your-kms-key"`
/// - Use S3 managed key
///   - `server_side_encryption="AES256"`
/// - Use customer key
///   - `server_side_encryption_customer_algorithm="AES256"`
///   - `server_side_encryption_customer_key="base64-of-your-aes256-key"`
///   - `server_side_encryption_customer_key_md5="base64-of-your-aes256-key-md5"`
///
/// After SSE have been configured, all requests send by this backed will attach those headers.
///
/// Reference: [Protecting data using server-side encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/serv-side-encryption.html)
///
/// # Example
///
/// ## Basic Setup
///
/// ```no_run
/// use std::sync::Arc;
///
/// use anyhow::Result;
/// use opendal::services::S3;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Create s3 backend builder.
///     let mut builder = S3::default();
///     // Set the root for s3, all operations will happen under this root.
///     //
///     // NOTE: the root must be absolute path.
///     builder.root("/path/to/dir");
///     // Set the bucket name, this is required.
///     builder.bucket("test");
///     // Set the endpoint.
///     //
///     // For examples:
///     // - "https://s3.amazonaws.com"
///     // - "http://127.0.0.1:9000"
///     // - "https://oss-ap-northeast-1.aliyuncs.com"
///     // - "https://cos.ap-seoul.myqcloud.com"
///     //
///     // Default to "https://s3.amazonaws.com"
///     builder.endpoint("https://s3.amazonaws.com");
///     // Set the access_key_id and secret_access_key.
///     //
///     // OpenDAL will try load credential from the env.
///     // If credential not set and no valid credential in env, OpenDAL will
///     // send request without signing like anonymous user.
///     builder.access_key_id("access_key_id");
///     builder.secret_access_key("secret_access_key");
///
///     let op: Operator = Operator::new(builder)?.finish();
///
///     Ok(())
/// }
/// ```
///
/// ## S3 with SSE-C
///
/// ```no_run
/// use anyhow::Result;
/// use log::info;
/// use opendal::services::S3;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = S3::default();
///
///     // Setup builders
///
///     // Enable SSE-C
///     builder.server_side_encryption_with_customer_key("AES256", "customer_key".as_bytes());
///
///     let op = Operator::new(builder)?.finish();
///     info!("operator: {:?}", op);
///
///     // Writing your testing code here.
///
///     Ok(())
/// }
/// ```
///
/// ## S3 with SSE-KMS and aws managed kms key
///
/// ```no_run
/// use anyhow::Result;
/// use log::info;
/// use opendal::services::S3;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = S3::default();
///
///     // Setup builders
///
///     // Enable SSE-KMS with aws managed kms key
///     builder.server_side_encryption_with_aws_managed_kms_key();
///
///     let op = Operator::new(builder)?.finish();
///     info!("operator: {:?}", op);
///
///     // Writing your testing code here.
///
///     Ok(())
/// }
/// ```
///
/// ## S3 with SSE-KMS and customer managed kms key
///
/// ```no_run
/// use anyhow::Result;
/// use log::info;
/// use opendal::services::S3;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = S3::default();
///
///     // Setup builders
///
///     // Enable SSE-KMS with customer managed kms key
///     builder.server_side_encryption_with_customer_managed_kms_key("aws_kms_key_id");
///
///     let op = Operator::new(builder)?.finish();
///     info!("operator: {:?}", op);
///
///     // Writing your testing code here.
///
///     Ok(())
/// }
/// ```
///
/// ## S3 with SSE-S3
///
/// ```no_run
/// use anyhow::Result;
/// use log::info;
/// use opendal::services::S3;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = S3::default();
///
///     // Setup builders
///
///     // Enable SSE-S3
///     builder.server_side_encryption_with_s3_key();
///
///     let op = Operator::new(builder)?.finish();
///     info!("operator: {:?}", op);
///
///     // Writing your testing code here.
///
///     Ok(())
/// }
/// ```
///
/// # Compatible Services
#[doc = include_str!("compatible_services.md")]
#[derive(Default, Clone)]
pub struct S3Builder {
    root: Option<String>,

    bucket: String,
    endpoint: Option<String>,
    region: Option<String>,
    role_arn: Option<String>,
    external_id: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    server_side_encryption: Option<String>,
    server_side_encryption_aws_kms_key_id: Option<String>,
    server_side_encryption_customer_algorithm: Option<String>,
    server_side_encryption_customer_key: Option<String>,
    server_side_encryption_customer_key_md5: Option<String>,

    /// temporary credentials, check the official [doc](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html) for detail
    security_token: Option<String>,

    disable_config_load: bool,
    enable_virtual_host_style: bool,

    http_client: Option<HttpClient>,
    customed_credential_load: Option<Arc<dyn AwsCredentialLoad>>,
}

impl Debug for S3Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Builder");

        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("role_arn", &self.role_arn)
            .field("external_id", &self.external_id)
            .field("disable_config_load", &self.disable_config_load)
            .field("enable_virtual_host_style", &self.enable_virtual_host_style);

        if self.access_key_id.is_some() {
            d.field("access_key_id", &"<redacted>");
        }
        if self.secret_access_key.is_some() {
            d.field("secret_access_key", &"<redacted>");
        }
        if self.server_side_encryption.is_some() {
            d.field("server_side_encryption", &"<redacted>");
        }
        if self.server_side_encryption_aws_kms_key_id.is_some() {
            d.field("server_side_encryption_aws_kms_key_id", &"<redacted>");
        }
        if self.server_side_encryption_customer_algorithm.is_some() {
            d.field("server_side_encryption_customer_algorithm", &"<redacted>");
        }
        if self.server_side_encryption_customer_key.is_some() {
            d.field("server_side_encryption_customer_key", &"<redacted>");
        }
        if self.server_side_encryption_customer_key_md5.is_some() {
            d.field("server_side_encryption_customer_key_md5", &"<redacted>");
        }

        if self.security_token.is_some() {
            d.field("security_token", &"<redacted>");
        }

        d.finish()
    }
}

impl S3Builder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set bucket name of this backend.
    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        self.bucket = bucket.to_string();

        self
    }

    /// Set endpoint of this backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - AWS S3: `https://s3.amazonaws.com` or `https://s3.{region}.amazonaws.com`
    /// - Aliyun OSS: `https://{region}.aliyuncs.com`
    /// - Tencent COS: `https://cos.{region}.myqcloud.com`
    /// - Minio: `http://127.0.0.1:9000`
    ///
    /// If user inputs endpoint without scheme like "s3.amazonaws.com", we
    /// will prepend "https://" before it.
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.endpoint = Some(endpoint.trim_end_matches('/').to_string())
        }

        self
    }

    /// Region represent the signing region of this endpoint.
    ///
    /// - If region is set, we will take user's input first.
    /// - If not, We will try to detect region via [RFC-0057: Auto Region](https://github.com/apache/incubator-opendal/blob/main/docs/rfcs/0057-auto-region.md).
    ///
    /// Most of time, region is not need to be set, especially for AWS S3 and minio.
    pub fn region(&mut self, region: &str) -> &mut Self {
        if !region.is_empty() {
            self.region = Some(region.to_string())
        }

        self
    }

    /// Set access_key_id of this backend.
    ///
    /// - If access_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn access_key_id(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.access_key_id = Some(v.to_string())
        }

        self
    }

    /// Set secret_access_key of this backend.
    ///
    /// - If secret_access_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn secret_access_key(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.secret_access_key = Some(v.to_string())
        }

        self
    }

    /// Set role_arn for this backend.
    pub fn role_arn(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.role_arn = Some(v.to_string())
        }

        self
    }

    /// Set external_id for this backend.
    pub fn external_id(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.external_id = Some(v.to_string())
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
    pub fn server_side_encryption(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.server_side_encryption = Some(v.to_string())
        }

        self
    }

    /// Set server_side_encryption_aws_kms_key_id for this backend
    ///
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
    /// is not set, S3 will use aws managed kms key to encrypt data.
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
    /// is a valid kms key id, S3 will use the provided kms key to encrypt data.
    /// - If the `server_side_encryption_aws_kms_key_id` is invalid or not found, an error will be
    /// returned.
    /// - If `server_side_encryption` is not `aws:kms`, setting `server_side_encryption_aws_kms_key_id`
    /// is a noop.
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn server_side_encryption_aws_kms_key_id(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.server_side_encryption_aws_kms_key_id = Some(v.to_string())
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
    pub fn server_side_encryption_customer_algorithm(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.server_side_encryption_customer_algorithm = Some(v.to_string())
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
    pub fn server_side_encryption_customer_key(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.server_side_encryption_customer_key = Some(v.to_string())
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
    pub fn server_side_encryption_customer_key_md5(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.server_side_encryption_customer_key_md5 = Some(v.to_string())
        }

        self
    }

    /// Enable server side encryption with aws managed kms key
    ///
    /// As known as: SSE-KMS
    ///
    /// NOTE: This function should not be used along with other `server_side_encryption_with_` functions.
    pub fn server_side_encryption_with_aws_managed_kms_key(&mut self) -> &mut Self {
        self.server_side_encryption = Some("aws:kms".to_string());
        self
    }

    /// Enable server side encryption with customer managed kms key
    ///
    /// As known as: SSE-KMS
    ///
    /// NOTE: This function should not be used along with other `server_side_encryption_with_` functions.
    pub fn server_side_encryption_with_customer_managed_kms_key(
        &mut self,
        aws_kms_key_id: &str,
    ) -> &mut Self {
        self.server_side_encryption = Some("aws:kms".to_string());
        self.server_side_encryption_aws_kms_key_id = Some(aws_kms_key_id.to_string());
        self
    }

    /// Enable server side encryption with s3 managed key
    ///
    /// As known as: SSE-S3
    ///
    /// NOTE: This function should not be used along with other `server_side_encryption_with_` functions.
    pub fn server_side_encryption_with_s3_key(&mut self) -> &mut Self {
        self.server_side_encryption = Some("AES256".to_string());
        self
    }

    /// Enable server side encryption with customer key.
    ///
    /// As known as: SSE-C
    ///
    /// NOTE: This function should not be used along with other `server_side_encryption_with_` functions.
    pub fn server_side_encryption_with_customer_key(
        &mut self,
        algorithm: &str,
        key: &[u8],
    ) -> &mut Self {
        self.server_side_encryption_customer_algorithm = Some(algorithm.to_string());
        self.server_side_encryption_customer_key = Some(BASE64_STANDARD.encode(key));
        self.server_side_encryption_customer_key_md5 =
            Some(BASE64_STANDARD.encode(Md5::digest(key).as_slice()));
        self
    }

    /// Set temporary credential used in AWS S3 connections
    ///
    /// # Warning
    ///
    /// security token's lifetime is short and requires users to refresh in time.
    pub fn security_token(&mut self, token: &str) -> &mut Self {
        if !token.is_empty() {
            self.security_token = Some(token.to_string());
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
    pub fn disable_config_load(&mut self) -> &mut Self {
        self.disable_config_load = true;
        self
    }

    /// Enable virtual host style so that opendal will send API requests
    /// in virtual host style instead of path style.
    ///
    /// - By default, opendal will send API to `https://s3.us-east-1.amazonaws.com/bucket_name`
    /// - Enabled, opendal will send API to `https://bucket_name.s3.us-east-1.amazonaws.com`
    pub fn enable_virtual_host_style(&mut self) -> &mut Self {
        self.enable_virtual_host_style = true;
        self
    }

    /// Adding a customed credential load for service.
    pub fn customed_credential_load(&mut self, cred: impl AwsCredentialLoad) -> &mut Self {
        self.customed_credential_load = Some(Arc::new(cred));
        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(&mut self, client: HttpClient) -> &mut Self {
        self.http_client = Some(client);
        self
    }

    /// Read RFC-0057: Auto Region for detailed behavior.
    ///
    /// - If region is already known, the region will be returned directly.
    /// - If region is not known, we will send API to `{endpoint}/{bucket}` to get
    ///   `x-amz-bucket-region`, use `us-east-1` if not found.
    ///
    /// Returning endpoint will trim bucket name:
    ///
    /// - `https://bucket_name.s3.amazonaws.com` => `https://s3.amazonaws.com`
    fn detect_region(&self, client: &HttpClient) -> Result<String> {
        debug_assert!(
            self.region.is_none(),
            "calling detect region with region already know is buggy"
        );

        // Builder's bucket must be valid.
        let bucket = self.bucket.as_str();

        let mut endpoint = match &self.endpoint {
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

        let url = format!("{endpoint}/{bucket}");
        debug!("backend detect region with url: {url}");

        let req = http::Request::head(&url).body(Body::Empty).map_err(|e| {
            error!("backend detect_region {}: {:?}", url, e);
            Error::new(ErrorKind::Unexpected, "build request for head")
                .with_context("service", Scheme::S3)
                .with_context("url", &url)
                .set_source(e)
        })?;

        let res = client.send(req)?;
        let (res, body) = res.into_parts();

        // Make sure the body has been consumed so that we can reuse
        // the connection later.
        let _ = body.consume();

        debug!(
            "auto detect region got response: status {:?}, header: {:?}",
            res.status, res.headers
        );
        match res.status {
            // The endpoint works, return with not changed endpoint and
            // default region.
            StatusCode::OK | StatusCode::FORBIDDEN => {
                let region = res
                    .headers
                    .get(constants::X_AMZ_BUCKET_REGION)
                    .unwrap_or(&HeaderValue::from_static("us-east-1"))
                    .to_str()
                    .map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "header value is not valid utf-8")
                            .set_source(e)
                    })?
                    .to_string();
                Ok(region)
            }
            // The endpoint should move, return with constructed endpoint
            StatusCode::MOVED_PERMANENTLY => {
                let region = res
                    .headers
                    .get(constants::X_AMZ_BUCKET_REGION)
                    .ok_or_else(|| Error::new(ErrorKind::Unexpected, "region is empty"))?
                    .to_str()
                    .map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "header value is not valid utf-8")
                            .set_source(e)
                    })?
                    .to_string();

                Ok(region)
            }
            // Unexpected status code, fallback to default region "us-east-1"
            code => Err(Error::new(
                ErrorKind::Unexpected,
                "can't detect region automatically, unexpected status code got",
            )
            .with_context("status", code.as_str())),
        }
    }

    /// Build endpoint with given region.
    fn build_endpoint(&self, region: &str) -> String {
        let bucket = {
            debug_assert!(!self.bucket.is_empty(), "bucket must be valid");

            self.bucket.as_str()
        };

        let mut endpoint = match &self.endpoint {
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

        // Update with endpoint templates.
        endpoint = if let Some(template) = ENDPOINT_TEMPLATES.get(endpoint.as_str()) {
            template.replace("{region}", region)
        } else {
            // If we don't know where about this endpoint, just leave
            // them as it.
            endpoint.to_string()
        };

        // Apply virtual host style.
        if self.enable_virtual_host_style {
            endpoint = endpoint.replace("//", &format!("//{bucket}."))
        } else {
            write!(endpoint, "/{bucket}").expect("write into string must succeed");
        };

        endpoint
    }
}

impl Builder for S3Builder {
    const SCHEME: Scheme = Scheme::S3;
    type Accessor = S3Backend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = S3Builder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("bucket").map(|v| builder.bucket(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("region").map(|v| builder.region(v));
        map.get("access_key_id").map(|v| builder.access_key_id(v));
        map.get("secret_access_key")
            .map(|v| builder.secret_access_key(v));
        map.get("security_token").map(|v| builder.security_token(v));
        map.get("role_arn").map(|v| builder.role_arn(v));
        map.get("external_id").map(|v| builder.external_id(v));
        map.get("server_side_encryption")
            .map(|v| builder.server_side_encryption(v));
        map.get("server_side_encryption_aws_kms_key_id")
            .map(|v| builder.server_side_encryption_aws_kms_key_id(v));
        map.get("server_side_encryption_customer_algorithm")
            .map(|v| builder.server_side_encryption_customer_algorithm(v));
        map.get("server_side_encryption_customer_key")
            .map(|v| builder.server_side_encryption_customer_key(v));
        map.get("server_side_encryption_customer_key_md5")
            .map(|v| builder.server_side_encryption_customer_key_md5(v));
        map.get("disable_config_load")
            .filter(|v| *v == "on" || *v == "true")
            .map(|_| builder.disable_config_load());
        map.get("enable_virtual_host_style")
            .filter(|v| *v == "on" || *v == "true")
            .map(|_| builder.enable_virtual_host_style());

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle bucket name.
        let bucket = match self.bucket.is_empty() {
            false => Ok(&self.bucket),
            true => Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", Scheme::S3),
            ),
        }?;
        debug!("backend use bucket {}", &bucket);

        let server_side_encryption = match &self.server_side_encryption {
            None => None,
            Some(v) => Some(v.parse().map_err(|e| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "server_side_encryption value is invalid",
                )
                .with_context("value", v)
                .set_source(e)
            })?),
        };

        let server_side_encryption_aws_kms_key_id =
            match &self.server_side_encryption_aws_kms_key_id {
                None => None,
                Some(v) => Some(v.parse().map_err(|e| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "server_side_encryption_aws_kms_key_id value is invalid",
                    )
                    .with_context("value", v)
                    .set_source(e)
                })?),
            };

        let server_side_encryption_customer_algorithm =
            match &self.server_side_encryption_customer_algorithm {
                None => None,
                Some(v) => Some(v.parse().map_err(|e| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "server_side_encryption_customer_algorithm value is invalid",
                    )
                    .with_context("value", v)
                    .set_source(e)
                })?),
            };

        let server_side_encryption_customer_key = match &self.server_side_encryption_customer_key {
            None => None,
            Some(v) => Some(v.parse().map_err(|e| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "server_side_encryption_customer_key value is invalid",
                )
                .with_context("value", v)
                .set_source(e)
            })?),
        };
        let server_side_encryption_customer_key_md5 =
            match &self.server_side_encryption_customer_key_md5 {
                None => None,
                Some(v) => Some(v.parse().map_err(|e| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "server_side_encryption_customer_key_md5 value is invalid",
                    )
                    .with_context("value", v)
                    .set_source(e)
                })?),
            };

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::S3)
            })?
        };

        let cfg = AwsConfigLoader::default();
        if !self.disable_config_load {
            cfg.load();
        }

        // Setting all value from user input if available.
        if let Some(region) = &self.region {
            cfg.set_region(region);
        }
        if let Some(v) = &self.access_key_id {
            cfg.set_access_key_id(v);
        }
        if let Some(v) = &self.secret_access_key {
            cfg.set_secret_access_key(v);
        }
        if let Some(v) = &self.security_token {
            cfg.set_session_token(v);
        }
        if let Some(v) = &self.role_arn {
            cfg.set_role_arn(v);
        }
        if let Some(v) = &self.external_id {
            cfg.set_external_id(v);
        }

        // Calculate region based on current cfg.
        let region = match cfg.region() {
            Some(region) => region,
            None => {
                // region is required to make signer work.
                //
                // If we don't know region after loading from builder and env.
                // We should try to detect them.
                let region = self
                    .detect_region(&client)
                    // If we met error during detect region, use "us-east-1"
                    // as default instead of returning error.
                    .unwrap_or_else(|err| {
                        warn!(
                            "backend detect region failed for {err:?}, using default region instead"
                        );
                        "us-east-1".to_string()
                    });
                cfg.set_region(&region);

                region
            }
        };
        debug!("backend use region: {region}");

        // Building endpoint.
        let endpoint = self.build_endpoint(&region);
        debug!("backend use endpoint: {endpoint}");

        let mut signer_builder = AwsV4Signer::builder();
        signer_builder.service("s3");
        signer_builder.allow_anonymous();
        signer_builder.config_loader(cfg.clone());
        signer_builder.credential_loader({
            let mut cred_loader = AwsCredentialLoader::new(cfg);
            cred_loader = cred_loader.with_allow_anonymous();
            cred_loader = cred_loader.with_client(client.sync_client());
            if self.disable_config_load {
                // If load config has been disable, we should also disable
                // ec2 metadata to avoid leaking permits.
                cred_loader = cred_loader.with_disable_ec2_metadata();
            }

            if let Some(ccl) = &self.customed_credential_load {
                cred_loader = cred_loader.with_customed_credential_loader(ccl.clone());
            }

            cred_loader
        });

        let signer = signer_builder
            .build()
            .map_err(|e| Error::new(ErrorKind::Unexpected, "build AwsV4Signer").set_source(e))?;

        debug!("backend build finished: {:?}", &self);
        Ok(S3Backend {
            root,
            endpoint,
            signer: Arc::new(signer),
            bucket: self.bucket.clone(),
            client,

            server_side_encryption,
            server_side_encryption_aws_kms_key_id,
            server_side_encryption_customer_algorithm,
            server_side_encryption_customer_key,
            server_side_encryption_customer_key_md5,
        })
    }
}

/// Backend for s3 services.
#[derive(Debug, Clone)]
pub struct S3Backend {
    bucket: String,
    endpoint: String,
    pub signer: Arc<AwsV4Signer>,
    pub client: HttpClient,
    // root will be "/" or "/abc/"
    root: String,

    server_side_encryption: Option<HeaderValue>,
    server_side_encryption_aws_kms_key_id: Option<HeaderValue>,
    server_side_encryption_customer_algorithm: Option<HeaderValue>,
    server_side_encryption_customer_key: Option<HeaderValue>,
    server_side_encryption_customer_key_md5: Option<HeaderValue>,
}

impl S3Backend {
    /// # Note
    ///
    /// header like X_AMZ_SERVER_SIDE_ENCRYPTION doesn't need to set while
    //  get or stat.
    pub(crate) fn insert_sse_headers(
        &self,
        mut req: http::request::Builder,
        is_write: bool,
    ) -> http::request::Builder {
        if is_write {
            if let Some(v) = &self.server_side_encryption {
                let mut v = v.clone();
                v.set_sensitive(true);

                req = req.header(
                    HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION),
                    v,
                )
            }
            if let Some(v) = &self.server_side_encryption_aws_kms_key_id {
                let mut v = v.clone();
                v.set_sensitive(true);

                req = req.header(
                    HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID),
                    v,
                )
            }
        }

        if let Some(v) = &self.server_side_encryption_customer_algorithm {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM),
                v,
            )
        }
        if let Some(v) = &self.server_side_encryption_customer_key {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY),
                v,
            )
        }
        if let Some(v) = &self.server_side_encryption_customer_key_md5 {
            let mut v = v.clone();
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5),
                v,
            )
        }

        req
    }
}

#[async_trait]
impl Accessor for S3Backend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = S3Writer;
    type BlockingWriter = ();
    type Pager = S3Pager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        use AccessorCapability::*;
        use AccessorHint::*;

        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::S3)
            .set_root(&self.root)
            .set_name(&self.bucket)
            .set_max_batch_operations(1000)
            .set_capabilities(Read | Write | List | Scan | Presign | Batch)
            .set_hints(ReadStreamable);

        am
    }

    async fn create(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let mut req = self.s3_put_object_request(path, Some(0), None, None, AsyncBody::Empty)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        let resp = self.client.send_async(req).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpCreate::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.s3_get_object(path, args.range()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let upload_id = if args.append() {
            let resp = self.s3_initiate_multipart_upload(path).await?;

            let status = resp.status();

            match status {
                StatusCode::OK => {
                    let bs = resp.into_body().bytes().await?;

                    let result: InitiateMultipartUploadResult =
                        quick_xml::de::from_reader(bs.reader())
                            .map_err(new_xml_deserialize_error)?;

                    Some(result.upload_id)
                }
                _ => return Err(parse_error(resp).await?),
            }
        } else {
            None
        };

        Ok((
            RpWrite::default(),
            S3Writer::new(self.clone(), args, path.to_string(), upload_id),
        ))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.s3_head_object(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_metadata(path, resp.headers()).map(RpStat::new),
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.s3_delete_object(path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        Ok((
            RpList::default(),
            S3Pager::new(Arc::new(self.clone()), &self.root, path, "/", args.limit()),
        ))
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        Ok((
            RpScan::default(),
            S3Pager::new(Arc::new(self.clone()), &self.root, path, "", args.limit()),
        ))
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        // We will not send this request out, just for signing.
        let mut req = match args.operation() {
            PresignOperation::Stat(_) => self.s3_head_object_request(path)?,
            PresignOperation::Read(v) => {
                self.s3_get_object_request(path, v.range(), v.override_content_disposition())?
            }
            PresignOperation::Write(_) => {
                self.s3_put_object_request(path, None, None, None, AsyncBody::Empty)?
            }
        };

        self.signer
            .sign_query(&mut req, args.expire())
            .map_err(new_request_sign_error)?;

        // We don't need this request anymore, consume it directly.
        let (parts, _) = req.into_parts();

        Ok(RpPresign::new(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        )))
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        let ops = args.into_operation();
        match ops {
            BatchOperations::Delete(ops) => {
                if ops.len() > 1000 {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "s3 services only allow delete up to 1000 keys at once",
                    )
                    .with_context("length", ops.len().to_string()));
                }

                let paths = ops.into_iter().map(|(p, _)| p).collect();

                let resp = self.s3_delete_objects(paths).await?;

                let status = resp.status();

                if let StatusCode::OK = status {
                    let bs = resp.into_body().bytes().await?;

                    let result: DeleteObjectsResult = quick_xml::de::from_reader(bs.reader())
                        .map_err(new_xml_deserialize_error)?;

                    let mut batched_result =
                        Vec::with_capacity(result.deleted.len() + result.error.len());
                    for i in result.deleted {
                        let path = build_rel_path(&self.root, &i.key);
                        batched_result.push((path, Ok(RpDelete::default())));
                    }
                    // TODO: we should handle those errors with code.
                    for i in result.error {
                        let path = build_rel_path(&self.root, &i.key);

                        batched_result.push((
                            path,
                            Err(Error::new(ErrorKind::Unexpected, &format!("{i:?}"))),
                        ));
                    }

                    Ok(RpBatch::new(BatchedResults::Delete(batched_result)))
                } else {
                    Err(parse_error(resp).await?)
                }
            }
        }
    }
}

impl S3Backend {
    fn s3_head_object_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::head(&url);

        req = self.insert_sse_headers(req, false);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    fn s3_get_object_request(
        &self,
        path: &str,
        range: BytesRange,
        override_content_disposition: Option<&str>,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        // Construct headers to add to the request
        let mut url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        if let Some(override_content_disposition) = override_content_disposition {
            url.push_str(&format!(
                "?{}={}",
                constants::RESPONSE_CONTENT_DISPOSITION,
                percent_encode_path(override_content_disposition)
            ));
        }

        let mut req = Request::get(&url);

        if !range.is_full() {
            req = req.header(http::header::RANGE, range.to_header());
        }

        // Set SSE headers.
        // TODO: how will this work with presign?
        req = self.insert_sse_headers(req, false);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    async fn s3_get_object(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.s3_get_object_request(path, range, None)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    pub fn s3_put_object_request(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        content_disposition: Option<&str>,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::put(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }

        if let Some(mime) = content_type {
            req = req.header(CONTENT_TYPE, mime)
        }

        if let Some(pos) = content_disposition {
            req = req.header(CONTENT_DISPOSITION, pos)
        }

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    async fn s3_head_object(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::head(&url);

        // Set SSE headers.
        req = self.insert_sse_headers(req, false);

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    async fn s3_delete_object(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::delete(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    /// Make this functions as `pub(suber)` because `DirStream` depends
    /// on this.
    pub(super) async fn s3_list_objects(
        &self,
        path: &str,
        continuation_token: &str,
        delimiter: &str,
        limit: Option<usize>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}?list-type=2&delimiter={delimiter}&prefix={}",
            self.endpoint,
            percent_encode_path(&p)
        );
        if let Some(limit) = limit {
            write!(url, "&max-keys={limit}").expect("write into string must succeed");
        }
        if !continuation_token.is_empty() {
            // AWS S3 could return continuation-token that contains `=`
            // which could lead `reqsign` parse query wrongly.
            // URL encode continuation-token before starting signing so that
            // our signer will not be confused.
            write!(
                url,
                "&continuation-token={}",
                percent_encode_path(continuation_token)
            )
            .expect("write into string must succeed");
        }

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    async fn s3_initiate_multipart_upload(
        &self,
        path: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}?uploads", self.endpoint, percent_encode_path(&p));

        let req = Request::post(&url);

        // Set SSE headers.
        let req = self.insert_sse_headers(req, true);

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    pub fn s3_upload_part_request(
        &self,
        path: &str,
        upload_id: &str,
        part_number: usize,
        size: Option<u64>,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}?partNumber={}&uploadId={}",
            self.endpoint,
            percent_encode_path(&p),
            part_number,
            percent_encode_path(upload_id)
        );

        let mut req = Request::put(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size);
        }

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn s3_complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: &[CompleteMultipartUploadRequestPart],
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}?uploadId={}",
            self.endpoint,
            percent_encode_path(&p),
            percent_encode_path(upload_id)
        );

        let req = Request::post(&url);

        // Set SSE headers.
        let req = self.insert_sse_headers(req, true);

        let content = quick_xml::se::to_string(&CompleteMultipartUploadRequest {
            part: parts.to_vec(),
        })
        .map_err(new_xml_deserialize_error)?;
        // Make sure content length has been set to avoid post with chunked encoding.
        let req = req.header(CONTENT_LENGTH, content.len());
        // Set content-type to `application/xml` to avoid mixed with form post.
        let req = req.header(CONTENT_TYPE, "application/xml");

        let mut req = req
            .body(AsyncBody::Bytes(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    async fn s3_delete_objects(&self, paths: Vec<String>) -> Result<Response<IncomingAsyncBody>> {
        let url = format!("{}/?delete", self.endpoint);

        let req = Request::post(&url);

        let content = quick_xml::se::to_string(&DeleteObjectsRequest {
            object: paths
                .into_iter()
                .map(|path| DeleteObjectsRequestObject {
                    key: build_abs_path(&self.root, &path),
                })
                .collect(),
        })
        .map_err(new_xml_deserialize_error)?;

        // Make sure content length has been set to avoid post with chunked encoding.
        let req = req.header(CONTENT_LENGTH, content.len());
        // Set content-type to `application/xml` to avoid mixed with form post.
        let req = req.header(CONTENT_TYPE, "application/xml");
        // Set content-md5 as required by API.
        let req = req.header("CONTENT-MD5", format_content_md5(content.as_bytes()));

        let mut req = req
            .body(AsyncBody::Bytes(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }
}

/// Result of CreateMultipartUpload
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct InitiateMultipartUploadResult {
    upload_id: String,
}

/// Request of CompleteMultipartUploadRequest
#[derive(Default, Debug, Serialize)]
#[serde(default, rename = "CompleteMultipartUpload", rename_all = "PascalCase")]
struct CompleteMultipartUploadRequest {
    part: Vec<CompleteMultipartUploadRequestPart>,
}

#[derive(Clone, Default, Debug, Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct CompleteMultipartUploadRequestPart {
    #[serde(rename = "PartNumber")]
    pub part_number: usize,
    /// # TODO
    ///
    /// quick-xml will do escape on `"` which leads to our serialized output is
    /// not the same as aws s3's example.
    ///
    /// Ideally, we could use `serialize_with` to address this (buf failed)
    ///
    /// ```ignore
    /// #[derive(Default, Debug, Serialize)]
    /// #[serde(default, rename_all = "PascalCase")]
    /// struct CompleteMultipartUploadRequestPart {
    ///     #[serde(rename = "PartNumber")]
    ///     part_number: usize,
    ///     #[serde(rename = "ETag", serialize_with = "partial_escape")]
    ///     etag: String,
    /// }
    ///
    /// fn partial_escape<S>(s: &str, ser: S) -> std::result::Result<S::Ok, S::Error>
    /// where
    ///     S: serde::Serializer,
    /// {
    ///     ser.serialize_str(&String::from_utf8_lossy(
    ///         &quick_xml::escape::partial_escape(s.as_bytes()),
    ///     ))
    /// }
    /// ```
    ///
    /// ref: <https://github.com/tafia/quick-xml/issues/362>
    #[serde(rename = "ETag")]
    pub etag: String,
}

/// Request of DeleteObjects.
#[derive(Default, Debug, Serialize)]
#[serde(default, rename = "Delete", rename_all = "PascalCase")]
struct DeleteObjectsRequest {
    object: Vec<DeleteObjectsRequestObject>,
}

#[derive(Default, Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteObjectsRequestObject {
    key: String,
}

/// Result of DeleteObjects.
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename = "DeleteResult", rename_all = "PascalCase")]
struct DeleteObjectsResult {
    deleted: Vec<DeleteObjectsResultDeleted>,
    error: Vec<DeleteObjectsResultError>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteObjectsResultDeleted {
    key: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct DeleteObjectsResultError {
    code: String,
    key: String,
    message: String,
}

#[cfg(test)]
mod tests {
    use backon::BlockingRetryable;
    use backon::ExponentialBuilder;
    use bytes::Buf;
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_region() {
        let _ = env_logger::try_init();

        let client = HttpClient::new().unwrap();

        let endpoint_cases = vec![
            Some("s3.amazonaws.com"),
            Some("https://s3.amazonaws.com"),
            Some("https://s3.us-east-2.amazonaws.com"),
            None,
        ];

        for endpoint in endpoint_cases {
            let mut b = S3Builder::default();
            b.bucket("test");
            if let Some(endpoint) = endpoint {
                b.endpoint(endpoint);
            }

            let region = { || b.detect_region(&client) }
                .retry(&ExponentialBuilder::default())
                .call()
                .expect("detect region must success");
            assert_eq!(region, "us-east-2");
        }
    }

    #[test]
    fn test_build_endpoint() {
        let _ = env_logger::try_init();

        let endpoint_cases = vec![
            Some("s3.amazonaws.com"),
            Some("https://s3.amazonaws.com"),
            Some("https://s3.us-east-2.amazonaws.com"),
            None,
        ];

        for endpoint in &endpoint_cases {
            let mut b = S3Builder::default();
            b.bucket("test");
            if let Some(endpoint) = endpoint {
                b.endpoint(endpoint);
            }

            let endpoint = b.build_endpoint("us-east-2");
            assert_eq!(endpoint, "https://s3.us-east-2.amazonaws.com/test");
        }

        for endpoint in &endpoint_cases {
            let mut b = S3Builder::default();
            b.bucket("test");
            b.enable_virtual_host_style();
            if let Some(endpoint) = endpoint {
                b.endpoint(endpoint);
            }

            let endpoint = b.build_endpoint("us-east-2");
            assert_eq!(endpoint, "https://test.s3.us-east-2.amazonaws.com");
        }
    }

    /// This example is from https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html#API_CreateMultipartUpload_Examples
    #[test]
    fn test_deserialize_initiate_multipart_upload_result() {
        let bs = Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Bucket>example-bucket</Bucket>
              <Key>example-object</Key>
              <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
            </InitiateMultipartUploadResult>"#,
        );

        let out: InitiateMultipartUploadResult =
            quick_xml::de::from_reader(bs.reader()).expect("must success");

        assert_eq!(
            out.upload_id,
            "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"
        )
    }

    /// This example is from https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html#API_CompleteMultipartUpload_Examples
    #[test]
    fn test_serialize_complete_multipart_upload_request() {
        let req = CompleteMultipartUploadRequest {
            part: vec![
                CompleteMultipartUploadRequestPart {
                    part_number: 1,
                    etag: "\"a54357aff0632cce46d942af68356b38\"".to_string(),
                },
                CompleteMultipartUploadRequestPart {
                    part_number: 2,
                    etag: "\"0c78aef83f66abc1fa1e8477f296d394\"".to_string(),
                },
                CompleteMultipartUploadRequestPart {
                    part_number: 3,
                    etag: "\"acbd18db4cc2f85cedef654fccc4a4d8\"".to_string(),
                },
            ],
        };

        let actual = quick_xml::se::to_string(&req).expect("must succeed");

        pretty_assertions::assert_eq!(
            actual,
            r#"<CompleteMultipartUpload>
             <Part>
                <PartNumber>1</PartNumber>
               <ETag>"a54357aff0632cce46d942af68356b38"</ETag>
             </Part>
             <Part>
                <PartNumber>2</PartNumber>
               <ETag>"0c78aef83f66abc1fa1e8477f296d394"</ETag>
             </Part>
             <Part>
               <PartNumber>3</PartNumber>
               <ETag>"acbd18db4cc2f85cedef654fccc4a4d8"</ETag>
             </Part>
            </CompleteMultipartUpload>"#
                // Cleanup space and new line
                .replace([' ', '\n'], "")
                // Escape `"` by hand to address <https://github.com/tafia/quick-xml/issues/362>
                .replace('"', "&quot;")
        )
    }

    /// This example is from https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html#API_DeleteObjects_Examples
    #[test]
    fn test_serialize_delete_objects_request() {
        let req = DeleteObjectsRequest {
            object: vec![
                DeleteObjectsRequestObject {
                    key: "sample1.txt".to_string(),
                },
                DeleteObjectsRequestObject {
                    key: "sample2.txt".to_string(),
                },
            ],
        };

        let actual = quick_xml::se::to_string(&req).expect("must succeed");

        pretty_assertions::assert_eq!(
            actual,
            r#"<Delete>
             <Object>
             <Key>sample1.txt</Key>
             </Object>
             <Object>
               <Key>sample2.txt</Key>
             </Object>
             </Delete>"#
                // Cleanup space and new line
                .replace([' ', '\n'], "")
        )
    }

    /// This example is from https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html#API_DeleteObjects_Examples
    #[test]
    fn test_deserialize_delete_objects_result() {
        let bs = Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
             <Deleted>
               <Key>sample1.txt</Key>
             </Deleted>
             <Error>
              <Key>sample2.txt</Key>
              <Code>AccessDenied</Code>
              <Message>Access Denied</Message>
             </Error>
            </DeleteResult>"#,
        );

        let out: DeleteObjectsResult =
            quick_xml::de::from_reader(bs.reader()).expect("must success");

        assert_eq!(out.deleted.len(), 1);
        assert_eq!(out.deleted[0].key, "sample1.txt");
        assert_eq!(out.error.len(), 1);
        assert_eq!(out.error[0].key, "sample2.txt");
        assert_eq!(out.error[0].code, "AccessDenied");
        assert_eq!(out.error[0].message, "Access Denied");
    }
}
