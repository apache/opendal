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
use http::StatusCode;
use log::debug;
use md5::Digest;
use md5::Md5;
use once_cell::sync::Lazy;
use reqsign::AwsConfig;
use reqsign::AwsCredentialLoad;
use reqsign::AwsLoader;
use reqsign::AwsV4Signer;

use super::core::*;
use super::error::parse_error;
use super::pager::WasabiPager;
use super::writer::WasabiWriter;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Allow constructing correct region endpoint if user gives a global endpoint.
static ENDPOINT_TEMPLATES: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let mut m = HashMap::new();
    // AWS S3 Service.
    m.insert(
        "https://s3.wasabisys.com",
        "https://s3.{region}.wasabisys.com",
    );
    m
});

/// Wasabi (an aws S3 compatible service) support
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] copy
/// - [x] list
/// - [x] scan
/// - [x] presign
/// - [x] rename
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
/// - `default_storage_class`: Set the default storage_class for backend.
/// - `server_side_encryption`: Set the server_side_encryption for backend.
/// - `server_side_encryption_aws_kms_key_id`: Set the server_side_encryption_aws_kms_key_id for backend.
/// - `server_side_encryption_customer_algorithm`: Set the server_side_encryption_customer_algorithm for backend.
/// - `server_side_encryption_customer_key`: Set the server_side_encryption_customer_key for backend.
/// - `server_side_encryption_customer_key_md5`: Set the server_side_encryption_customer_key_md5 for backend.
/// - `disable_config_load`: Disable aws config load from env
/// - `enable_virtual_host_style`: Enable virtual host style.
///
/// Refer to [`WasabiBuilder`]'s public API docs for more information.
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
/// use opendal::services::Wasabi;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Create s3 backend builder.
///     let mut builder = Wasabi::default();
///     // Set the root for s3, all operations will happen under this root.
///     //
///     // NOTE: the root must be absolute path.
///     builder.root("/path/to/dir");
///     // Set the bucket name, this is required.
///     builder.bucket("test");
///     // Set the endpoint.
///     //
///     // For examples:
///     // - "https://s3.wasabisys.com"
///     // - "http://127.0.0.1:9000"
///     // - "https://oss-ap-northeast-1.aliyuncs.com"
///     // - "https://cos.ap-seoul.myqcloud.com"
///     //
///     // Default to "https://s3.wasabisys.com"
///     builder.endpoint("https://s3.wasabisys.com");
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
/// ## Wasabi with SSE-C
///
/// ```no_run
/// use anyhow::Result;
/// use log::info;
/// use opendal::services::Wasabi;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = Wasabi::default();
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
/// ## Wasabi with SSE-KMS and aws managed kms key
///
/// ```no_run
/// use anyhow::Result;
/// use log::info;
/// use opendal::services::Wasabi;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = Wasabi::default();
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
/// ## Wasabi with SSE-KMS and customer managed kms key
///
/// ```no_run
/// use anyhow::Result;
/// use log::info;
/// use opendal::services::Wasabi;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = Wasabi::default();
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
/// ## Wasabi with SSE-S3
///
/// ```no_run
/// use anyhow::Result;
/// use log::info;
/// use opendal::services::Wasabi;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = Wasabi::default();
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
#[derive(Default)]
pub struct WasabiBuilder {
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
    default_storage_class: Option<String>,

    /// temporary credentials, check the official [doc](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html) for detail
    security_token: Option<String>,

    disable_config_load: bool,
    disable_ec2_metadata: bool,
    enable_virtual_host_style: bool,

    http_client: Option<HttpClient>,
    customed_credential_load: Option<Box<dyn AwsCredentialLoad>>,
}

impl Debug for WasabiBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Builder");

        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region);

        d.finish_non_exhaustive()
    }
}

impl WasabiBuilder {
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
    /// - `https://s3.wasabisys.com` or `https://s3.{region}.wasabisys.com`
    ///
    /// If user inputs endpoint without scheme like "s3.wasabisys.com", we
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
    /// - If not, the default `us-east-1` will be used.
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

    /// Set default storage_class for this backend.
    /// Unlike S3, wasabi only supports one single storage class,
    /// which is most like standard S3 storage class,
    /// check `https://docs.wasabi.com/docs/operations-on-objects` for more details.
    ///
    /// Available values:
    /// - `STANDARD`
    pub fn default_storage_class(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.default_storage_class = Some(v.to_string())
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

    /// Set temporary credential used in service connections
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

    /// Disable load credential from ec2 metadata.
    ///
    /// This option is used to disable the default behavior of opendal
    /// to load credential from ec2 metadata, a.k.a, IMDSv2
    pub fn disable_ec2_metadata(&mut self) -> &mut Self {
        self.disable_ec2_metadata = true;
        self
    }

    /// Enable virtual host style so that opendal will send API requests
    /// in virtual host style instead of path style.
    ///
    /// - By default, opendal will send API to `https://s3.us-east-1.wasabisys.com/bucket_name`
    /// - Enabled, opendal will send API to `https://bucket_name.s3.us-east-1.wasabisys.com`
    pub fn enable_virtual_host_style(&mut self) -> &mut Self {
        self.enable_virtual_host_style = true;
        self
    }

    /// Adding a customed credential load for service.
    pub fn customed_credential_load(&mut self, cred: Box<dyn AwsCredentialLoad>) -> &mut Self {
        self.customed_credential_load = Some(cred);
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

    /// Check if `bucket` is valid
    /// `bucket` must be not empty and if `enable_virtual_host_style` is true
    /// it couldn't contain dot(.) character
    fn is_bucket_valid(&self) -> bool {
        if self.bucket.is_empty() {
            return false;
        }
        // If enable virtual host style, `bucket` will reside in domain part,
        // for example `https://bucket_name.s3.us-east-1.amazonaws.com`,
        // so `bucket` with dot can't be recognized correctly for this format.
        if self.enable_virtual_host_style && self.bucket.contains('.') {
            return false;
        }
        true
    }

    /// Build endpoint with given region.
    fn build_endpoint(&self, region: &str) -> String {
        let bucket = {
            debug_assert!(self.is_bucket_valid(), "bucket must be valid");

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
            None => "https://s3.wasabisys.com".to_string(),
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

impl Builder for WasabiBuilder {
    const SCHEME: Scheme = Scheme::Wasabi;
    type Accessor = WasabiBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = WasabiBuilder::default();

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
        map.get("disable_ec2_metadata")
            .filter(|v| *v == "on" || *v == "true")
            .map(|_| builder.disable_ec2_metadata());
        map.get("enable_virtual_host_style")
            .filter(|v| *v == "on" || *v == "true")
            .map(|_| builder.enable_virtual_host_style());
        map.get("default_storage_class")
            .map(|v| builder.default_storage_class(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle bucket name.
        let bucket = if self.is_bucket_valid() {
            Ok(&self.bucket)
        } else {
            Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", Scheme::S3),
            )
        }?;
        debug!("backend use bucket {}", &bucket);

        let default_storage_class = match &self.default_storage_class {
            None => None,
            Some(v) => Some(
                build_header_value(v).map_err(|err| err.with_context("key", "storage_class"))?,
            ),
        };

        let server_side_encryption = match &self.server_side_encryption {
            None => None,
            Some(v) => Some(
                build_header_value(v)
                    .map_err(|err| err.with_context("key", "server_side_encryption"))?,
            ),
        };

        let server_side_encryption_aws_kms_key_id =
            match &self.server_side_encryption_aws_kms_key_id {
                None => None,
                Some(v) => Some(build_header_value(v).map_err(|err| {
                    err.with_context("key", "server_side_encryption_aws_kms_key_id")
                })?),
            };

        let server_side_encryption_customer_algorithm =
            match &self.server_side_encryption_customer_algorithm {
                None => None,
                Some(v) => Some(build_header_value(v).map_err(|err| {
                    err.with_context("key", "server_side_encryption_customer_algorithm")
                })?),
            };

        let server_side_encryption_customer_key =
            match &self.server_side_encryption_customer_key {
                None => None,
                Some(v) => Some(build_header_value(v).map_err(|err| {
                    err.with_context("key", "server_side_encryption_customer_key")
                })?),
            };

        let server_side_encryption_customer_key_md5 =
            match &self.server_side_encryption_customer_key_md5 {
                None => None,
                Some(v) => Some(build_header_value(v).map_err(|err| {
                    err.with_context("key", "server_side_encryption_customer_key_md5")
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

        let mut cfg = AwsConfig::default();
        if !self.disable_config_load {
            cfg = cfg.from_profile();
            cfg = cfg.from_env();
        }

        // Setting all value from user input if available.
        if let Some(v) = self.region.take() {
            cfg.region = Some(v);
        }
        if let Some(v) = self.access_key_id.take() {
            cfg.access_key_id = Some(v)
        }
        if let Some(v) = self.secret_access_key.take() {
            cfg.secret_access_key = Some(v)
        }
        if let Some(v) = self.security_token.take() {
            cfg.session_token = Some(v)
        }
        if let Some(v) = self.role_arn.take() {
            cfg.role_arn = Some(v)
        }
        if let Some(v) = self.external_id.take() {
            cfg.external_id = Some(v)
        }

        if cfg.region.is_none() {
            // region is required to make signer work.
            //
            // If we don't know region after loading from builder and env.
            // We will use `us-east-1` as default.
            cfg.region = Some("us-east-1".to_string());
        }

        let region = cfg.region.to_owned().unwrap();
        debug!("backend use region: {region}");

        // Building endpoint.
        let endpoint = self.build_endpoint(&region);
        debug!("backend use endpoint: {endpoint}");

        let mut loader = AwsLoader::new(client.client(), cfg).with_allow_anonymous();
        if self.disable_ec2_metadata {
            loader = loader.with_disable_ec2_metadata();
        }
        if let Some(v) = self.customed_credential_load.take() {
            loader = loader.with_customed_credential_loader(v);
        }

        let signer = AwsV4Signer::new("s3", &region);

        debug!("backend build finished");
        Ok(WasabiBackend {
            core: Arc::new(WasabiCore {
                bucket: bucket.to_string(),
                endpoint,
                root,
                server_side_encryption,
                server_side_encryption_aws_kms_key_id,
                server_side_encryption_customer_algorithm,
                server_side_encryption_customer_key,
                server_side_encryption_customer_key_md5,
                default_storage_class,
                signer,
                loader,
                client,
            }),
        })
    }
}

/// Backend for wasabi service.
#[derive(Debug, Clone)]
pub struct WasabiBackend {
    core: Arc<WasabiCore>,
}

#[async_trait]
impl Accessor for WasabiBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = WasabiWriter;
    type BlockingWriter = ();
    type Pager = WasabiPager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Wasabi)
            .set_root(&self.core.root)
            .set_name(&self.core.bucket)
            .set_capability(Capability {
                read: true,
                read_can_next: true,
                write: true,
                list: true,
                scan: true,
                copy: true,
                presign: true,
                batch: true,
                rename: true,
                ..Default::default()
            });

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let mut req =
            self.core
                .put_object_request(path, Some(0), None, None, None, AsyncBody::Empty)?;

        self.core.sign(&mut req).await?;

        let resp = self.core.send(req).await?;

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
        let resp = self
            .core
            .get_object(path, args.range(), args.if_none_match())
            .await?;

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
        Ok((
            RpWrite::default(),
            WasabiWriter::new(self.core.clone(), args, path.to_string()),
        ))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.copy_object(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                // According to the documentation, when using copy_object, a 200 error may occur and we need to detect it.
                // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html#API_CopyObject_RequestSyntax
                resp.into_body().consume().await?;

                Ok(RpCopy::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.core.head_object(path, args.if_none_match()).await?;

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
        let resp = self.core.delete_object(path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        Ok((
            RpList::default(),
            WasabiPager::new(self.core.clone(), path, "/", args.limit()),
        ))
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        Ok((
            RpScan::default(),
            WasabiPager::new(self.core.clone(), path, "", args.limit()),
        ))
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        // We will not send this request out, just for signing.
        let mut req = match args.operation() {
            PresignOperation::Stat(v) => self.core.head_object_request(path, v.if_none_match())?,
            PresignOperation::Read(v) => self.core.get_object_request(
                path,
                v.range(),
                v.override_content_disposition(),
                v.override_cache_control(),
                v.if_none_match(),
            )?,
            PresignOperation::Write(_) => {
                self.core
                    .put_object_request(path, None, None, None, None, AsyncBody::Empty)?
            }
        };

        self.core.sign_query(&mut req, args.expire()).await?;

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
        if ops.len() > 1000 {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "s3 services only allow delete up to 1000 keys at once",
            )
            .with_context("length", ops.len().to_string()));
        }

        let paths = ops.into_iter().map(|(p, _)| p).collect();

        let resp = self.core.delete_objects(paths).await?;

        let status = resp.status();

        if let StatusCode::OK = status {
            let bs = resp.into_body().bytes().await?;

            let result: DeleteObjectsResult =
                quick_xml::de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

            let mut batched_result = Vec::with_capacity(result.deleted.len() + result.error.len());
            for i in result.deleted {
                let path = build_rel_path(&self.core.root, &i.key);
                batched_result.push((path, Ok(RpDelete::default().into())));
            }
            // TODO: we should handle those errors with code.
            for i in result.error {
                let path = build_rel_path(&self.core.root, &i.key);

                batched_result.push((
                    path,
                    Err(Error::new(ErrorKind::Unexpected, &format!("{i:?}"))),
                ));
            }

            Ok(RpBatch::new(batched_result))
        } else {
            Err(parse_error(resp).await?)
        }
    }

    /// Execute rename API call
    /// Wasabi will auto-create missing path for destination `to` if any
    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let resp = self.core.rename_object(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpRename::default()),
            _ => Err(parse_error(resp).await?),
        }
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
            let mut b = WasabiBuilder::default();
            b.bucket(bucket);
            if enable_virtual_host_style {
                b.enable_virtual_host_style();
            }
            assert_eq!(b.is_bucket_valid(), expected)
        }
    }

    #[test]
    fn test_build_endpoint() {
        let _ = env_logger::try_init();

        let endpoint_cases = vec![
            Some("s3.wasabisys.com"),
            Some("https://s3.wasabisys.com"),
            Some("https://s3.us-east-2.amazonaws.com"),
            None,
        ];

        for endpoint in &endpoint_cases {
            let mut b = WasabiBuilder::default();
            b.bucket("test");
            if let Some(endpoint) = endpoint {
                b.endpoint(endpoint);
            }

            let endpoint = b.build_endpoint("us-east-2");
            assert_eq!(endpoint, "https://s3.us-east-2.wasabisys.com/test");
        }

        for endpoint in &endpoint_cases {
            let mut b = WasabiBuilder::default();
            b.bucket("test");
            b.enable_virtual_host_style();
            if let Some(endpoint) = endpoint {
                b.endpoint(endpoint);
            }

            let endpoint = b.build_endpoint("us-east-2");
            assert_eq!(endpoint, "https://test.s3.us-east-2.wasabisys.com");
        }
    }
}
