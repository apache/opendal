// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use http::header::HeaderName;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::HeaderValue;
use http::StatusCode;
use isahc::AsyncBody;
use isahc::AsyncReadResponseExt;
use log::debug;
use log::error;
use log::info;
use once_cell::sync::Lazy;
use reqsign::services::aws::loader::CredentialLoadChain;
use reqsign::services::aws::loader::DummyLoader;
use reqsign::services::aws::v4::Signer;
use serde::Deserialize;
use serde::Serialize;

use super::dir_stream::DirStream;
use super::error::parse_error;
use crate::accessor::AccessorCapability;
use crate::error::other;
use crate::error::BackendError;
use crate::error::ObjectError;
use crate::http_util::new_request_build_error;
use crate::http_util::new_request_send_error;
use crate::http_util::new_request_sign_error;
use crate::http_util::new_response_consume_error;
use crate::http_util::parse_content_length;
use crate::http_util::parse_error_response;
use crate::http_util::parse_etag;
use crate::http_util::parse_last_modified;
use crate::http_util::percent_encode_path;
use crate::http_util::HttpClient;
use crate::io_util::unshared_reader;
use crate::multipart::ObjectPart;
use crate::ops::BytesRange;
use crate::ops::OpAbortMultipart;
use crate::ops::OpCompleteMultipart;
use crate::ops::OpCreate;
use crate::ops::OpCreateMultipart;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpPresign;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::OpWriteMultipart;
use crate::ops::Operation;
use crate::ops::PresignedRequest;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BytesReader;
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::Scheme;

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
}

/// Builder for s3 services
#[derive(Default, Clone)]
pub struct Builder {
    root: Option<String>,

    bucket: String,
    endpoint: Option<String>,
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    server_side_encryption: Option<String>,
    server_side_encryption_aws_kms_key_id: Option<String>,
    server_side_encryption_customer_algorithm: Option<String>,
    server_side_encryption_customer_key: Option<String>,
    server_side_encryption_customer_key_md5: Option<String>,

    disable_credential_loader: bool,
    enable_virtual_host_style: bool,
}

impl Debug for Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Builder");

        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("disable_credential_loader", &self.disable_credential_loader)
            .field("enable_virtual_host_style", &self.disable_credential_loader);

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

        d.finish()
    }
}

impl Builder {
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
    /// - If not, We will try to detect region via [RFC-0057: Auto Region](https://github.com/datafuselabs/opendal/blob/main/docs/rfcs/0057-auto-region.md).
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
        self.server_side_encryption_customer_key = Some(base64::encode(key));
        self.server_side_encryption_customer_key_md5 =
            Some(base64::encode(md5::compute(key).as_slice()));
        self
    }

    /// Disable credential loader so that opendal will not load credentials
    /// from environment.
    pub fn disable_credential_loader(&mut self) -> &mut Self {
        self.disable_credential_loader = true;
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

    /// Read RFC-0057: Auto Region for detailed behavior.
    ///
    /// - If region is already known, the region will be returned directly.
    /// - If region is not known, we will send API to `{endpoint}/{bucket}` to get
    ///   `x-amz-bucket-region`, use `us-east-1` if not found.
    ///
    /// Returning endpoint will trim bucket name:
    ///
    /// - `https://bucket_name.s3.amazonaws.com` => `https://s3.amazonaws.com`
    fn detect_region(
        &self,
        client: &HttpClient,
        bucket: &str,
        context: &HashMap<String, String>,
    ) -> Result<(String, String)> {
        let mut endpoint = match &self.endpoint {
            Some(endpoint) => {
                if endpoint.starts_with("http") {
                    endpoint.to_string()
                } else {
                    // Prefix https if endpoint doesn't start with scheme.
                    format!("https://{}", endpoint)
                }
            }
            None => "https://s3.amazonaws.com".to_string(),
        };

        // If endpoint contains bucket name, we should trim them.
        endpoint = endpoint.replace(&format!("//{bucket}."), "//");

        if let Some(region) = &self.region {
            let endpoint = if let Some(template) = ENDPOINT_TEMPLATES.get(endpoint.as_str()) {
                template.replace("{region}", region)
            } else {
                endpoint.to_string()
            };

            return Ok((endpoint, region.to_string()));
        }

        let url = format!("{endpoint}/{bucket}");
        debug!("backend detect region with url: {url}");

        let req = http::Request::head(&url)
            .body(isahc::Body::empty())
            .map_err(|e| {
                error!("backend detect_region {}: {:?}", url, e);
                other(BackendError::new(
                    context.clone(),
                    anyhow!("build request {}: {:?}", url, e),
                ))
            })?;

        let res = client.send(req).map_err(|e| {
            error!("backend detect_region: {}: {:?}", url, e);
            other(BackendError::new(
                context.clone(),
                anyhow!("sending request: {}: {:?}", url, e),
            ))
        })?;

        debug!(
            "auto detect region got response: status {:?}, header: {:?}",
            res.status(),
            res.headers()
        );
        match res.status() {
            // The endpoint works, return with not changed endpoint and
            // default region.
            StatusCode::OK | StatusCode::FORBIDDEN => {
                let region = res
                    .headers()
                    .get("x-amz-bucket-region")
                    .unwrap_or(&HeaderValue::from_static("us-east-1"))
                    .to_str()
                    .map_err(|e| other(BackendError::new(context.clone(), e)))?
                    .to_string();
                Ok((endpoint, region))
            }
            // The endpoint should move, return with constructed endpoint
            StatusCode::MOVED_PERMANENTLY => {
                let region = res
                    .headers()
                    .get("x-amz-bucket-region")
                    .ok_or_else(|| {
                        other(BackendError::new(
                            context.clone(),
                            anyhow!("can't detect region automatically, region is empty"),
                        ))
                    })?
                    .to_str()
                    .map_err(|e| other(BackendError::new(context.clone(), e)))?
                    .to_string();
                let template = ENDPOINT_TEMPLATES.get(endpoint.as_str()).ok_or_else(|| {
                    other(BackendError::new(
                        context.clone(),
                        anyhow!(
                            "can't detect region automatically, no valid endpoint template for {endpoint}",
                        ),
                    ))
                })?;

                let endpoint = template.replace("{region}", &region);

                Ok((endpoint, region))
            }
            // Unexpected status code
            code => Err(other(BackendError::new(
                context.clone(),
                anyhow!(
                    "can't detect region automatically, unexpected response: status code {code}",
                ),
            ))),
        }
    }

    /// Finish the build process and create a new accessor.
    pub fn build(&mut self) -> Result<Backend> {
        info!("backend build started: {:?}", &self);

        let root = match &self.root {
            // Use "/" as root if user not specified.
            None => "/".to_string(),
            Some(v) => {
                let mut v = v
                    .split('/')
                    .filter(|v| !v.is_empty())
                    .collect::<Vec<&str>>()
                    .join("/");
                if !v.starts_with('/') {
                    v.insert(0, '/');
                }
                if !v.ends_with('/') {
                    v.push('/')
                }
                v
            }
        };
        info!("backend use root {}", &root);

        // Handle endpoint, region and bucket name.
        let bucket = match self.bucket.is_empty() {
            false => Ok(&self.bucket),
            true => Err(other(BackendError::new(
                HashMap::from([("bucket".to_string(), "".to_string())]),
                anyhow!("bucket is empty"),
            ))),
        }?;
        debug!("backend use bucket {}", &bucket);

        // Setup error context so that we don't need to construct many times.
        let mut context: HashMap<String, String> =
            HashMap::from([("bucket".to_string(), bucket.to_string())]);

        let server_side_encryption = match &self.server_side_encryption {
            None => None,
            Some(v) => Some(v.parse().map_err(|e| {
                other(BackendError::new(
                    context.clone(),
                    anyhow!("server_side_encryption value {} invalid: {}", v, e),
                ))
            })?),
        };

        let server_side_encryption_aws_kms_key_id =
            match &self.server_side_encryption_aws_kms_key_id {
                None => None,
                Some(v) => Some(v.parse().map_err(|e| {
                    other(BackendError::new(
                        context.clone(),
                        anyhow!(
                            "server_side_encryption_aws_kms_key_id value {} invalid: {}",
                            v,
                            e
                        ),
                    ))
                })?),
            };

        let server_side_encryption_customer_algorithm =
            match &self.server_side_encryption_customer_algorithm {
                None => None,
                Some(v) => Some(v.parse().map_err(|e| {
                    other(BackendError::new(
                        context.clone(),
                        anyhow!(
                            "server_side_encryption_customer_algorithm value {} invalid: {}",
                            v,
                            e
                        ),
                    ))
                })?),
            };

        let server_side_encryption_customer_key = match &self.server_side_encryption_customer_key {
            None => None,
            Some(v) => Some(v.parse().map_err(|e| {
                other(BackendError::new(
                    context.clone(),
                    anyhow!(
                        "server_side_encryption_customer_key value {} invalid: {}",
                        v,
                        e
                    ),
                ))
            })?),
        };
        let server_side_encryption_customer_key_md5 =
            match &self.server_side_encryption_customer_key_md5 {
                None => None,
                Some(v) => Some(v.parse().map_err(|e| {
                    other(BackendError::new(
                        context.clone(),
                        anyhow!(
                            "server_side_encryption_customer_key_md5 value {} invalid: {}",
                            v,
                            e
                        ),
                    ))
                })?),
            };

        let client = HttpClient::new();

        let (mut endpoint, region) = self.detect_region(&client, bucket, &context)?;
        // Construct endpoint which contains bucket name.
        if self.enable_virtual_host_style {
            endpoint = endpoint.replace("//", &format!("//{bucket}."))
        } else {
            write!(endpoint, "/{bucket}").expect("write into string must succeed");
        }
        context.insert("endpoint".to_string(), endpoint.clone());
        context.insert("region".to_string(), region.clone());
        debug!("backend use endpoint: {}, region: {}", &endpoint, &region);

        let mut signer_builder = Signer::builder();
        signer_builder.service("s3");
        signer_builder.region(&region);
        signer_builder.allow_anonymous();
        if self.disable_credential_loader {
            signer_builder.credential_loader({
                let mut chain = CredentialLoadChain::default();
                chain.push(DummyLoader {});

                chain
            });
        }

        if let (Some(ak), Some(sk)) = (&self.access_key_id, &self.secret_access_key) {
            signer_builder.access_key(ak);
            signer_builder.secret_key(sk);
        }

        let signer = signer_builder
            .build()
            .map_err(|e| other(BackendError::new(context, e)))?;

        info!("backend build finished: {:?}", &self);
        Ok(Backend {
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

    /// Finish the build process and create a new accessor.
    #[deprecated = "Use Builder::build() instead"]
    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        info!("backend build started: {:?}", &self);

        let root = match &self.root {
            // Use "/" as root if user not specified.
            None => "/".to_string(),
            Some(v) => {
                let mut v = v
                    .split('/')
                    .filter(|v| !v.is_empty())
                    .collect::<Vec<&str>>()
                    .join("/");
                if !v.starts_with('/') {
                    v.insert(0, '/');
                }
                if !v.ends_with('/') {
                    v.push('/')
                }
                v
            }
        };
        info!("backend use root {}", &root);

        // Handle endpoint, region and bucket name.
        let bucket = match self.bucket.is_empty() {
            false => Ok(&self.bucket),
            true => Err(other(BackendError::new(
                HashMap::from([("bucket".to_string(), "".to_string())]),
                anyhow!("bucket is empty"),
            ))),
        }?;
        debug!("backend use bucket {}", &bucket);

        // Setup error context so that we don't need to construct many times.
        let mut context: HashMap<String, String> =
            HashMap::from([("bucket".to_string(), bucket.to_string())]);

        let server_side_encryption = match &self.server_side_encryption {
            None => None,
            Some(v) => Some(v.parse().map_err(|e| {
                other(BackendError::new(
                    context.clone(),
                    anyhow!("server_side_encryption value {} invalid: {}", v, e),
                ))
            })?),
        };

        let server_side_encryption_aws_kms_key_id =
            match &self.server_side_encryption_aws_kms_key_id {
                None => None,
                Some(v) => Some(v.parse().map_err(|e| {
                    other(BackendError::new(
                        context.clone(),
                        anyhow!(
                            "server_side_encryption_aws_kms_key_id value {} invalid: {}",
                            v,
                            e
                        ),
                    ))
                })?),
            };

        let server_side_encryption_customer_algorithm =
            match &self.server_side_encryption_customer_algorithm {
                None => None,
                Some(v) => Some(v.parse().map_err(|e| {
                    other(BackendError::new(
                        context.clone(),
                        anyhow!(
                            "server_side_encryption_customer_algorithm value {} invalid: {}",
                            v,
                            e
                        ),
                    ))
                })?),
            };

        let server_side_encryption_customer_key = match &self.server_side_encryption_customer_key {
            None => None,
            Some(v) => Some(v.parse().map_err(|e| {
                other(BackendError::new(
                    context.clone(),
                    anyhow!(
                        "server_side_encryption_customer_key value {} invalid: {}",
                        v,
                        e
                    ),
                ))
            })?),
        };
        let server_side_encryption_customer_key_md5 =
            match &self.server_side_encryption_customer_key_md5 {
                None => None,
                Some(v) => Some(v.parse().map_err(|e| {
                    other(BackendError::new(
                        context.clone(),
                        anyhow!(
                            "server_side_encryption_customer_key_md5 value {} invalid: {}",
                            v,
                            e
                        ),
                    ))
                })?),
            };

        let client = HttpClient::new();

        let (mut endpoint, region) = self.detect_region(&client, bucket, &context)?;
        // Construct endpoint which contains bucket name.
        if self.enable_virtual_host_style {
            endpoint = endpoint.replace("//", &format!("//{bucket}."))
        } else {
            write!(endpoint, "/{bucket}").expect("write into string must succeed");
        }
        context.insert("endpoint".to_string(), endpoint.clone());
        context.insert("region".to_string(), region.clone());
        debug!("backend use endpoint: {}, region: {}", &endpoint, &region);

        let mut signer_builder = Signer::builder();
        signer_builder.service("s3");
        signer_builder.region(&region);
        signer_builder.allow_anonymous();
        if self.disable_credential_loader {
            signer_builder.credential_loader({
                let mut chain = CredentialLoadChain::default();
                chain.push(DummyLoader {});

                chain
            });
        }

        if let (Some(ak), Some(sk)) = (&self.access_key_id, &self.secret_access_key) {
            signer_builder.access_key(ak);
            signer_builder.secret_key(sk);
        }

        let signer = signer_builder
            .build()
            .map_err(|e| other(BackendError::new(context, e)))?;

        info!("backend build finished: {:?}", &self);
        Ok(Arc::new(Backend {
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
        }))
    }
}

/// Backend for s3 services.
#[derive(Debug, Clone)]
pub struct Backend {
    bucket: String,
    endpoint: String,
    signer: Arc<Signer>,
    client: HttpClient,
    // root will be "/" or "/abc/"
    root: String,

    server_side_encryption: Option<HeaderValue>,
    server_side_encryption_aws_kms_key_id: Option<HeaderValue>,
    server_side_encryption_customer_algorithm: Option<HeaderValue>,
    server_side_encryption_customer_key: Option<HeaderValue>,
    server_side_encryption_customer_key_md5: Option<HeaderValue>,
}

impl Backend {
    /// Create a new builder for s3.
    #[deprecated = "Use Builder::default() instead"]
    pub fn build() -> Builder {
        Builder::default()
    }

    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Result<Self> {
        let mut builder = Builder::default();

        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                "bucket" => builder.bucket(v),
                "endpoint" => builder.endpoint(v),
                "region" => builder.region(v),
                "access_key_id" => builder.access_key_id(v),
                "secret_access_key" => builder.secret_access_key(v),
                "server_side_encryption" => builder.server_side_encryption(v),
                "server_side_encryption_aws_kms_key_id" => {
                    builder.server_side_encryption_aws_kms_key_id(v)
                }
                "server_side_encryption_customer_algorithm" => {
                    builder.server_side_encryption_customer_algorithm(v)
                }
                "server_side_encryption_customer_key" => {
                    builder.server_side_encryption_customer_key(v)
                }
                "server_side_encryption_customer_key_md5" => {
                    builder.server_side_encryption_customer_key_md5(v)
                }
                "disable_credential_loader" if !v.is_empty() => builder.disable_credential_loader(),
                "enable_virtual_host_style" if !v.is_empty() => builder.enable_virtual_host_style(),
                _ => continue,
            };
        }

        builder.build()
    }

    /// get_abs_path will return the absolute path of the given path in the s3 format.
    ///
    /// Read [RFC-112](https://github.com/datafuselabs/opendal/pull/112) for more details.
    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        if path == "/" {
            return self.root.trim_start_matches('/').to_string();
        }
        // root must be normalized like `/abc/`
        format!("{}{}", self.root, path)
            .trim_start_matches('/')
            .to_string()
    }

    /// get_rel_path will return the relative path of the given path in the s3 format.
    pub(crate) fn get_rel_path(&self, path: &str) -> String {
        let path = format!("/{}", path);

        match path.strip_prefix(&self.root) {
            Some(v) => v.to_string(),
            None => unreachable!(
                "invalid path {} that not start with backend root {}",
                &path, &self.root
            ),
        }
    }

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
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::S3)
            .set_root(&self.root)
            .set_name(&self.bucket)
            .set_capabilities(AccessorCapability::Presign | AccessorCapability::Multipart);

        am
    }

    async fn create(&self, args: &OpCreate) -> Result<()> {
        let p = self.get_abs_path(args.path());

        let mut req = self.put_object_request(&p, AsyncBody::from_bytes_static(""))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error("create", &p, e))?;

        let mut resp = self
            .client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error("create", &p, e))?;

        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => {
                resp.consume()
                    .await
                    .map_err(|err| new_response_consume_error("create", &p, err))?;
                Ok(())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error("create", args.path(), er);
                Err(err)
            }
        }
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let p = self.get_abs_path(args.path());

        let resp = self.get_object(&p, args.offset(), args.size()).await?;

        match resp.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(Box::new(resp.into_body())),
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error("read", args.path(), er);
                Err(err)
            }
        }
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let p = self.get_abs_path(args.path());

        let mut req = self.put_object_request(
            &p,
            AsyncBody::from_reader_sized(unshared_reader(r), args.size()),
        )?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error("write", &p, e))?;

        let mut resp = self
            .client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error("write", &p, e))?;

        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => {
                resp.consume()
                    .await
                    .map_err(|err| new_response_consume_error("write", &p, err))?;
                Ok(args.size())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error("write", args.path(), er);
                Err(err)
            }
        }
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let p = self.get_abs_path(args.path());

        // Stat root always returns a DIR.
        if self.get_rel_path(&p).is_empty() {
            let mut m = ObjectMetadata::default();
            m.set_mode(ObjectMode::DIR);

            return Ok(m);
        }

        let resp = self.head_object(&p).await?;

        match resp.status() {
            StatusCode::OK => {
                let mut m = ObjectMetadata::default();

                if let Some(v) = parse_content_length(resp.headers())
                    .map_err(|e| other(ObjectError::new("stat", &p, e)))?
                {
                    m.set_content_length(v);
                }

                if let Some(v) = parse_etag(resp.headers())
                    .map_err(|e| other(ObjectError::new("stat", &p, e)))?
                {
                    m.set_etag(v);
                    m.set_content_md5(v.trim_matches('"'));
                }

                if let Some(v) = parse_last_modified(resp.headers())
                    .map_err(|e| other(ObjectError::new("stat", &p, e)))?
                {
                    m.set_last_modified(v);
                }

                if p.ends_with('/') {
                    m.set_mode(ObjectMode::DIR);
                } else {
                    m.set_mode(ObjectMode::FILE);
                };

                Ok(m)
            }
            StatusCode::NOT_FOUND if p.ends_with('/') => {
                let mut m = ObjectMetadata::default();
                m.set_mode(ObjectMode::DIR);

                Ok(m)
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error("stat", args.path(), er);
                Err(err)
            }
        }
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let p = self.get_abs_path(args.path());

        let resp = self.delete_object(&p).await?;

        match resp.status() {
            StatusCode::NO_CONTENT => Ok(()),
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error("delete", args.path(), er);
                Err(err)
            }
        }
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let mut path = self.get_abs_path(args.path());
        // Make sure list path is endswith '/'
        if !path.ends_with('/') && !path.is_empty() {
            path.push('/')
        }

        Ok(Box::new(DirStream::new(Arc::new(self.clone()), &path)))
    }

    fn presign(&self, args: &OpPresign) -> Result<PresignedRequest> {
        let path = self.get_abs_path(args.path());

        // We will not send this request out, just for signing.
        let mut req = match args.operation() {
            Operation::Read => self.get_object_request(&path, None, None)?,
            Operation::Write => self.put_object_request(&path, AsyncBody::empty())?,
            op => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    ObjectError::new(
                        "presign",
                        &path,
                        anyhow!("presign for {op} is not supported"),
                    ),
                ))
            }
        };

        self.signer
            .sign_query(&mut req, args.expire())
            .map_err(|e| new_request_sign_error("presign", args.path(), e))?;

        // We don't need this request anymore, consume it directly.
        let (parts, _) = req.into_parts();

        Ok(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        ))
    }

    async fn create_multipart(&self, args: &OpCreateMultipart) -> Result<String> {
        let path = self.get_abs_path(args.path());

        let mut resp = self.s3_initiate_multipart_upload(&path).await?;

        match resp.status() {
            StatusCode::OK => {
                let bs = resp.bytes().await.map_err(|e| {
                    new_response_consume_error(Operation::CreateMultipart, &path, e)
                })?;

                let result: InitiateMultipartUploadResult = quick_xml::de::from_slice(&bs)
                    .map_err(|err| {
                        other(ObjectError::new(
                            Operation::CreateMultipart,
                            &path,
                            anyhow!("parse xml: {err:?}"),
                        ))
                    })?;

                Ok(result.upload_id)
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::CreateMultipart, args.path(), er);
                Err(err)
            }
        }
    }

    async fn write_multipart(&self, args: &OpWriteMultipart, r: BytesReader) -> Result<ObjectPart> {
        let p = self.get_abs_path(args.path());

        let mut req = self.s3_upload_part_request(
            &p,
            args.upload_id(),
            args.part_number(),
            AsyncBody::from_reader_sized(unshared_reader(r), args.size()),
        )?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error(Operation::WriteMultipart, &p, e))?;

        let mut resp = self
            .client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::WriteMultipart, &p, e))?;

        match resp.status() {
            StatusCode::OK => {
                let etag = parse_etag(resp.headers())
                    .map_err(|e| other(ObjectError::new(Operation::WriteMultipart, &p, e)))?
                    .ok_or_else(|| {
                        other(ObjectError::new(
                            Operation::WriteMultipart,
                            &p,
                            anyhow!("ETag not present in returning response"),
                        ))
                    })?
                    .to_string();

                resp.consume().await.map_err(|err| {
                    new_response_consume_error(Operation::WriteMultipart, &p, err)
                })?;

                Ok(ObjectPart::new(args.part_number(), &etag))
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::WriteMultipart, args.path(), er);
                Err(err)
            }
        }
    }

    async fn complete_multipart(&self, args: &OpCompleteMultipart) -> Result<()> {
        let path = self.get_abs_path(args.path());

        let mut resp = self
            .s3_complete_multipart_upload(&path, args.upload_id(), args.parts())
            .await?;

        match resp.status() {
            StatusCode::OK => {
                resp.consume().await.map_err(|e| {
                    new_response_consume_error(Operation::CompleteMultipart, &path, e)
                })?;

                Ok(())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::CompleteMultipart, args.path(), er);
                Err(err)
            }
        }
    }

    async fn abort_multipart(&self, args: &OpAbortMultipart) -> Result<()> {
        let path = self.get_abs_path(args.path());

        let mut resp = self
            .s3_abort_multipart_upload(&path, args.upload_id())
            .await?;

        match resp.status() {
            StatusCode::NO_CONTENT => {
                resp.consume()
                    .await
                    .map_err(|e| new_response_consume_error(Operation::AbortMultipart, &path, e))?;

                Ok(())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::AbortMultipart, args.path(), er);
                Err(err)
            }
        }
    }
}

impl Backend {
    fn get_object_request(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<isahc::Request<AsyncBody>> {
        let url = format!("{}/{}", self.endpoint, percent_encode_path(path));

        let mut req = isahc::Request::get(&url);

        if offset.is_some() || size.is_some() {
            req = req.header(
                http::header::RANGE,
                BytesRange::new(offset, size).to_string(),
            );
        }

        // Set SSE headers.
        // TODO: how will this work with presign?
        req = self.insert_sse_headers(req, false);

        let req = req
            .body(AsyncBody::empty())
            .map_err(|e| new_request_build_error("read", path, e))?;

        Ok(req)
    }

    async fn get_object(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<isahc::Response<AsyncBody>> {
        let mut req = self.get_object_request(path, offset, size)?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error("read", path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error("read", path, e))
    }

    fn put_object_request(&self, path: &str, body: AsyncBody) -> Result<isahc::Request<AsyncBody>> {
        let url = format!("{}/{}", self.endpoint, percent_encode_path(path));

        let mut req = isahc::Request::put(&url);

        // TODO: we can use if-let in the future.
        //
        // error[E0658]: `let` expressions in this position are unstable
        //     --> src/services/s3/backend.rs:1361:12
        //      |
        // 1361 |         if let Some(content_length) = body.len() && !body.is_empty() {
        //      |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        //      |
        //      = note: see issue #53667 <https://github.com/rust-lang/rust/issues/53667> for more information
        if !body.is_empty() {
            if let Some(content_length) = body.len() {
                req = req.header(CONTENT_LENGTH, content_length)
            }
        }

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        // Set body
        let req = req
            .body(body)
            .map_err(|e| new_request_build_error("write", path, e))?;

        Ok(req)
    }

    async fn head_object(&self, path: &str) -> Result<isahc::Response<AsyncBody>> {
        let url = format!("{}/{}", self.endpoint, percent_encode_path(path));

        let mut req = isahc::Request::head(&url);

        // Set SSE headers.
        req = self.insert_sse_headers(req, false);

        let mut req = req
            .body(AsyncBody::empty())
            .map_err(|e| new_request_build_error("stat", path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error("stat", path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error("stat", path, e))
    }

    async fn delete_object(&self, path: &str) -> Result<isahc::Response<AsyncBody>> {
        let url = format!("{}/{}", self.endpoint, percent_encode_path(path));

        let mut req = isahc::Request::delete(&url)
            .body(AsyncBody::empty())
            .map_err(|e| new_request_build_error("delete", path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error("delete", path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error("delete", path, e))
    }

    /// Make this functions as `pub(suber)` because `DirStream` depends
    /// on this.
    pub(super) async fn list_objects(
        &self,
        path: &str,
        continuation_token: &str,
    ) -> Result<isahc::Response<AsyncBody>> {
        let mut url = format!(
            "{}?list-type=2&delimiter=/&prefix={}",
            self.endpoint,
            percent_encode_path(path)
        );
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

        let mut req = isahc::Request::get(&url)
            .body(AsyncBody::empty())
            .map_err(|e| new_request_build_error("list", path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error("list", path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error("list", path, e))
    }

    async fn s3_initiate_multipart_upload(&self, path: &str) -> Result<isahc::Response<AsyncBody>> {
        let url = format!("{}/{}?uploads", self.endpoint, percent_encode_path(path));

        let req = isahc::Request::post(&url);

        // Set SSE headers.
        let req = self.insert_sse_headers(req, true);

        let mut req = req
            .body(AsyncBody::empty())
            .map_err(|e| new_request_build_error(Operation::CreateMultipart, path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error(Operation::CreateMultipart, path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::CreateMultipart, path, e))
    }

    fn s3_upload_part_request(
        &self,
        path: &str,
        upload_id: &str,
        part_number: usize,
        body: AsyncBody,
    ) -> Result<isahc::Request<AsyncBody>> {
        let url = format!(
            "{}/{}?partNumber={}&uploadId={}",
            self.endpoint,
            percent_encode_path(path),
            part_number,
            upload_id
        );

        let mut req = isahc::Request::put(&url);

        if !body.is_empty() {
            if let Some(content_length) = body.len() {
                req = req.header(CONTENT_LENGTH, content_length)
            }
        }

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        // Set body
        let req = req
            .body(body)
            .map_err(|e| new_request_build_error(Operation::WriteMultipart, path, e))?;

        Ok(req)
    }

    async fn s3_complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: &[ObjectPart],
    ) -> Result<isahc::Response<AsyncBody>> {
        let url = format!(
            "{}/{}?uploadId={}",
            self.endpoint,
            percent_encode_path(path),
            upload_id
        );

        let req = isahc::Request::post(&url);

        // Set SSE headers.
        let req = self.insert_sse_headers(req, true);

        let content = quick_xml::se::to_string(&CompleteMultipartUploadRequest {
            part: parts
                .iter()
                .map(|v| CompleteMultipartUploadRequestPart {
                    part_number: v.part_number(),
                    etag: v.etag().to_string(),
                })
                .collect(),
        })
        .map_err(|err| {
            other(ObjectError::new(
                Operation::CompleteMultipart,
                path,
                anyhow!("build xml: {err:?}"),
            ))
        })?;
        // Make sure content length has been set to avoid post with chunked encoding.
        let req = req.header(CONTENT_LENGTH, content.len());
        // Set content-type to `application/xml` to avoid mixed with form post.
        let req = req.header(CONTENT_TYPE, "application/xml");

        let mut req = req
            .body(AsyncBody::from(content))
            .map_err(|e| new_request_build_error(Operation::CompleteMultipart, path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error(Operation::CompleteMultipart, path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::CompleteMultipart, path, e))
    }

    async fn s3_abort_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
    ) -> Result<isahc::Response<AsyncBody>> {
        let url = format!(
            "{}/{}?uploadId={}",
            self.endpoint,
            percent_encode_path(path),
            upload_id,
        );

        let mut req = isahc::Request::delete(&url)
            .body(AsyncBody::empty())
            .map_err(|e| new_request_build_error(Operation::AbortMultipart, path, e))?;

        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error(Operation::AbortMultipart, path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::AbortMultipart, path, e))
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

#[derive(Default, Debug, Serialize)]
#[serde(default, rename_all = "PascalCase")]
struct CompleteMultipartUploadRequestPart {
    #[serde(rename = "$unflatten=PartNumber")]
    part_number: usize,
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
    ///     #[serde(rename = "$unflatten=PartNumber")]
    ///     part_number: usize,
    ///     #[serde(rename = "$unflatten=ETag", serialize_with = "partial_escape")]
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
    #[serde(rename = "$unflatten=ETag")]
    etag: String,
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::iproduct;

    use super::*;

    #[tokio::test]
    async fn test_detect_region() {
        let _ = env_logger::try_init();

        let client = HttpClient::new();

        let endpoint_cases = vec![
            Some("s3.amazonaws.com"),
            Some("https://s3.amazonaws.com"),
            Some("https://s3.us-east-2.amazonaws.com"),
            None,
        ];

        let region_cases = vec![Some("us-east-2"), None];

        for (endpoint, region) in iproduct!(endpoint_cases, region_cases) {
            let mut b = Builder::default();
            if let Some(endpoint) = endpoint {
                b.endpoint(endpoint);
            }
            if let Some(region) = region {
                b.region(region);
            }

            let (endpoint, region) = b
                .detect_region(&client, "test", &HashMap::new())
                .expect("detect region must success");
            assert_eq!(endpoint, "https://s3.us-east-2.amazonaws.com");
            assert_eq!(region, "us-east-2");
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
            quick_xml::de::from_slice(&bs).expect("must success");

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
                // Cleanup space
                .replace(' ', "")
                // Cleanup new line
                .replace('\n', "")
                // Escape `"` by hand to address <https://github.com/tafia/quick-xml/issues/362>
                .replace('"', "&quot;")
        )
    }
}
