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

use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::mem;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::BufMut;
use futures::TryStreamExt;
use http::header::HeaderName;
use http::HeaderValue;
use http::Response;
use http::StatusCode;
use hyper::body::HttpBody;
use hyper::Body;
use log::debug;
use log::error;
use log::info;
use metrics::increment_counter;
use minitrace::trace;
use once_cell::sync::Lazy;
use reqsign::services::aws::v4::Signer;
use time::format_description::well_known::Rfc2822;
use time::OffsetDateTime;

use super::object_stream::S3ObjectStream;
use crate::error::other;
use crate::error::BackendError;
use crate::error::ObjectError;
use crate::io_util::new_http_channel;
use crate::io_util::HttpBodyWriter;
use crate::object::Metadata;
use crate::object::ObjectStreamer;
use crate::ops::BytesRange;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::BytesReader;
use crate::BytesWriter;
use crate::ObjectMode;

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
}

impl Debug for Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Builder");

        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region);

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
            self.endpoint = Some(endpoint.to_string())
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

    // Read RFC-0057: Auto Region for detailed behavior.
    async fn detect_region(
        &self,
        client: &hyper::Client<
            hyper_tls::HttpsConnector<hyper::client::HttpConnector>,
            hyper::Body,
        >,
        bucket: &str,
        context: &HashMap<String, String>,
    ) -> Result<(String, String)> {
        let endpoint = match &self.endpoint {
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

        if let Some(region) = &self.region {
            return if let Some(template) = ENDPOINT_TEMPLATES.get(endpoint.as_str()) {
                let endpoint = template.replace("{region}", region);
                Ok((endpoint, region.to_string()))
            } else {
                Ok((endpoint.to_string(), region.to_string()))
            };
        }

        let req = hyper::Request::head(format!("{endpoint}/{bucket}"))
            .body(hyper::Body::empty())
            .expect("must be valid request");
        let res = client
            .request(req)
            .await
            .map_err(|e| other(BackendError::new(context.clone(), e)))?;

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
                Ok((endpoint.to_string(), region))
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
                            "can't detect region automatically, no valid endpoint template for {}",
                            &endpoint
                        ),
                    ))
                })?;

                let endpoint = template.replace("{region}", &region);

                Ok((endpoint, region))
            }
            // Unexpected status code
            code => {
                return Err(other(BackendError::new(
                    context.clone(),
                    anyhow!(
                        "can't detect region automatically, unexpected response: status code {}",
                        code
                    ),
                )))
            }
        }
    }

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

        let client = hyper::Client::builder().build(hyper_tls::HttpsConnector::new());

        let (endpoint, region) = self.detect_region(&client, bucket, &context).await?;
        context.insert("endpoint".to_string(), endpoint.clone());
        context.insert("region".to_string(), region.clone());
        debug!("backend use endpoint: {}, region: {}", &endpoint, &region);

        let mut signer_builder = reqsign::services::aws::v4::Signer::builder();
        signer_builder.service("s3");
        signer_builder.region(&region);
        signer_builder.allow_anonymous();

        if let (Some(ak), Some(sk)) = (&self.access_key_id, &self.secret_access_key) {
            signer_builder.access_key(ak);
            signer_builder.secret_key(sk);
        }

        let signer = signer_builder
            .build()
            .await
            .map_err(|e| other(BackendError::new(context, e)))?;

        info!("backend build finished: {:?}", &self);
        Ok(Arc::new(Backend {
            root,
            endpoint,
            signer: Arc::new(signer),
            bucket: self.bucket.clone(),
            client,

            server_side_encryption: mem::take(&mut self.server_side_encryption),
            server_side_encryption_aws_kms_key_id: mem::take(
                &mut self.server_side_encryption_aws_kms_key_id,
            ),
            server_side_encryption_customer_algorithm: mem::take(
                &mut self.server_side_encryption_customer_algorithm,
            ),
            server_side_encryption_customer_key: mem::take(
                &mut self.server_side_encryption_customer_key,
            ),
            server_side_encryption_customer_key_md5: mem::take(
                &mut self.server_side_encryption_customer_key_md5,
            ),
        }))
    }
}

/// Backend for s3 services.
#[derive(Debug, Clone)]
pub struct Backend {
    bucket: String,
    endpoint: String,
    signer: Arc<Signer>,
    client: hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>, hyper::Body>,
    // root will be "/" or "/abc/"
    root: String,

    server_side_encryption: Option<String>,
    server_side_encryption_aws_kms_key_id: Option<String>,
    server_side_encryption_customer_algorithm: Option<String>,
    server_side_encryption_customer_key: Option<String>,
    server_side_encryption_customer_key_md5: Option<String>,
}

impl Backend {
    pub fn build() -> Builder {
        Builder::default()
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
                let mut v: HeaderValue = v.parse().expect("must be valid header value");
                v.set_sensitive(true);

                req = req.header(
                    HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION),
                    v,
                )
            }
            if let Some(v) = &self.server_side_encryption_aws_kms_key_id {
                let mut v: HeaderValue = v.parse().expect("must be valid header value");
                v.set_sensitive(true);

                req = req.header(
                    HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID),
                    v,
                )
            }
        }

        if let Some(v) = &self.server_side_encryption_customer_algorithm {
            let mut v: HeaderValue = v.parse().expect("must be valid header value");
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM),
                v,
            )
        }
        if let Some(v) = &self.server_side_encryption_customer_key {
            let mut v: HeaderValue = v.parse().expect("must be valid header value");
            v.set_sensitive(true);

            req = req.header(
                HeaderName::from_static(constants::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY),
                v,
            )
        }
        if let Some(v) = &self.server_side_encryption_customer_key_md5 {
            let mut v: HeaderValue = v.parse().expect("must be valid header value");
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
    #[trace("read")]
    async fn create(&self, args: &OpCreate) -> Result<()> {
        increment_counter!("opendal_s3_create_requests");
        let p = self.get_abs_path(args.path());

        let req = self.put_object(&p, 0, Body::empty()).await;
        let resp = self.client.request(req).await.map_err(|e| {
            error!("object {} put_object: {:?}", args.path(), e);
            other(ObjectError::new("read", args.path(), e))
        })?;

        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => {
                debug!("object {} create finished", args.path());
                Ok(())
            }
            _ => Err(parse_error_response_without_body(
                resp,
                "write",
                args.path(),
            )),
        }
    }

    #[trace("read")]
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        increment_counter!("opendal_s3_read_requests");

        let p = self.get_abs_path(args.path());
        debug!(
            "object {} read start: offset {:?}, size {:?}",
            &p,
            args.offset(),
            args.size()
        );

        let resp = self.get_object(&p, args.offset(), args.size()).await?;

        match resp.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                debug!(
                    "object {} reader created: offset {:?}, size {:?}",
                    &p,
                    args.offset(),
                    args.size()
                );

                Ok(Box::new(
                    resp.into_body()
                        .into_stream()
                        .map_err(move |e| other(ObjectError::new("read", &p, e)))
                        .into_async_read(),
                ))
            }
            _ => Err(parse_error_response_with_body(resp, "read", &p).await),
        }
    }

    #[trace("write")]
    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        let p = self.get_abs_path(args.path());
        debug!("object {} write start: size {}", &p, args.size());

        let (tx, body) = new_http_channel();

        let req = self.put_object(&p, args.size(), body).await;

        let bs = HttpBodyWriter::new(args, tx, self.client.request(req), |op, resp| {
            match resp.status() {
                StatusCode::CREATED | StatusCode::OK => {
                    debug!("object {} write finished: size {:?}", op.path(), op.size());
                    Ok(())
                }
                _ => Err(parse_error_response_without_body(resp, "write", op.path())),
            }
        });

        Ok(Box::new(bs))
    }

    #[trace("stat")]
    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        increment_counter!("opendal_s3_stat_requests");

        let p = self.get_abs_path(args.path());
        debug!("object {} stat start", &p);

        // Stat root always returns a DIR.
        if self.get_rel_path(&p).is_empty() {
            let mut m = Metadata::default();
            m.set_path(args.path());
            m.set_content_length(0);
            m.set_mode(ObjectMode::DIR);
            m.set_complete();

            debug!("backed root object stat finished");
            return Ok(m);
        }

        let resp = self.head_object(&p).await?;

        match resp.status() {
            StatusCode::OK => {
                let mut m = Metadata::default();
                m.set_path(args.path());

                // Parse content_length
                if let Some(v) = resp.headers().get(http::header::CONTENT_LENGTH) {
                    let v =
                        u64::from_str(v.to_str().expect("header must not contain non-ascii value"))
                            .expect("content length header must contain valid length");

                    m.set_content_length(v);
                }

                // Parse content_md5
                if let Some(v) = resp.headers().get(HeaderName::from_static("content-md5")) {
                    let v = v.to_str().expect("header must not contain non-ascii value");
                    m.set_content_md5(v);
                }

                // Parse last_modified
                if let Some(v) = resp.headers().get(http::header::LAST_MODIFIED) {
                    let v = v.to_str().expect("header must not contain non-ascii value");
                    let t =
                        OffsetDateTime::parse(v, &Rfc2822).expect("must contain valid time format");
                    m.set_last_modified(t);
                }

                if p.ends_with('/') {
                    m.set_mode(ObjectMode::DIR);
                } else {
                    m.set_mode(ObjectMode::FILE);
                };

                m.set_complete();

                debug!("object {} stat finished: {:?}", &p, m);
                Ok(m)
            }
            StatusCode::NOT_FOUND if p.ends_with('/') => {
                let mut m = Metadata::default();
                m.set_path(args.path());
                m.set_content_length(0);
                m.set_mode(ObjectMode::DIR);
                m.set_complete();

                debug!("object {} stat finished", &p);
                Ok(m)
            }
            _ => Err(parse_error_response_with_body(resp, "stat", &p).await),
        }
    }
    #[trace("delete")]
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        increment_counter!("opendal_s3_delete_requests");

        let p = self.get_abs_path(&args.path);
        debug!("object {} delete start", &p);

        let resp = self.delete_object(&p).await?;

        match resp.status() {
            StatusCode::NO_CONTENT => {
                debug!("object {} delete finished", &p);
                Ok(())
            }
            _ => Err(parse_error_response_with_body(resp, "delete", &p).await),
        }
    }
    #[trace("list")]
    async fn list(&self, args: &OpList) -> Result<ObjectStreamer> {
        increment_counter!("opendal_s3_list_requests");

        let mut path = self.get_abs_path(&args.path);
        // Make sure list path is endswith '/'
        if !path.ends_with('/') && !path.is_empty() {
            path.push('/')
        }
        debug!("object {} list start", &path);

        Ok(Box::new(S3ObjectStream::new(self.clone(), path)))
    }
}

impl Backend {
    #[trace("get_object")]
    pub(crate) async fn get_object(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<hyper::Response<hyper::Body>> {
        let mut req = hyper::Request::get(&format!("{}/{}/{}", self.endpoint, self.bucket, path));

        if offset.is_some() || size.is_some() {
            req = req.header(
                http::header::RANGE,
                BytesRange::new(offset, size).to_string(),
            );
        }

        // Set SSE headers.
        req = self.insert_sse_headers(req, false);

        let mut req = req
            .body(hyper::Body::empty())
            .expect("must be valid request");

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.request(req).await.map_err(|e| {
            error!("object {} get_object: {:?}", path, e);
            other(ObjectError::new("read", path, e))
        })
    }

    #[trace("put_object")]
    pub(crate) async fn put_object(
        &self,
        path: &str,
        size: u64,
        body: hyper::Body,
    ) -> hyper::Request<hyper::Body> {
        let mut req = hyper::Request::put(&format!("{}/{}/{}", self.endpoint, self.bucket, path));

        // Set content length.
        req = req.header(http::header::CONTENT_LENGTH, size.to_string());

        // Set SSE headers.
        req = self.insert_sse_headers(req, true);

        // Set body
        let mut req = req.body(body).expect("must be valid request");

        self.signer.sign(&mut req).await.expect("sign must success");

        req
    }

    #[trace("head_object")]
    pub(crate) async fn head_object(&self, path: &str) -> Result<hyper::Response<hyper::Body>> {
        let mut req = hyper::Request::head(&format!("{}/{}/{}", self.endpoint, self.bucket, path));

        // Set SSE headers.
        req = self.insert_sse_headers(req, false);

        let mut req = req
            .body(hyper::Body::empty())
            .expect("must be valid request");

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.request(req).await.map_err(|e| {
            error!("object {} head_object: {:?}", path, e);
            other(ObjectError::new("stat", path, e))
        })
    }

    #[trace("delete_object")]
    pub(crate) async fn delete_object(&self, path: &str) -> Result<hyper::Response<hyper::Body>> {
        let mut req =
            hyper::Request::delete(&format!("{}/{}/{}", self.endpoint, self.bucket, path))
                .body(hyper::Body::empty())
                .expect("must be valid request");

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.request(req).await.map_err(|e| {
            error!("object {} delete_object: {:?}", path, e);
            other(ObjectError::new("delete", path, e))
        })
    }

    #[trace("list_objects")]
    pub(crate) async fn list_objects(
        &self,
        path: &str,
        continuation_token: &str,
    ) -> Result<hyper::Response<hyper::Body>> {
        let mut uri = format!(
            "{}/{}?list-type=2&delimiter=/&prefix={}",
            self.endpoint, self.bucket, path
        );
        if !continuation_token.is_empty() {
            uri.push_str(&format!("&continuation-token={}", continuation_token))
        }

        let mut req = hyper::Request::get(uri)
            .body(hyper::Body::empty())
            .expect("must be valid request");

        self.signer.sign(&mut req).await.expect("sign must success");

        self.client.request(req).await.map_err(|e| {
            error!("object {} list_object: {:?}", path, e);
            other(ObjectError::new("list", path, e))
        })
    }
}

// Read and decode whole error response.
fn parse_error_response_without_body(resp: Response<Body>, op: &'static str, path: &str) -> Error {
    let (part, _) = resp.into_parts();
    let kind = match part.status {
        StatusCode::NOT_FOUND => ErrorKind::NotFound,
        StatusCode::FORBIDDEN => ErrorKind::PermissionDenied,
        _ => ErrorKind::Other,
    };

    Error::new(
        kind,
        ObjectError::new(op, path, anyhow!("response part: {:?}", part)),
    )
}

// Read and decode whole error response.
async fn parse_error_response_with_body(
    resp: Response<Body>,
    op: &'static str,
    path: &str,
) -> Error {
    let (part, mut body) = resp.into_parts();
    let kind = match part.status {
        StatusCode::NOT_FOUND => ErrorKind::NotFound,
        StatusCode::FORBIDDEN => ErrorKind::PermissionDenied,
        _ => ErrorKind::Other,
    };

    // Only read 4KiB from the response to avoid broken services.
    let mut bs = Vec::new();
    let mut limit = 4 * 1024;

    while let Some(b) = body.data().await {
        match b {
            Ok(b) => {
                bs.put_slice(&b[..min(b.len(), limit)]);
                limit -= b.len();
                if limit == 0 {
                    break;
                }
            }
            Err(e) => {
                return other(ObjectError::new(
                    op,
                    path,
                    anyhow!("parse error response parse: {:?}", e),
                ))
            }
        }
    }

    Error::new(
        kind,
        ObjectError::new(
            op,
            path,
            anyhow!(
                "response part: {:?}, body: {:?}",
                part,
                String::from_utf8_lossy(&bs)
            ),
        ),
    )
}

#[cfg(test)]
mod tests {
    use itertools::iproduct;

    use super::*;

    #[tokio::test]
    async fn test_detect_region() {
        let client = hyper::Client::builder().build(hyper_tls::HttpsConnector::new());

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
                .await
                .expect("detect region must success");
            assert_eq!(endpoint, "https://s3.us-east-2.amazonaws.com");
            assert_eq!(region, "us-east-2");
        }
    }
}
