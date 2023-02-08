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
use std::sync::Arc;

use async_trait::async_trait;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::RANGE;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Uri;
use log::debug;
use reqsign::AliyunOssBuilder;
use reqsign::AliyunOssSigner;

use super::dir_stream::DirStream;
use super::error::parse_error;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Aliyun Object Storage Sevice (OSS) support
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] list
/// - [ ] presign
/// - [ ] multipart
/// - [ ] blocking
///
/// # Configuration
///
/// - `root`: Set the work dir for backend.
/// - `bucket`: Set the container name for backend.
/// - `endpoint`: Set the endpoint for backend.
/// - `presign_endpoint`: Set the endpoint for presign.
/// - `access_key_id`: Set the access_key_id for backend.
/// - `access_key_secret`: Set the access_key_secret for backend.
/// - `role_arn`: Set the role of backend.
/// - `oidc_token`: Set the oidc_token for backend.
/// - `allow_anonymous`: Set the backend access OSS in anonymous way.
///
/// Refer to [`OssBuilder`]'s public API docs for more information.
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use std::sync::Arc;
///
/// use anyhow::Result;
/// use opendal::services::Oss;
/// use opendal::Object;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Create OSS backend builder.
///     let mut builder = Oss::default();
///     // Set the root for oss, all operations will happen under this root.
///     //
///     // NOTE: the root must be absolute path.
///     builder.root("/path/to/dir");
///     // Set the bucket name, this is required.
///     builder.bucket("test");
///     // Set the endpoint.
///     //
///     // For example:
///     // - "https://oss-ap-northeast-1.aliyuncs.com"
///     // - "https://oss-hangzhou.aliyuncs.com"
///     builder.endpoint("https://oss-cn-beijing.aliyuncs.com");
///     // Set the access_key_id and access_key_secret.
///     //
///     // OpenDAL will try load credential from the env.
///     // If credential not set and no valid credential in env, OpenDAL will
///     // send request without signing like anonymous user.
///     builder.access_key_id("access_key_id");
///     builder.access_key_secret("access_key_secret");
///
///     let op: Operator = Operator::create(builder)?.finish();
///
///     // Create an object handle to start operation on object.
///     let _: Object = op.object("test_file");
///
///     Ok(())
/// }
/// ```
#[derive(Default, Clone)]
pub struct OssBuilder {
    root: Option<String>,

    endpoint: Option<String>,
    presign_endpoint: Option<String>,
    bucket: String,

    // authenticate options
    access_key_id: Option<String>,
    access_key_secret: Option<String>,

    allow_anonymous: bool,

    http_client: Option<HttpClient>,
}

impl Debug for OssBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Builder");
        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("presign_endpoint", &self.presign_endpoint)
            .field("allow_anonymous", &self.allow_anonymous);

        if self.access_key_id.is_some() {
            d.field("access_key_id", &"<redacted>");
        }

        if self.access_key_secret.is_some() {
            d.field("access_key_secret", &"<redacted>");
        }

        d.finish()
    }
}

impl OssBuilder {
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
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.endpoint = Some(endpoint.trim_end_matches('/').to_string())
        }

        self
    }

    /// Set a endpoint for generating presigned urls.
    ///
    /// You can offer a public endpoint like <https://oss-cn-beijing.aliyuncs.com> to return a presinged url for
    /// public accessors, along with an internal endpoint like <https://oss-cn-beijing-internal.aliyuncs.com>
    /// to access objects in a faster path.
    ///
    /// - If presign_endpoint is set, we will use presign_endpoint on generating presigned urls.
    /// - if not, we will use endpoint as default.
    pub fn presign_endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.presign_endpoint = Some(endpoint.trim_end_matches('/').to_string())
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

    /// Set access_key_secret of this backend.
    ///
    /// - If access_key_secret is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn access_key_secret(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.access_key_secret = Some(v.to_string())
        }

        self
    }

    /// Anonymously access the bucket.
    pub fn allow_anonymous(&mut self) -> &mut Self {
        self.allow_anonymous = true;
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

    /// preprocess the endpoint option
    fn parse_endpoint(&self, endpoint: &Option<String>, bucket: &str) -> Result<(String, String)> {
        let (endpoint, host) = match endpoint.clone() {
            Some(ep) => {
                let uri = ep.parse::<Uri>().map_err(|err| {
                    Error::new(ErrorKind::BackendConfigInvalid, "endpoint is invalid")
                        .with_context("service", Scheme::Oss)
                        .with_context("endpoint", &ep)
                        .set_source(err)
                })?;
                let host = uri.host().ok_or_else(|| {
                    Error::new(ErrorKind::BackendConfigInvalid, "endpoint host is empty")
                        .with_context("service", Scheme::Oss)
                        .with_context("endpoint", &ep)
                })?;
                let full_host = format!("{bucket}.{host}");
                let endpoint = format!("https://{full_host}");
                (endpoint, full_host)
            }
            None => {
                return Err(
                    Error::new(ErrorKind::BackendConfigInvalid, "endpoint is empty")
                        .with_context("service", Scheme::Oss),
                );
            }
        };
        Ok((endpoint, host))
    }
}

impl Builder for OssBuilder {
    const SCHEME: Scheme = Scheme::Oss;
    type Accessor = OssBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = OssBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("bucket").map(|v| builder.bucket(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("presign_endpoint")
            .map(|v| builder.presign_endpoint(v));
        map.get("access_key_id").map(|v| builder.access_key_id(v));
        map.get("access_key_secret")
            .map(|v| builder.access_key_secret(v));
        map.get("allow_anonymous")
            .filter(|v| *v == "on" || *v == "true")
            .map(|_| builder.allow_anonymous());

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle endpoint, region and bucket name.
        let bucket = match self.bucket.is_empty() {
            false => Ok(&self.bucket),
            true => Err(
                Error::new(ErrorKind::BackendConfigInvalid, "bucket is empty")
                    .with_context("service", Scheme::Oss),
            ),
        }?;

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Oss)
            })?
        };

        // Retrieve endpoint and host by parsing the endpoint option and bucket. If presign_endpoint is not
        // set, take endpoint as default presign_endpoint.
        let (endpoint, host) = self.parse_endpoint(&self.endpoint, bucket)?;
        debug!("backend use bucket {}, endpoint: {}", &bucket, &endpoint);

        let presign_endpoint = if self.presign_endpoint.is_some() {
            self.parse_endpoint(&self.presign_endpoint, bucket)?.0
        } else {
            endpoint.clone()
        };
        debug!("backend use presign_endpoint: {}", &presign_endpoint);

        let mut signer_builder = AliyunOssBuilder::default();

        if self.allow_anonymous {
            signer_builder.allow_anonymous();
        }

        signer_builder.bucket(bucket);

        if let (Some(ak), Some(sk)) = (&self.access_key_id, &self.access_key_secret) {
            signer_builder.access_key_id(ak);
            signer_builder.access_key_secret(sk);
        }

        let signer = signer_builder.build().map_err(|e| {
            Error::new(ErrorKind::BackendConfigInvalid, "build AliyunOssSigner")
                .with_context("service", Scheme::Oss)
                .with_context("endpoint", &endpoint)
                .with_context("bucket", bucket)
                .set_source(e)
        })?;

        debug!("Backend build finished: {:?}", &self);

        Ok(OssBackend {
            root,
            endpoint,
            presign_endpoint,
            host,
            client,
            bucket: self.bucket.clone(),
            signer: Arc::new(signer),
        })
    }
}

#[derive(Clone)]
/// Aliyun Object Storage Service backend
pub struct OssBackend {
    client: HttpClient,

    root: String,
    bucket: String,
    /// buffered host string
    ///
    /// format: <bucket-name>.<endpoint-domain-name>
    host: String,
    endpoint: String,
    presign_endpoint: String,
    signer: Arc<AliyunOssSigner>,
}

impl Debug for OssBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("host", &self.host)
            .finish()
    }
}

#[async_trait]
impl Accessor for OssBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();

    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Oss)
            .set_root(&self.root)
            .set_name(&self.bucket)
            .set_capabilities(
                AccessorCapability::Read
                    | AccessorCapability::Write
                    | AccessorCapability::List
                    | AccessorCapability::Presign,
            )
            .set_hints(AccessorHint::ReadIsStreamable);
        am
    }

    async fn create(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let resp = self
            .oss_put_object(path, None, None, AsyncBody::Empty)
            .await?;
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
        let resp = self.oss_get_object(path, args.range()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_object_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
        let resp = self
            .oss_put_object(
                path,
                Some(args.size()),
                args.content_type(),
                AsyncBody::Reader(r),
            )
            .await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpWrite::new(args.size()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        if path == "/" {
            let m = ObjectMetadata::new(ObjectMode::DIR);
            return Ok(RpStat::new(m));
        }

        let resp = self.oss_head_object(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_object_metadata(path, resp.headers()).map(RpStat::new),
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                let m = ObjectMetadata::new(ObjectMode::DIR);
                Ok(RpStat::new(m))
            }

            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.obs_delete_object(path).await?;
        let status = resp.status();
        match status {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => {
                resp.into_body().consume().await?;
                Ok(RpDelete::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, ObjectPager)> {
        Ok((
            RpList::default(),
            Box::new(DirStream::new(Arc::new(self.clone()), &self.root, path)) as ObjectPager,
        ))
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        // We will not send this request out, just for signing.
        let mut req = match args.operation() {
            PresignOperation::Stat(_) => self.oss_head_object_request(path, true)?,
            PresignOperation::Read(v) => self.oss_get_object_request(path, v.range(), true)?,
            PresignOperation::Write(v) => {
                self.oss_put_object_request(path, None, v.content_type(), AsyncBody::Empty, true)?
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "oss doesn't support multipart now",
                ))
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
}

impl OssBackend {
    fn oss_put_object_request(
        &self,
        path: &str,
        size: Option<u64>,
        content_type: Option<&str>,
        body: AsyncBody,
        is_presign: bool,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let url = format!("{}/{}", endpoint, percent_encode_path(&p));

        let mut req = Request::put(&url);

        req = req.header(CONTENT_LENGTH, size.unwrap_or_default());

        if let Some(mime) = content_type {
            req = req.header(CONTENT_TYPE, mime);
        }

        let req = req.body(body).map_err(new_request_build_error)?;
        Ok(req)
    }

    fn oss_get_object_request(
        &self,
        path: &str,
        range: BytesRange,
        is_presign: bool,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let url = format!("{}/{}", endpoint, percent_encode_path(&p));

        let mut req = Request::get(&url);
        req = req.header(CONTENT_TYPE, "application/octet-stream");

        if !range.is_full() {
            req = req.header(RANGE, range.to_header());
            // Adding `x-oss-range-behavior` header to use standard behavior.
            // ref: https://help.aliyun.com/document_detail/39571.html
            req = req.header("x-oss-range-behavior", "standard");
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    fn oss_delete_object_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(false);
        let url = format!("{}/{}", endpoint, percent_encode_path(&p));
        let req = Request::delete(&url);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    fn oss_head_object_request(&self, path: &str, is_presign: bool) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let url = format!("{}/{}", endpoint, percent_encode_path(&p));

        let req = Request::head(&url);
        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    fn oss_list_object_request(
        &self,
        path: &str,
        token: Option<String>,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let endpoint = self.get_endpoint(false);
        let url = format!(
            "{}/?list-type=2&delimiter=/&prefix={}{}",
            endpoint,
            percent_encode_path(&p),
            token
                .map(|t| format!("&continuation-token={}", percent_encode_path(&t)))
                .unwrap_or_default(),
        );

        let req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;
        Ok(req)
    }

    async fn oss_get_object(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.oss_get_object_request(path, range, false)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;
        self.client.send_async(req).await
    }

    async fn oss_head_object(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.oss_head_object_request(path, false)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;
        self.client.send_async(req).await
    }

    async fn oss_put_object(
        &self,
        path: &str,
        size: Option<u64>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.oss_put_object_request(path, size, content_type, body, false)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;
        self.client.send_async(req).await
    }

    pub(super) async fn oss_list_object(
        &self,
        path: &str,
        token: Option<String>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.oss_list_object_request(path, token)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;
        self.client.send_async(req).await
    }

    async fn obs_delete_object(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.oss_delete_object_request(path)?;
        self.signer.sign(&mut req).map_err(new_request_sign_error)?;
        self.client.send_async(req).await
    }

    fn get_endpoint(&self, is_presign: bool) -> &str {
        if is_presign {
            &self.presign_endpoint
        } else {
            &self.endpoint
        }
    }
}
