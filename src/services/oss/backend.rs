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
use std::io::Result;
use std::sync::Arc;

use async_trait::async_trait;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Uri;
use reqsign::services::aliyun::oss::Signer;

use super::dir_stream::DirStream;
use super::error::parse_error;
use crate::accessor::AccessorCapability;
use crate::error::new_other_backend_error;
use crate::error::new_other_object_error;
use crate::http_util::new_request_build_error;
use crate::http_util::new_request_send_error;
use crate::http_util::new_request_sign_error;
use crate::http_util::new_response_consume_error;
use crate::http_util::parse_content_length;
use crate::http_util::parse_error_response;
use crate::http_util::parse_etag;
use crate::http_util::parse_last_modified;
use crate::http_util::percent_encode_path;
use crate::http_util::AsyncBody;
use crate::http_util::HttpClient;
use crate::http_util::IncomingAsyncBody;
use crate::object::ObjectPageStreamer;
use crate::ops::BytesRange;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::Operation;
use crate::path::build_abs_path;
use crate::path::normalize_root;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BytesReader;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::ObjectStreamer;
use crate::Scheme;

type AsyncReq = Request<AsyncBody>;
type AsyncResp = Response<IncomingAsyncBody>;

const OSS_DEFAULT_ENDPOINT_HOST: &str = "oss-accelerate.aliyuncs.com";

/// Builder for Aliyun Object Storage Service
#[derive(Default, Clone)]
pub struct Builder {
    root: Option<String>,

    region: Option<String>,
    endpoint: Option<String>,
    bucket: String,

    // authenticate options
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    role_arn: Option<String>,
    oidc_token: Option<String>,

    allow_anonymous: bool,
}

impl Debug for Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Builder");
        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("allow_anonymous", &self.allow_anonymous.to_string());

        if self.access_key_id.is_some() {
            d.field("access_key_id", &"<redacted>");
        }

        if self.secret_access_key.is_some() {
            d.field("secret_access_key", &"<redacted>");
        }

        if self.role_arn.is_some() {
            d.field("role_arn", &"<redacted>");
        }

        if self.oidc_token.is_some() {
            d.field("oidc_token", &"<redacted>");
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
    /// if the endpoint is set, the `region` field will be ignored.
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.endpoint = Some(endpoint.trim_end_matches('/').to_string())
        }

        self
    }

    /// Region represent the signing region and storage region of this endpoint.
    ///
    /// The final endpoint will be set like: `https://<region>.aliyuncs.com`
    ///
    /// Please ensure the `region` is valid, like `oss-cn-hangzhou`, e.g.
    ///
    /// NOTE:
    /// - if the `endpoint` field is set, the `region` field will be ignored
    /// - if both of them are unset, the global oss accelerate endpoint will be used
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

    /// Set access_key_secret of this backend.
    ///
    /// - If secret_access_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn secret_access_key(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.secret_access_key = Some(v.to_string())
        }

        self
    }

    /// Set the role of this backend
    pub fn role_arn(&mut self, role_arn: &str) -> &mut Self {
        if !role_arn.is_empty() {
            self.role_arn = Some(role_arn.to_string());
        }
        self
    }

    /// Aliyun's temporary token support
    pub fn oidc_token(&mut self, token: &str) -> &mut Self {
        if !token.is_empty() {
            self.oidc_token = Some(token.to_string());
        }
        self
    }

    /// Anonymously access the bucket.
    pub fn allow_anonymous(&mut self) -> &mut Self {
        self.allow_anonymous = true;
        self
    }

    /// finish building
    pub fn build(&self) -> Result<Backend> {
        log::info!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.clone().unwrap_or_default());
        log::info!("backend use root {}", &root);

        // Handle endpoint, region and bucket name.
        let bucket = match self.bucket.is_empty() {
            false => Ok(&self.bucket),
            true => Err(new_other_backend_error(
                HashMap::from([("bucket".to_string(), "".to_string())]),
                anyhow::anyhow!("bucket is empty"),
            )),
        }?;
        log::debug!("backend use bucket {}", &bucket);

        // Setup error context so that we don't need to construct many times.
        let mut context: HashMap<String, String> =
            HashMap::from([("bucket".to_string(), bucket.to_string())]);

        let (endpoint, host) = match self.endpoint.clone() {
            Some(ep) => {
                let uri = ep.parse::<Uri>().map_err(|err| {
                    new_other_backend_error(
                        context.clone(),
                        anyhow::anyhow!("invalid endpoint uri: {:?}", err),
                    )
                })?;
                let host = uri.host().ok_or_else(|| {
                    new_other_backend_error(
                        context.clone(),
                        anyhow::anyhow!("host should be valid"),
                    )
                })?;
                let full_host = format!("{}.{}", bucket, host);
                let ep = format!("https://{}", full_host);
                (ep, full_host)
            }
            None => match self.region.clone() {
                Some(rg) => {
                    let host = format!("{}.{}.aliyuncs.com", bucket, rg);
                    (format!("https://{}", host), host)
                }
                None => {
                    let host = format!("{}.{}", bucket, OSS_DEFAULT_ENDPOINT_HOST);
                    (format!("https://{}", host), host)
                }
            },
        };
        context.insert("endpoint".to_string(), endpoint.clone());

        let mut signer_builder = reqsign::services::aliyun::oss::Builder::default();

        if self.allow_anonymous {
            signer_builder.allow_anonymous();
        }

        signer_builder.bucket(&bucket);

        if let (Some(ak), Some(sk)) = (&self.access_key_id, &self.secret_access_key) {
            signer_builder.access_key_id(ak);
            signer_builder.access_key_secret(sk);
        }

        if let Some(token) = &self.oidc_token {
            signer_builder.oidc_token(token);
        }
        if let Some(role_arn) = &self.role_arn {
            signer_builder.role_arn(role_arn);
        }
        let signer = signer_builder
            .build()
            .map_err(|e| new_other_backend_error(context, e))?;

        log::info!("Backend build finished: {:?}", &self);

        Ok(Backend {
            root,
            endpoint,
            host,
            client: HttpClient::new(),
            bucket: self.bucket.clone(),
            signer: Arc::new(signer),
        })
    }
}

#[derive(Clone)]
/// Aliyun Object Storage Service backend
pub struct Backend {
    client: HttpClient,

    root: String,
    bucket: String,
    /// buffered host string
    ///
    /// format: <bucket-name>.<endpoint-domain-name>
    host: String,
    endpoint: String,
    signer: Arc<Signer>,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("host", &self.host)
            .finish()
    }
}

impl Backend {
    /// The builder of OSS backend
    pub fn builder() -> Builder {
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
                "oidc_token" => builder.oidc_token(v),
                "role_arn" => builder.role_arn(v),
                "allow_anonymous" => builder.allow_anonymous(),
                _ => continue,
            };
        }
        builder.build()
    }
}

impl Backend {
    fn put_object_request(
        &self,
        path: &str,
        size: Option<u64>,
        body: AsyncBody,
    ) -> Result<AsyncReq> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::put(&url);

        let timestamp = reqsign::time::format_rfc2822(time::OffsetDateTime::now_utc());

        req = req
            .header(header::HOST, &self.host)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::DATE, timestamp);

        if let Some(size) = size {
            req = req.header(header::CONTENT_LENGTH, size)
        }

        let req = req
            .body(body)
            .map_err(|e| new_request_build_error(Operation::Write, path, e))?;
        Ok(req)
    }

    fn get_object_request(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<AsyncReq> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let timestamp = reqsign::time::format_rfc2822(time::OffsetDateTime::now_utc());
        let mut req = Request::get(&url);
        req = req
            .header("Host", &self.host)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::DATE, timestamp);

        if offset.unwrap_or_default() != 0 || size.is_some() {
            req = req.header(header::RANGE, BytesRange::new(offset, size).to_string());
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(|e| new_request_build_error(Operation::Read, path, e))?;

        Ok(req)
    }

    fn delete_object_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let timestamp = reqsign::time::format_rfc2822(time::OffsetDateTime::now_utc());
        let mut req = Request::delete(&url);
        req = req
            .header("Host", &self.host)
            .header(header::DATE, timestamp);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(|e| new_request_build_error(Operation::Delete, path, e))?;

        Ok(req)
    }

    fn head_object_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let timestamp = reqsign::time::format_rfc2822(time::OffsetDateTime::now_utc());
        let mut req = Request::head(&url);
        req = req
            .header(header::HOST, &self.host)
            .header(header::DATE, timestamp);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(|e| new_request_build_error(Operation::Stat, path, e))?;

        Ok(req)
    }

    fn list_object_request(&self, path: &str, token: String) -> Result<AsyncReq> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/?list-type=2&delimiter=/&prefix={}&continuation-token={}",
            self.endpoint,
            percent_encode_path(&p),
            token
        );

        let timestamp = reqsign::time::format_rfc2822(time::OffsetDateTime::now_utc());

        let req = Request::get(&url)
            .header(header::HOST, &self.host)
            .header(header::DATE, timestamp)
            .body(AsyncBody::Empty)
            .map_err(|e| new_request_build_error(Operation::List, path, e))?;
        Ok(req)
    }

    async fn get_object(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<AsyncResp> {
        let req = self.get_object_request(path, offset, size)?;

        self.sign_and_send(req, Operation::Read, path).await
    }

    async fn put_object(&self, path: &str) -> Result<AsyncResp> {
        let req = self.put_object_request(path, Some(0), AsyncBody::Empty)?;
        self.sign_and_send(req, Operation::Create, path).await
    }

    async fn stat_object(&self, path: &str) -> Result<AsyncResp> {
        let req = self.head_object_request(path)?;
        self.sign_and_send(req, Operation::Stat, path).await
    }

    pub(super) async fn list_object(&self, path: &str, token: String) -> Result<AsyncResp> {
        let req = self.list_object_request(path, token)?;
        self.sign_and_send(req, Operation::List, path).await
    }

    async fn delete_object(&self, path: &str) -> Result<AsyncResp> {
        let req = self.delete_object_request(path)?;
        self.sign_and_send(req, Operation::Delete, path).await
    }
    async fn sign_and_send(
        &self,
        mut req: Request<AsyncBody>,
        op: Operation,
        path: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        self.signer
            .sign(&mut req)
            .map_err(|e| new_request_sign_error(op, path, e))?;
        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(op, path, e))
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Oss)
            .set_root(&self.root)
            .set_name(&self.bucket)
            .set_capabilities(
                AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
            );
        am
    }

    async fn create(&self, path: &str, _: OpCreate) -> Result<()> {
        let resp = self.put_object(path).await?;
        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body()
                    .consume()
                    .await
                    .map_err(|err| new_response_consume_error(Operation::Create, path, err))?;
                Ok(())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Create, path, er);
                Err(err)
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        let resp = self.get_object(path, args.offset(), args.size()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(resp.into_body().reader()),
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Read, path, er);
                Err(err)
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        let req = self.put_object_request(path, Some(args.size()), AsyncBody::Reader(r))?;
        let resp = self.sign_and_send(req, Operation::Write, path).await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body()
                    .consume()
                    .await
                    .map_err(|err| new_response_consume_error(Operation::Write, path, err))?;
                Ok(args.size())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Write, path, er);
                Err(err)
            }
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<ObjectMetadata> {
        if path == "/" {
            let m = ObjectMetadata::new(ObjectMode::DIR);

            return Ok(m);
        }

        let resp = self.stat_object(path).await?;
        let status = resp.status();

        match status {
            StatusCode::OK => {
                let mut m = if path.ends_with('/') {
                    ObjectMetadata::new(ObjectMode::DIR)
                } else {
                    ObjectMetadata::new(ObjectMode::FILE)
                };

                if let Some(v) = parse_content_length(resp.headers())
                    .map_err(|e| new_other_object_error(Operation::Stat, path, e))?
                {
                    m.set_content_length(v);
                }

                if let Some(v) = parse_etag(resp.headers())
                    .map_err(|e| new_other_object_error(Operation::Stat, path, e))?
                {
                    m.set_etag(v);
                }

                if let Some(v) = parse_last_modified(resp.headers())
                    .map_err(|e| new_other_object_error(Operation::Stat, path, e))?
                {
                    m.set_last_modified(v);
                }
                Ok(m)
            }
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                let m = ObjectMetadata::new(ObjectMode::DIR);
                Ok(m)
            }

            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Stat, path, er);
                Err(err)
            }
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<()> {
        let p = build_abs_path(&self.root, path);
        let resp = self.delete_object(&p).await?;
        let status = resp.status();
        match status {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(()),
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Delete, path, er);
                Err(err)
            }
        }
    }

    async fn list(&self, path: &str, _: OpList) -> Result<ObjectStreamer> {
        Ok(Box::new(ObjectPageStreamer::new(DirStream::new(
            Arc::new(self.clone()),
            &self.root,
            path,
        ))))
    }
}
