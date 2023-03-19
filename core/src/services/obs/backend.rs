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
use std::sync::Arc;

use async_trait::async_trait;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Uri;
use log::debug;
use reqsign::HuaweicloudObsSigner;

use super::error::parse_error;
use super::pager::ObsPager;
use super::writer::ObsWriter;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Huawei Cloud OBS services support.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] list
/// - [x] scan
/// - [ ] presign
/// - [ ] blocking
///
/// # Configuration
///
/// - `root`: Set the work directory for backend
/// - `bucket`: Set the container name for backend
/// - `endpoint`: Customizable endpoint setting
/// - `access_key_id`: Set the access_key_id for backend.
/// - `secret_access_key`: Set the secret_access_key for backend.
///
/// You can refer to [`ObsBuilder`]'s docs for more information
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::services::Obs;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // create backend builder
///     let mut builder = Obs::default();
///
///     // set the storage bucket for OpenDAL
///     builder.bucket("test");
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
#[derive(Default, Clone)]
pub struct ObsBuilder {
    root: Option<String>,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    bucket: Option<String>,
    http_client: Option<HttpClient>,
}

impl Debug for ObsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl ObsBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_string())
        }

        self
    }

    /// Set endpoint of this backend.
    ///
    /// Both huaweicloud default domain and user domain endpoints are allowed.
    /// Please DO NOT add the bucket name to the endpoint.
    ///
    /// - `https://obs.cn-north-4.myhuaweicloud.com`
    /// - `obs.cn-north-4.myhuaweicloud.com` (https by default)
    /// - `https://custom.obs.com` (port should not be set)
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            self.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }

        self
    }

    /// Set access_key_id of this backend.
    /// - If it is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn access_key_id(&mut self, access_key_id: &str) -> &mut Self {
        if !access_key_id.is_empty() {
            self.access_key_id = Some(access_key_id.to_string());
        }

        self
    }

    /// Set secret_access_key of this backend.
    /// - If it is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn secret_access_key(&mut self, secret_access_key: &str) -> &mut Self {
        if !secret_access_key.is_empty() {
            self.secret_access_key = Some(secret_access_key.to_string());
        }

        self
    }

    /// Set bucket of this backend.
    /// The param is required.
    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        if !bucket.is_empty() {
            self.bucket = Some(bucket.to_string());
        }

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
}

impl Builder for ObsBuilder {
    const SCHEME: Scheme = Scheme::Obs;
    type Accessor = ObsBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = ObsBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("bucket").map(|v| builder.bucket(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("access_key_id").map(|v| builder.access_key_id(v));
        map.get("secret_access_key")
            .map(|v| builder.secret_access_key(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let bucket = match &self.bucket {
            Some(bucket) => Ok(bucket.to_string()),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", Scheme::Obs),
            ),
        }?;
        debug!("backend use bucket {}", &bucket);

        let uri = match &self.endpoint {
            Some(endpoint) => endpoint.parse::<Uri>().map_err(|err| {
                Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                    .with_context("service", Scheme::Obs)
                    .set_source(err)
            }),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_context("service", Scheme::Obs)),
        }?;

        let scheme = match uri.scheme_str() {
            Some(scheme) => scheme.to_string(),
            None => "https".to_string(),
        };

        let (endpoint, is_obs_default) = {
            let host = uri.host().unwrap_or_default().to_string();
            if host.starts_with("obs.") && host.ends_with(".myhuaweicloud.com") {
                (format!("{bucket}.{host}"), true)
            } else {
                (host, false)
            }
        };
        debug!("backend use endpoint {}", &endpoint);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Obs)
            })?
        };

        let mut signer_builder = HuaweicloudObsSigner::builder();
        if let (Some(access_key_id), Some(secret_access_key)) =
            (&self.access_key_id, &self.secret_access_key)
        {
            signer_builder
                .access_key(access_key_id)
                .secret_key(secret_access_key);
        }

        // Set the bucket name in CanonicalizedResource.
        // 1. If the bucket is bound to a user domain name, use the user domain name as the bucket name,
        // for example, `/obs.ccc.com/object`. `obs.ccc.com` is the user domain name bound to the bucket.
        // 2. If you do not access OBS using a user domain name, this field is in the format of `/bucket/object`.
        //
        // Please refer to this doc for more details:
        // https://support.huaweicloud.com/intl/en-us/api-obs/obs_04_0010.html
        if is_obs_default {
            signer_builder.bucket(&bucket);
        } else {
            signer_builder.bucket(&endpoint);
        }

        let signer = signer_builder.build().map_err(|e| {
            Error::new(ErrorKind::Unexpected, "build HuaweicloudObsSigner")
                .with_context("service", Scheme::Obs)
                .set_source(e)
        })?;

        debug!("backend build finished: {:?}", &self);
        Ok(ObsBackend {
            client,
            root,
            endpoint: format!("{}://{}", &scheme, &endpoint),
            signer: Arc::new(signer),
            bucket,
        })
    }
}

/// Backend for Huaweicloud OBS services.
#[derive(Debug, Clone)]
pub struct ObsBackend {
    pub client: HttpClient,
    root: String,
    endpoint: String,
    pub signer: Arc<HuaweicloudObsSigner>,
    bucket: String,
}

#[async_trait]
impl Accessor for ObsBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = ObsWriter;
    type BlockingWriter = ();
    type Pager = ObsPager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        use AccessorCapability::*;
        use AccessorHint::*;

        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Obs)
            .set_root(&self.root)
            .set_name(&self.bucket)
            .set_capabilities(Read | Write | List | Scan)
            .set_hints(ReadStreamable);

        am
    }

    async fn create(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let mut req = self.obs_put_object_request(path, Some(0), None, AsyncBody::Empty)?;

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
        let resp = self.obs_get_object(path, args.range()).await?;

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
        if args.append() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "append write is not supported",
            ));
        }

        Ok((
            RpWrite::default(),
            ObsWriter::new(self.clone(), args, path.to_string()),
        ))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.obs_get_head_object(path).await?;

        let status = resp.status();

        // The response is very similar to azblob.
        match status {
            StatusCode::OK => parse_into_metadata(path, resp.headers()).map(RpStat::new),
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.obs_delete_object(path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT | StatusCode::ACCEPTED | StatusCode::NOT_FOUND => {
                Ok(RpDelete::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        Ok((
            RpList::default(),
            ObsPager::new(Arc::new(self.clone()), &self.root, path, "/", args.limit()),
        ))
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        Ok((
            RpScan::default(),
            ObsPager::new(Arc::new(self.clone()), &self.root, path, "", args.limit()),
        ))
    }
}

impl ObsBackend {
    async fn obs_get_object(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::get(&url);

        if !range.is_full() {
            req = req.header(http::header::RANGE, range.to_header())
        }

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    pub fn obs_put_object_request(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
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

        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    async fn obs_get_head_object(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        // The header 'Origin' is optional for API calling, the doc has mistake, confirmed with customer service of huaweicloud.
        // https://support.huaweicloud.com/intl/en-us/api-obs/obs_04_0084.html

        let req = Request::head(&url);

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    async fn obs_delete_object(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let req = Request::delete(&url);

        let mut req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }

    pub(crate) async fn obs_list_objects(
        &self,
        path: &str,
        next_marker: &str,
        delimiter: &str,
        limit: Option<usize>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let mut queries = vec![];
        if !path.is_empty() {
            queries.push(format!("prefix={}", percent_encode_path(&p)));
        }
        if !delimiter.is_empty() {
            queries.push(format!("delimiter={delimiter}"));
        }
        if let Some(limit) = limit {
            queries.push(format!("max-keys={limit}"));
        }
        if !next_marker.is_empty() {
            queries.push(format!("marker={next_marker}"));
        }

        let url = if queries.is_empty() {
            self.endpoint.to_string()
        } else {
            format!("{}?{}", self.endpoint, queries.join("&"))
        };

        let mut req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.signer.sign(&mut req).map_err(new_request_sign_error)?;

        self.client.send_async(req).await
    }
}
