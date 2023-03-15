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

use async_trait::async_trait;
use bytes::Buf;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;

use super::error::parse_error;
use super::list_response::Multistatus;
use super::pager::WebdavPager;
use super::writer::WebdavWriter;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// [WebDAV](https://datatracker.ietf.org/doc/html/rfc4918) backend support.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] list
/// - [ ] ~~scan~~
/// - [ ] ~~presign~~
/// - [ ] blocking
///
/// # Notes
///
/// Bazel Remote Caching and Ccache HTTP Storage is also part of this service.
/// Users can use `webdav` to connect those services.
///
/// # Configuration
///
/// - `endpoint`: set the endpoint for webdav
/// - `root`: Set the work directory for backend
///
/// You can refer to [`WebdavBuilder`]'s docs for more information
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::services::Webdav;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // create backend builder
///     let mut builder = Webdav::default();
///
///     builder
///         .endpoint("127.0.0.1")
///         .username("xxx")
///         .password("xxx");
///
///     let op: Operator = Operator::new(builder)?.finish();
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct WebdavBuilder {
    endpoint: Option<String>,
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
    root: Option<String>,
    http_client: Option<HttpClient>,
}

impl Debug for WebdavBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("Builder");
        de.field("endpoint", &self.endpoint);
        de.field("root", &self.root);

        de.finish()
    }
}

impl WebdavBuilder {
    /// Set endpoint for http backend.
    ///
    /// For example: `https://example.com`
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };

        self
    }

    /// set the password for Webdav
    ///
    /// default: no password
    pub fn username(&mut self, username: &str) -> &mut Self {
        if !username.is_empty() {
            self.username = Some(username.to_owned());
        }
        self
    }

    /// set the password for Webdav
    ///
    /// default: no password
    pub fn password(&mut self, password: &str) -> &mut Self {
        if !password.is_empty() {
            self.password = Some(password.to_owned());
        }
        self
    }

    /// set the bearer token for Webdav
    ///
    /// default: no access token
    pub fn token(&mut self, token: &str) -> &mut Self {
        if !token.is_empty() {
            self.token = Some(token.to_owned());
        }
        self
    }

    /// Set root path of http backend.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

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

impl Builder for WebdavBuilder {
    const SCHEME: Scheme = Scheme::Webdav;
    type Accessor = WebdavBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = WebdavBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("username").map(|v| builder.username(v));
        map.get("password").map(|v| builder.password(v));
        map.get("token").map(|v| builder.token(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let endpoint = match &self.endpoint {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                    .with_context("service", Scheme::Webdav))
            }
        };

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Webdav)
            })?
        };

        let mut auth = None;
        if let Some(username) = &self.username {
            auth = Some(format_authorization_by_basic(
                username,
                self.password.as_deref().unwrap_or_default(),
            )?);
        }
        if let Some(token) = &self.token {
            auth = Some(format_authorization_by_bearer(token)?)
        }

        debug!("backend build finished: {:?}", &self);
        Ok(WebdavBackend {
            endpoint: endpoint.to_string(),
            authorization: auth,
            root,
            client,
        })
    }
}
/// Backend is used to serve `Accessor` support for http.
#[derive(Clone)]
pub struct WebdavBackend {
    endpoint: String,
    root: String,
    client: HttpClient,

    authorization: Option<String>,
}

impl Debug for WebdavBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("client", &self.client)
            .finish()
    }
}

#[async_trait]
impl Accessor for WebdavBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = WebdavWriter;
    type BlockingWriter = ();
    type Pager = WebdavPager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(Scheme::Webdav)
            .set_root(&self.root)
            .set_capabilities(
                AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
            )
            .set_hints(AccessorHint::ReadStreamable);

        ma
    }

    async fn create(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        // create dir recursively, split path by `/` and create each dir except the last one
        let abs_path = build_abs_path(&self.root, path);
        let abs_path = abs_path.as_str();
        let mut parts: Vec<&str> = abs_path.split('/').filter(|x| !x.is_empty()).collect();
        if !parts.is_empty() {
            parts.pop();
        }

        let mut sub_path = String::new();
        for sub_part in parts {
            let sub_path_with_slash = sub_part.to_owned() + "/";
            sub_path.push_str(&sub_path_with_slash);
            self.create_internal(&sub_path).await?;
        }

        self.create_internal(abs_path).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.webdav_get(path, args.range()).await?;

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

        let p = build_abs_path(&self.root, path);

        Ok((RpWrite::default(), WebdavWriter::new(self.clone(), args, p)))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.webdav_head(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_metadata(path, resp.headers()).map(RpStat::new),
            // HTTP Server like nginx could return FORBIDDEN if auto-index
            // is not enabled, we should ignore them.
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN if path.ends_with('/') => {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.webdav_delete(path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Pager)> {
        // XML body must start without a new line. Otherwise, the server will panic: `xmlParseChunk() failed`
        let all_prop_xml_body = r#"<?xml version="1.0" encoding="utf-8" ?>
            <D:propfind xmlns:D="DAV:">
                <D:allprop/>
            </D:propfind>
        "#;

        let async_body = AsyncBody::Bytes(bytes::Bytes::from(all_prop_xml_body));
        let resp = self
            .webdav_propfind(path, None, "application/xml".into(), async_body)
            .await?;
        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::MULTI_STATUS => {
                let bs = resp.into_body().bytes().await?;
                let result: Multistatus =
                    quick_xml::de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

                Ok((
                    RpList::default(),
                    WebdavPager::new(&self.root, path, result),
                ))
            }
            StatusCode::NOT_FOUND if path.ends_with('/') => Ok((
                RpList::default(),
                WebdavPager::new(
                    &self.root,
                    path,
                    Multistatus {
                        response: Vec::new(),
                    },
                ),
            )),
            _ => Err(parse_error(resp).await?),
        }
    }
}

impl WebdavBackend {
    async fn webdav_get(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::get(&url);

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth.clone())
        }

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }

    pub async fn webdav_put(
        &self,
        abs_path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        content_disposition: Option<&str>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let url = format!("{}/{}", self.endpoint, percent_encode_path(abs_path));

        let mut req = Request::put(&url);

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth.clone())
        }

        if let Some(size) = size {
            req = req.header(header::CONTENT_LENGTH, size)
        }

        if let Some(mime) = content_type {
            req = req.header(header::CONTENT_TYPE, mime)
        }

        if let Some(cd) = content_disposition {
            req = req.header(header::CONTENT_DISPOSITION, cd)
        }

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }

    async fn webdav_mkcol(
        &self,
        abs_path: &str,
        content_type: Option<&str>,
        content_disposition: Option<&str>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let url = format!("{}/{}", self.endpoint, percent_encode_path(abs_path));

        let mut req = Request::builder().method("MKCOL").uri(&url);
        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth);
        }

        if let Some(mime) = content_type {
            req = req.header(header::CONTENT_TYPE, mime)
        }

        if let Some(cd) = content_disposition {
            req = req.header(header::CONTENT_DISPOSITION, cd)
        }

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }

    async fn webdav_propfind(
        &self,
        path: &str,
        size: Option<u64>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));
        let mut req = Request::builder()
            .method("PROPFIND")
            .uri(&url)
            .header("Depth", "1");

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth);
        }

        if let Some(size) = size {
            req = req.header(header::CONTENT_LENGTH, size)
        }

        if let Some(mime) = content_type {
            req = req.header(header::CONTENT_TYPE, mime)
        }

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }

    async fn webdav_head(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::head(&url);

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth.clone())
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }

    async fn webdav_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::delete(&url);

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth.clone())
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }

    async fn create_internal(&self, abs_path: &str) -> Result<RpCreate> {
        let resp = if abs_path.ends_with('/') {
            self.webdav_mkcol(abs_path, None, None, AsyncBody::Empty)
                .await?
        } else {
            self.webdav_put(abs_path, Some(0), None, None, AsyncBody::Empty)
                .await?
        };

        let status = resp.status();

        match status {
            StatusCode::CREATED
            | StatusCode::OK
            // `File exists` will return `Method Not Allowed`
            | StatusCode::METHOD_NOT_ALLOWED
            // create existing dir will return conflict
            | StatusCode::CONFLICT
            // create existing file will return no_content
            | StatusCode::NO_CONTENT => {
                resp.into_body().consume().await?;
                Ok(RpCreate::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}
