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
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::str::FromStr;

use async_trait::async_trait;
use bytes::Buf;
use http::header;
use http::HeaderMap;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use serde::Deserialize;

use super::error::parse_error;
use super::lister::Multistatus;
use super::lister::WebdavLister;
use super::writer::WebdavWriter;
use crate::raw::*;
use crate::*;

/// Config for [WebDAV](https://datatracker.ietf.org/doc/html/rfc4918) backend support.
#[derive(Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct WebdavConfig {
    endpoint: Option<String>,
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
    root: Option<String>,
}

impl Debug for WebdavConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("WebdavConfig");

        d.field("endpoint", &self.endpoint)
            .field("username", &self.username)
            .field("password", &self.password)
            .field("token", &self.token)
            .field("root", &self.root);

        d.finish_non_exhaustive()
    }
}

/// [WebDAV](https://datatracker.ietf.org/doc/html/rfc4918) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct WebdavBuilder {
    config: WebdavConfig,
    http_client: Option<HttpClient>,
}

impl Debug for WebdavBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("WebdavBuilder");

        d.field("config", &self.config);

        d.finish_non_exhaustive()
    }
}

impl WebdavBuilder {
    /// Set endpoint for http backend.
    ///
    /// For example: `https://example.com`
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.config.endpoint = if endpoint.is_empty() {
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
            self.config.username = Some(username.to_owned());
        }
        self
    }

    /// set the password for Webdav
    ///
    /// default: no password
    pub fn password(&mut self, password: &str) -> &mut Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_owned());
        }
        self
    }

    /// set the bearer token for Webdav
    ///
    /// default: no access token
    pub fn token(&mut self, token: &str) -> &mut Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_owned());
        }
        self
    }

    /// Set root path of http backend.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.config.root = if root.is_empty() {
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

        let endpoint = match &self.config.endpoint {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                    .with_context("service", Scheme::Webdav));
            }
        };

        let uri = http::Uri::from_str(endpoint).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                .set_source(err)
                .with_context("service", Scheme::Webdav)
        })?;
        // Some webdav server may have base dir like `/remote.php/webdav/`
        // returned in the `href`.
        let base_dir = uri.path().trim_end_matches('/');

        let root = normalize_root(&self.config.root.take().unwrap_or_default());
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
        if let Some(username) = &self.config.username {
            auth = Some(format_authorization_by_basic(
                username,
                self.config.password.as_deref().unwrap_or_default(),
            )?);
        }
        if let Some(token) = &self.config.token {
            auth = Some(format_authorization_by_bearer(token)?)
        }

        debug!("backend build finished: {:?}", &self);
        Ok(WebdavBackend {
            endpoint: endpoint.to_string(),
            base_dir: base_dir.to_string(),
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
    base_dir: String,
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
    type Writer = oio::OneShotWriter<WebdavWriter>;
    type BlockingWriter = ();
    type Lister = Option<oio::PageLister<WebdavLister>>;
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(Scheme::Webdav)
            .set_root(&self.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,
                read_can_next: true,
                read_with_range: true,

                write: true,
                write_can_empty: true,

                create_dir: true,
                delete: true,

                copy: true,

                rename: true,

                list: true,

                ..Default::default()
            });

        ma
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.ensure_parent_path(path).await?;
        self.create_dir_internal(path).await?;

        Ok(RpCreateDir::default())
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.webdav_get(path, args).await?;
        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let size = parse_content_length(resp.headers())?;
                let range = parse_content_range(resp.headers())?;
                Ok((
                    RpRead::new().with_size(size).with_range(range),
                    resp.into_body(),
                ))
            }
            StatusCode::RANGE_NOT_SATISFIABLE => {
                resp.into_body().consume().await?;
                Ok((RpRead::new().with_size(Some(0)), IncomingAsyncBody::empty()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.ensure_parent_path(path).await?;

        let p = build_abs_path(&self.root, path);

        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(WebdavWriter::new(self.clone(), args, p)),
        ))
    }

    /// # Notes
    ///
    /// There is a strange dead lock issues when copying a non-exist file, so we will check
    /// if the source exists first.
    ///
    /// For example: <https://github.com/apache/incubator-opendal/pull/2809>
    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        if let Err(err) = self.stat(from, OpStat::default()).await {
            if err.kind() == ErrorKind::NotFound {
                return Err(err);
            }
        }

        self.ensure_parent_path(to).await?;

        let resp = self.webdav_copy(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::NO_CONTENT => Ok(RpCopy::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        self.ensure_parent_path(to).await?;

        let resp = self.webdav_move(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::NO_CONTENT => Ok(RpRename::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let mut header_map = HeaderMap::new();
        // not include children
        header_map.insert("Depth", "0".parse().unwrap());
        header_map.insert(header::ACCEPT, "application/xml".parse().unwrap());

        let resp = self.webdav_propfind(path, Some(header_map)).await?;

        let status = resp.status();

        if !status.is_success() {
            return Err(parse_error(resp).await?);
        }

        let bs = resp.into_body().bytes().await?;
        let result: Multistatus =
            quick_xml::de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;
        let item = result
            .response
            .first()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed getting item stat: bad response",
                )
            })?
            .parse_into_metadata()?;
        Ok(RpStat::new(item))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.webdav_delete(path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        if args.recursive() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "webdav doesn't support list with recursive",
            ));
        }

        let mut header_map = HeaderMap::new();
        header_map.insert("Depth", "1".parse().unwrap());
        header_map.insert(header::CONTENT_TYPE, "application/xml".parse().unwrap());
        let resp = self.webdav_propfind(path, Some(header_map)).await?;
        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::MULTI_STATUS => {
                let bs = resp.into_body().bytes().await?;
                let result: Multistatus =
                    quick_xml::de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

                let l = WebdavLister::new(&self.base_dir, &self.root, path, result);

                Ok((RpList::default(), Some(oio::PageLister::new(l))))
            }
            StatusCode::NOT_FOUND if path.ends_with('/') => Ok((RpList::default(), None)),
            _ => Err(parse_error(resp).await?),
        }
    }
}

impl WebdavBackend {
    async fn webdav_get(&self, path: &str, args: OpRead) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);
        let url: String = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::get(&url);

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth.clone())
        }

        let range = args.range();
        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn webdav_put(
        &self,
        abs_path: &str,
        size: Option<u64>,
        args: &OpWrite,
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

        if let Some(mime) = args.content_type() {
            req = req.header(header::CONTENT_TYPE, mime)
        }

        if let Some(cd) = args.content_disposition() {
            req = req.header(header::CONTENT_DISPOSITION, cd)
        }

        // Set body
        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    async fn webdav_mkcol(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::builder().method("MKCOL").uri(&url);
        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth);
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    async fn webdav_propfind(
        &self,
        path: &str,
        headers: Option<HeaderMap>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!("{}/{}", self.endpoint, percent_encode_path(&p));
        let mut req = Request::builder().method("PROPFIND").uri(&url);

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth);
        }

        if let Some(headers) = headers {
            for (name, value) in headers {
                // all key should be not None, otherwise panic
                req = req.header(name.unwrap(), value);
            }
        }

        // rfc4918 9.1: retrieve all properties define in specification
        let body;
        {
            req = req.header(header::CONTENT_TYPE, "application/xml");
            // XML body must start without a new line. Otherwise, the server will panic: `xmlParseChunk() failed`
            let all_prop_xml_body = r#"<?xml version="1.0" encoding="utf-8" ?>
            <D:propfind xmlns:D="DAV:">
                <D:allprop/>
            </D:propfind>
        "#;
            body = AsyncBody::Bytes(bytes::Bytes::from(all_prop_xml_body));
        }

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
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

        self.client.send(req).await
    }

    async fn webdav_copy(&self, from: &str, to: &str) -> Result<Response<IncomingAsyncBody>> {
        let source = build_abs_path(&self.root, from);
        let target = build_abs_path(&self.root, to);

        let source = format!("{}/{}", self.endpoint, percent_encode_path(&source));
        let target = format!("{}/{}", self.endpoint, percent_encode_path(&target));

        let mut req = Request::builder().method("COPY").uri(&source);

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth);
        }

        req = req.header("Destination", target);

        // We always specific "T" for keeping to overwrite the destination.
        req = req.header("Overwrite", "T");

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    async fn webdav_move(&self, from: &str, to: &str) -> Result<Response<IncomingAsyncBody>> {
        let source = build_abs_path(&self.root, from);
        let target = build_abs_path(&self.root, to);

        let source = format!("{}/{}", self.endpoint, percent_encode_path(&source));
        let target = format!("{}/{}", self.endpoint, percent_encode_path(&target));

        let mut req = Request::builder().method("MOVE").uri(&source);

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth);
        }

        req = req.header("Destination", target);

        // We always specific "T" for keeping to overwrite the destination.
        req = req.header("Overwrite", "T");

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    async fn create_dir_internal(&self, path: &str) -> Result<()> {
        let resp = self.webdav_mkcol(path).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED
            // Allow multiple status
            | StatusCode::MULTI_STATUS
            // The MKCOL method can only be performed on a deleted or non-existent resource.
            // This error means the directory already exists which is allowed by create_dir.
            | StatusCode::METHOD_NOT_ALLOWED => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn ensure_parent_path(&self, mut path: &str) -> Result<()> {
        let mut dirs = VecDeque::default();

        while path != "/" {
            // check path first.
            let parent = get_parent(path);

            let mut header_map = HeaderMap::new();
            // not include children
            header_map.insert("Depth", "0".parse().unwrap());
            header_map.insert(header::ACCEPT, "application/xml".parse().unwrap());

            let resp = self.webdav_propfind(parent, Some(header_map)).await?;
            match resp.status() {
                StatusCode::OK | StatusCode::MULTI_STATUS => break,
                StatusCode::NOT_FOUND => {
                    dirs.push_front(parent);
                    path = parent
                }
                _ => return Err(parse_error(resp).await?),
            }
        }

        for dir in dirs {
            self.create_dir_internal(dir).await?;
        }
        Ok(())
    }
}
