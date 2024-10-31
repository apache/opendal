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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use http::header;
use http::header::IF_MATCH;
use http::header::IF_NONE_MATCH;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;

use super::error::parse_error;
use crate::raw::*;
use crate::services::HttpConfig;
use crate::*;

impl Configurator for HttpConfig {
    type Builder = HttpBuilder;
    fn into_builder(self) -> Self::Builder {
        HttpBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// HTTP Read-only service support like [Nginx](https://www.nginx.com/) and [Caddy](https://caddyserver.com/).
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct HttpBuilder {
    config: HttpConfig,
    http_client: Option<HttpClient>,
}

impl Debug for HttpBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("HttpBuilder");

        de.field("config", &self.config).finish()
    }
}

impl HttpBuilder {
    /// Set endpoint for http backend.
    ///
    /// For example: `https://example.com`
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };

        self
    }

    /// set username for http backend
    ///
    /// default: no username
    pub fn username(mut self, username: &str) -> Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_owned());
        }
        self
    }

    /// set password for http backend
    ///
    /// default: no password
    pub fn password(mut self, password: &str) -> Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_owned());
        }
        self
    }

    /// set bearer token for http backend
    ///
    /// default: no access token
    pub fn token(mut self, token: &str) -> Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_string());
        }
        self
    }

    /// Set root path of http backend.
    pub fn root(mut self, root: &str) -> Self {
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
    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for HttpBuilder {
    const SCHEME: Scheme = Scheme::Http;
    type Config = HttpConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let endpoint = match &self.config.endpoint {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                    .with_context("service", Scheme::Http))
            }
        };

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", root);

        let client = if let Some(client) = self.http_client {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Http)
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

        Ok(HttpBackend {
            endpoint: endpoint.to_string(),
            authorization: auth,
            root,
            client,
        })
    }
}

/// Backend is used to serve `Accessor` support for http.
#[derive(Clone)]
pub struct HttpBackend {
    endpoint: String,
    root: String,
    client: HttpClient,

    authorization: Option<String>,
}

impl Debug for HttpBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("client", &self.client)
            .finish()
    }
}

impl Access for HttpBackend {
    type Reader = HttpBody;
    type Writer = ();
    type Lister = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> Arc<AccessorInfo> {
        AccessorInfo::default()
            .with_scheme(Scheme::Http)
            .with_root(&self.root)
            .with_native_capability(Capability {
                stat: true,
                stat_with_if_match: true,
                stat_with_if_none_match: true,

                read: true,

                read_with_if_match: true,
                read_with_if_none_match: true,

                presign: !self.has_authorization(),
                presign_read: !self.has_authorization(),
                presign_stat: !self.has_authorization(),

                ..Default::default()
            })
            .into()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.http_head(path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_metadata(path, resp.headers()).map(RpStat::new),
            // HTTP Server like nginx could return FORBIDDEN if auto-index
            // is not enabled, we should ignore them.
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN if path.ends_with('/') => {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.http_get(path, args.range(), &args).await?;

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

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        if self.has_authorization() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Http doesn't support presigned request on backend with authorization",
            ));
        }

        let req = match args.operation() {
            PresignOperation::Stat(v) => self.http_head_request(path, v)?,
            PresignOperation::Read(v) => self.http_get_request(path, BytesRange::default(), v)?,
            _ => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "Http doesn't support presigned write",
                ))
            }
        };

        let (parts, _) = req.into_parts();

        Ok(RpPresign::new(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        )))
    }
}

impl HttpBackend {
    pub fn has_authorization(&self) -> bool {
        self.authorization.is_some()
    }

    pub fn http_get_request(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Request<Buffer>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::get(&url);

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth.clone())
        }

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }

        req.body(Buffer::new()).map_err(new_request_build_error)
    }

    pub async fn http_get(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let req = self.http_get_request(path, range, args)?;
        self.client.fetch(req).await
    }

    pub fn http_head_request(&self, path: &str, args: &OpStat) -> Result<Request<Buffer>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::head(&url);

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth.clone())
        }

        req.body(Buffer::new()).map_err(new_request_build_error)
    }

    async fn http_head(&self, path: &str, args: &OpStat) -> Result<Response<Buffer>> {
        let req = self.http_head_request(path, args)?;
        self.client.send(req).await
    }
}
