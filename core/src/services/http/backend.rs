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
use http::header;
use http::header::IF_MATCH;
use http::header::IF_NONE_MATCH;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use serde::Deserialize;

use super::error::parse_error;
use crate::raw::*;
use crate::services::http::reader::HttpReader;
use crate::*;

/// Config for Http service support.
#[derive(Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct HttpConfig {
    /// endpoint of this backend
    pub endpoint: Option<String>,
    /// username of this backend
    pub username: Option<String>,
    /// password of this backend
    pub password: Option<String>,
    /// token of this backend
    pub token: Option<String>,
    /// root of this backend
    pub root: Option<String>,
}

impl Debug for HttpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("HttpConfig");
        de.field("endpoint", &self.endpoint);
        de.field("root", &self.root);

        de.finish_non_exhaustive()
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
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
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
    pub fn username(&mut self, username: &str) -> &mut Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_owned());
        }
        self
    }

    /// set password for http backend
    ///
    /// default: no password
    pub fn password(&mut self, password: &str) -> &mut Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_owned());
        }
        self
    }

    /// set bearer token for http backend
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

impl Builder for HttpBuilder {
    const SCHEME: Scheme = Scheme::Http;
    type Accessor = HttpBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let config = HttpConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");

        HttpBuilder {
            config,
            http_client: None,
        }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let endpoint = match &self.config.endpoint {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                    .with_context("service", Scheme::Http))
            }
        };

        let root = normalize_root(&self.config.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let client = if let Some(client) = self.http_client.take() {
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

        debug!("backend build finished: {:?}", &self);
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

#[async_trait]
impl Accessor for HttpBackend {
    type Reader = HttpReader;
    type Writer = ();
    type Lister = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(Scheme::Http)
            .set_root(&self.root)
            .set_native_capability(Capability {
                stat: true,
                stat_with_if_match: true,
                stat_with_if_none_match: true,

                read: true,
                read_can_next: true,
                read_with_range: true,
                read_with_if_match: true,
                read_with_if_none_match: true,

                ..Default::default()
            });

        ma
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
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        Ok((RpRead::default(), HttpReader::new(self.clone(), path, args)))
    }
}

impl HttpBackend {
    pub async fn http_get(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Response<oio::Buffer>> {
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

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    async fn http_head(&self, path: &str, args: &OpStat) -> Result<Response<oio::Buffer>> {
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

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use wiremock::matchers::basic_auth;
    use wiremock::matchers::bearer_token;
    use wiremock::matchers::headers;
    use wiremock::matchers::method;
    use wiremock::matchers::path;
    use wiremock::Mock;
    use wiremock::MockServer;
    use wiremock::ResponseTemplate;

    use super::*;
    use crate::Operator;

    #[tokio::test]
    async fn test_read() -> Result<()> {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/hello"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "13")
                    .set_body_string("Hello, World!"),
            )
            .mount(&mock_server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/hello"))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "13"))
            .mount(&mock_server)
            .await;

        let mut builder = HttpBuilder::default();
        builder.endpoint(&mock_server.uri());
        builder.root("/");
        let op = Operator::new(builder)?.finish();

        let bs = op.read("hello").await?;

        assert_eq!(bs, b"Hello, World!");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_via_basic_auth() -> Result<()> {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let (username, password) = ("your_username", "your_password");

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/hello"))
            .and(basic_auth(username, password))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "13")
                    .set_body_string("Hello, World!"),
            )
            .mount(&mock_server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/hello"))
            .and(basic_auth(username, password))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "13"))
            .mount(&mock_server)
            .await;

        let mut builder = HttpBuilder::default();
        builder.endpoint(&mock_server.uri());
        builder.root("/");
        builder.username(username).password(password);
        let op = Operator::new(builder)?.finish();

        let bs = op.read("hello").await?;

        assert_eq!(bs, b"Hello, World!");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_via_bearer_auth() -> Result<()> {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let token = "your_token";

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/hello"))
            .and(bearer_token(token))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "13")
                    .set_body_string("Hello, World!"),
            )
            .mount(&mock_server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/hello"))
            .and(bearer_token(token))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "13"))
            .mount(&mock_server)
            .await;

        let mut builder = HttpBuilder::default();
        builder.endpoint(&mock_server.uri());
        builder.root("/");
        builder.token(token);
        let op = Operator::new(builder)?.finish();

        let bs = op.read("hello").await?;

        assert_eq!(bs, b"Hello, World!");
        Ok(())
    }

    #[tokio::test]
    async fn test_stat() -> Result<()> {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let mock_server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/hello"))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "128"))
            .mount(&mock_server)
            .await;

        let mut builder = HttpBuilder::default();
        builder.endpoint(&mock_server.uri());
        builder.root("/");
        let op = Operator::new(builder)?.finish();
        let bs = op.stat("hello").await?;

        assert_eq!(bs.mode(), EntryMode::FILE);
        assert_eq!(bs.content_length(), 128);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_with() -> Result<()> {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/hello"))
            .and(headers("if-none-match", vec!["*"]))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "13")
                    .set_body_string("Hello, World!"),
            )
            .mount(&mock_server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/hello"))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "13"))
            .mount(&mock_server)
            .await;

        let mut builder = HttpBuilder::default();
        builder.endpoint(&mock_server.uri());
        builder.root("/");
        let op = Operator::new(builder)?.finish();

        let match_bs = op.read_with("hello").if_none_match("*").await?;
        assert_eq!(match_bs, b"Hello, World!");

        Ok(())
    }

    #[tokio::test]
    async fn test_stat_with() -> Result<()> {
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let mock_server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/hello"))
            .and(headers("if-none-match", vec!["*"]))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "128"))
            .mount(&mock_server)
            .await;

        let mut builder = HttpBuilder::default();
        builder.endpoint(&mock_server.uri());
        builder.root("/");
        let op = Operator::new(builder)?.finish();
        let bs = op.stat_with("hello").if_none_match("*").await?;

        assert_eq!(bs.mode(), EntryMode::FILE);
        assert_eq!(bs.content_length(), 128);
        Ok(())
    }
}
