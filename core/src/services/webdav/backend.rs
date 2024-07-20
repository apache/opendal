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
use std::str::FromStr;
use std::sync::Arc;

use http::Response;
use http::StatusCode;
use log::debug;
use serde::{Deserialize, Serialize};

use super::core::*;
use super::error::parse_error;
use super::lister::WebdavLister;
use super::writer::WebdavWriter;
use crate::raw::*;
use crate::*;

/// Config for [WebDAV](https://datatracker.ietf.org/doc/html/rfc4918) backend support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct WebdavConfig {
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
    /// WebDAV Service doesn't support copy.
    pub disable_copy: bool,
}

impl Debug for WebdavConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("WebdavConfig");

        d.field("endpoint", &self.endpoint)
            .field("username", &self.username)
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

    /// set the username for Webdav
    ///
    /// default: no username
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
    type Config = WebdavConfig;

    fn from_config(config: Self::Config) -> Self {
        WebdavBuilder {
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
                    .with_context("service", Scheme::Webdav));
            }
        };
        // Some services might return the path with suffix `/remote.php/webdav/`, we need to trim them.
        let server_path = http::Uri::from_str(endpoint)
            .map_err(|err| {
                Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                    .with_context("service", Scheme::Webdav)
                    .set_source(err)
            })?
            .path()
            .trim_end_matches('/')
            .to_string();

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", root);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Webdav)
            })?
        };

        let mut authorization = None;
        if let Some(username) = &self.config.username {
            authorization = Some(format_authorization_by_basic(
                username,
                self.config.password.as_deref().unwrap_or_default(),
            )?);
        }
        if let Some(token) = &self.config.token {
            authorization = Some(format_authorization_by_bearer(token)?)
        }

        debug!("backend build finished: {:?}", &self);

        let core = Arc::new(WebdavCore {
            endpoint: endpoint.to_string(),
            server_path,
            authorization,
            disable_copy: self.config.disable_copy,
            root,
            client,
        });
        Ok(WebdavBackend { core })
    }
}

/// Backend is used to serve `Accessor` support for http.
#[derive(Clone)]
pub struct WebdavBackend {
    core: Arc<WebdavCore>,
}

impl Debug for WebdavBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebdavBackend")
            .field("core", &self.core)
            .finish()
    }
}

impl Access for WebdavBackend {
    type Reader = HttpBody;
    type Writer = oio::OneShotWriter<WebdavWriter>;
    type Lister = oio::PageLister<WebdavLister>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(Scheme::Webdav)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                write: true,
                write_can_empty: true,

                create_dir: true,
                delete: true,

                copy: !self.core.disable_copy,

                rename: true,

                list: true,
                // We already support recursive list but some details still need to polish.
                // list_with_recursive: true,
                ..Default::default()
            });

        ma.into()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.core.webdav_mkcol(path).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let metadata = self.core.webdav_stat(path).await?;
        Ok(RpStat::new(metadata))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.webdav_get(path, args.range(), &args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                Ok((RpRead::default(), resp.into_body()))
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)).await?)
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        // Ensure parent path exists
        self.core.webdav_mkcol(get_parent(path)).await?;

        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(WebdavWriter::new(self.core.clone(), args, path.to_string())),
        ))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.webdav_delete(path).await?;

        let status = resp.status();
        match status {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        Ok((
            RpList::default(),
            oio::PageLister::new(WebdavLister::new(self.core.clone(), path, args)),
        ))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.webdav_copy(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::NO_CONTENT => Ok(RpCopy::default()),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        let resp = self.core.webdav_move(from, to).await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::NO_CONTENT | StatusCode::OK => {
                Ok(RpRename::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}
