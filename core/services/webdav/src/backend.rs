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
use std::str::FromStr;
use std::sync::Arc;

use http::Response;
use http::StatusCode;
use log::debug;

use super::WEBDAV_SCHEME;
use super::config::WebdavConfig;
use super::core::*;
use super::deleter::WebdavDeleter;
use super::error::parse_error;
use super::lister::WebdavLister;
use super::writer::WebdavWriter;
use opendal_core::raw::oio;
use opendal_core::raw::*;
use opendal_core::*;

/// [WebDAV](https://datatracker.ietf.org/doc/html/rfc4918) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct WebdavBuilder {
    pub(super) config: WebdavConfig,
}

impl Debug for WebdavBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebdavBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl WebdavBuilder {
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

    /// set the username for Webdav
    ///
    /// default: no username
    pub fn username(mut self, username: &str) -> Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_owned());
        }
        self
    }

    /// set the password for Webdav
    ///
    /// default: no password
    pub fn password(mut self, password: &str) -> Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_owned());
        }
        self
    }

    /// set the bearer token for Webdav
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

    /// Disable automatic parent directory creation before write operations.
    ///
    /// By default, OpenDAL creates parent directories using MKCOL before writing files.
    /// This requires PROPFIND support to check directory existence.
    ///
    /// Some WebDAV-compatible servers (e.g., bazel-remote) don't support PROPFIND
    /// or don't require explicit directory creation. Enable this option to skip
    /// the MKCOL calls and write files directly.
    ///
    /// Default: false
    pub fn disable_create_dir(mut self, disable: bool) -> Self {
        self.config.disable_create_dir = disable;
        self
    }

    /// Deprecated: WebDAV user metadata capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "WebDAV user metadata capability is enabled by default. Use CapabilityOverrideLayer to override write_with_user_metadata for endpoints without PROPPATCH support."
    )]
    pub fn enable_user_metadata(self, _enable: bool) -> Self {
        self
    }

    /// Set the XML namespace prefix for user metadata properties.
    ///
    /// This prefix is used in PROPPATCH/PROPFIND XML requests.
    /// Different servers may require different prefixes.
    ///
    /// Default: "opendal"
    pub fn user_metadata_prefix(mut self, prefix: &str) -> Self {
        if !prefix.is_empty() {
            self.config.user_metadata_prefix = Some(prefix.to_string());
        }
        self
    }

    /// Set the XML namespace URI for user metadata properties.
    ///
    /// This URI uniquely identifies the namespace for custom properties.
    /// Different servers may require different namespace URIs.
    ///
    /// Default: `https://opendal.apache.org/ns`
    pub fn user_metadata_uri(mut self, uri: &str) -> Self {
        if !uri.is_empty() {
            self.config.user_metadata_uri = Some(uri.to_string());
        }
        self
    }
}

impl Builder for WebdavBuilder {
    type Config = WebdavConfig;

    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", &self);

        let endpoint = match &self.config.endpoint {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                    .with_context("service", WEBDAV_SCHEME));
            }
        };
        // Some services might return the path with suffix `/remote.php/webdav/`, we need to trim them.
        let server_path = http::Uri::from_str(endpoint)
            .map_err(|err| {
                Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                    .with_context("service", WEBDAV_SCHEME)
                    .set_source(err)
            })?
            .path()
            .trim_end_matches('/')
            .to_string();

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {root}");

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

        let core = Arc::new(WebdavCore {
            info: ServiceInfo::new(WEBDAV_SCHEME, &root, ""),
            capability: Capability {
                stat: true,

                read: true,
                read_with_suffix: true,

                write: true,
                write_can_empty: true,
                write_with_user_metadata: true,

                create_dir: true,
                delete: true,

                copy: true,

                rename: true,

                list: true,

                // We already support recursive list but some details still need to polish.
                // list_with_recursive: true,
                shared: true,

                ..Default::default()
            },
            endpoint: endpoint.to_string(),
            server_path,
            authorization,
            root,
            user_metadata_prefix: self
                .config
                .user_metadata_prefix
                .unwrap_or_else(|| DEFAULT_USER_METADATA_PREFIX.to_string()),
            user_metadata_uri: self
                .config
                .user_metadata_uri
                .unwrap_or_else(|| DEFAULT_USER_METADATA_URI.to_string()),
            disable_create_dir: self.config.disable_create_dir,
        });
        Ok(WebdavBackend { core })
    }
}

#[derive(Clone, Debug)]
pub struct WebdavBackend {
    core: Arc<WebdavCore>,
}

/// Reader returned by this backend.
pub struct WebdavReader {
    backend: WebdavBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl WebdavReader {
    fn new(backend: WebdavBackend, ctx: OperationContext, path: &str, args: OpRead) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::StreamRead for WebdavReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend
            .core
            .webdav_get(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();

        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                RpRead::new(parse_into_metadata(path, resp.headers())?),
                resp.into_body(),
            ),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(Response::from_parts(part, buf)));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

impl Service for WebdavBackend {
    type Reader = oio::StreamReader<WebdavReader>;
    type Writer = oio::OneShotWriter<WebdavWriter>;
    type Lister = oio::PageLister<WebdavLister>;
    type Deleter = oio::OneShotDeleter<WebdavDeleter>;
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.core.webdav_mkcol(ctx, path).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let metadata = self.core.webdav_stat(ctx, path).await?;
        Ok(RpStat::new(metadata))
    }
    async fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        let (rp, output): (_, oio::StreamReader<WebdavReader>) = {
            Ok((
                RpRead::default(),
                oio::StreamReader::new(WebdavReader::new(self.clone(), ctx.clone(), path, args)),
            ))
        }?;

        Ok((rp, output))
    }

    async fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        let (rp, output): (_, oio::OneShotWriter<WebdavWriter>) = {
            // Ensure parent path exists (unless disabled for servers that don't support PROPFIND)
            if !self.core.disable_create_dir {
                self.core.webdav_mkcol(ctx, get_parent(path)).await?;
            }

            Ok((
                RpWrite::default(),
                oio::OneShotWriter::new(WebdavWriter::new(
                    self.core.clone(),
                    ctx.clone(),
                    args,
                    path.to_string(),
                )),
            ))
        }?;

        Ok((rp, output))
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, output): (_, oio::OneShotDeleter<WebdavDeleter>) = {
            Ok((
                RpDelete::default(),
                oio::OneShotDeleter::new(WebdavDeleter::new(self.core.clone(), ctx.clone())),
            ))
        }?;

        Ok((rp, output))
    }

    async fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        let (rp, output): (_, oio::PageLister<WebdavLister>) = {
            Ok((
                RpList::default(),
                oio::PageLister::new(WebdavLister::new(
                    self.core.clone(),
                    ctx.clone(),
                    path,
                    args,
                )),
            ))
        }?;

        Ok((rp, output))
    }

    async fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
        let (rp, output): (_, ()) = {
            let resp = self.core.webdav_copy(ctx, from, to).await?;

            let status = resp.status();

            match status {
                StatusCode::CREATED | StatusCode::NO_CONTENT => Ok((RpCopy::default(), ())),
                _ => Err(parse_error(resp)),
            }
        }?;

        Ok((rp, output))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        let resp = self.core.webdav_move(ctx, from, to).await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::NO_CONTENT | StatusCode::OK => {
                Ok(RpRename::default())
            }
            _ => Err(parse_error(resp)),
        }
    }
}
