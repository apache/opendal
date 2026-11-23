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

use http::StatusCode;
use log::debug;

use super::WEBDAV_SCHEME;
use super::config::WebdavConfig;
use super::core::parse_error;
use super::core::*;
use super::deleter::WebdavDeleter;
use super::lister::WebdavLister;
use super::reader::*;
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

    /// Disable automatic parent directory creation before write, copy, and rename operations.
    ///
    /// By default, OpenDAL creates missing parent directories using `PROPFIND` and `MKCOL`
    /// before writing, copying, or renaming files. Enable this option to skip those automatic
    /// parent directory checks and `MKCOL` calls for servers that do not support `PROPFIND`
    /// or do not require explicit parent collections (for example, `bazel-remote`).
    ///
    /// Explicit `create_dir` calls are unaffected.
    ///
    /// Default: `false`
    pub fn disable_create_dir(mut self, disable: bool) -> Self {
        self.config.disable_create_dir = disable;
        self
    }

    /// Disable native recursive listing with WebDAV's `Depth: infinity`.
    ///
    /// Set this to `true` for servers that reject or ignore infinity depth.
    /// With capability simulation enabled, OpenDAL walks the directory tree
    /// using `Depth: 1` requests instead.
    ///
    /// The default is `false`.
    pub fn disable_depth_infinity(mut self, disable: bool) -> Self {
        self.config.disable_depth_infinity = disable;
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

    /// Deprecated: WebDAV conditional read capabilities are enabled by default.
    #[deprecated(
        since = "0.60.0",
        note = "WebDAV conditional read capabilities are enabled by default. Use CapabilityOverrideLayer to override read_with_if_match, read_with_if_none_match, read_with_if_modified_since and read_with_if_unmodified_since for endpoints without ETag support."
    )]
    pub fn enable_conditional_read(self, _enable: bool) -> Self {
        self
    }
}

impl Builder for WebdavBuilder {
    type Config = WebdavConfig;

    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", self);

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
                read_with_if_match: true,
                read_with_if_none_match: true,
                read_with_if_modified_since: true,
                read_with_if_unmodified_since: true,

                write: true,
                write_can_empty: true,
                write_with_if_match: true,
                write_with_if_none_match: true,
                write_with_if_not_exists: true,
                write_with_user_metadata: true,

                create_dir: true,
                delete: true,
                delete_with_recursive: true,
                delete_with_if_match: true,

                copy: true,

                rename: true,
                rename_with_if_not_exists: true,

                list: true,
                list_with_recursive: !self.config.disable_depth_infinity,

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
    pub(crate) core: Arc<WebdavCore>,
}

impl Service for WebdavBackend {
    type Reader = oio::StreamReader<WebdavReader>;
    type Writer = oio::OneShotWriter<WebdavWriter>;
    type Lister = oio::PageLister<WebdavLister>;
    type Deleter = oio::OneShotDeleter<WebdavDeleter>;
    type Copier = oio::OneShotCopier;
    type Composer = ();

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
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<WebdavReader> = {
            Ok(oio::StreamReader::new(WebdavReader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        if args.if_version_match().is_some() || args.if_version_not_match().is_some() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "WebDAV does not support write version conditions",
            ));
        }

        Ok(oio::OneShotWriter::new(WebdavWriter::new(
            self.core.clone(),
            ctx.clone(),
            args,
            path.to_string(),
        )))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<WebdavDeleter> = {
            Ok(oio::OneShotDeleter::new(WebdavDeleter::new(
                self.core.clone(),
                ctx.clone(),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        if args.recursive() && !self.core.capability.list_with_recursive {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "WebDAV recursive listing with Depth: infinity is disabled",
            ));
        }

        Ok(oio::PageLister::new(WebdavLister::new(
            self.core.clone(),
            ctx.clone(),
            path,
            args,
        )))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        let backend = self.clone();
        let core = self.core.clone();
        let ctx = ctx.clone();
        let from = from.to_string();
        let to = to.to_string();
        let source_content_length_hint = args.source_content_length_hint();

        Ok(oio::OneShotCopier::new_with(move || {
            let backend = backend.clone();
            let core = core.clone();
            let ctx = ctx.clone();
            let from = from.clone();
            let to = to.clone();

            async move {
                let source_size = match source_content_length_hint {
                    Some(size) => size,
                    None => backend
                        .stat(&ctx, &from, OpStat::default())
                        .await?
                        .into_metadata()
                        .content_length(),
                };

                let resp = core.webdav_copy(&ctx, &from, &to).await?;
                let status = resp.status();

                match status {
                    StatusCode::CREATED | StatusCode::NO_CONTENT => {
                        Ok(MetadataBuilder::file(source_size).build())
                    }
                    _ => Err(parse_error(
                        ErrorContext::new(ServiceOperation("Copy")),
                        resp,
                    )),
                }
            }
        }))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.core.webdav_stat(ctx, from).await?;
        if !self.core.disable_create_dir {
            self.core.webdav_mkcol(ctx, get_parent(to)).await?;
        }
        let resp = self
            .core
            .webdav_move(ctx, from, to, args.if_not_exists())
            .await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::NO_CONTENT | StatusCode::OK => {
                Ok(RpRename::default())
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Move")),
                resp,
            )),
        }
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use bytes::Bytes;
    use futures::stream;
    use http::Request;
    use http::Response;

    use super::*;

    const FILE_STAT_RESPONSE: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/src.txt</D:href>
    <D:propstat>
      <D:prop>
        <D:getcontentlength>5</D:getcontentlength>
        <D:getlastmodified>Sat, 07 May 2022 05:52:22 GMT</D:getlastmodified>
        <D:resourcetype/>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"#;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RecordedRequest {
        method: String,
        path: String,
        destination: Option<String>,
    }

    #[derive(Clone, Default)]
    struct RecordingHttpTransport {
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    impl RecordingHttpTransport {
        fn requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().unwrap().clone()
        }

        fn methods_and_paths(&self) -> Vec<(String, String)> {
            self.requests()
                .into_iter()
                .map(|req| (req.method, req.path))
                .collect()
        }
    }

    impl HttpTransport for RecordingHttpTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let method = req.method().as_str().to_string();
            let path = req.uri().path().to_string();
            let destination = req
                .headers()
                .get("Destination")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);

            self.requests.lock().unwrap().push(RecordedRequest {
                method: method.clone(),
                path: path.clone(),
                destination,
            });

            let (status, body) = match (method.as_str(), path.as_str()) {
                ("PROPFIND", "/src.txt") => (StatusCode::MULTI_STATUS, FILE_STAT_RESPONSE),
                ("PROPFIND", _) => (StatusCode::NOT_FOUND, ""),
                ("MKCOL", _) | ("COPY", _) | ("MOVE", _) | ("PUT", _) => (StatusCode::CREATED, ""),
                _ => (StatusCode::METHOD_NOT_ALLOWED, ""),
            };

            let data = Bytes::from_static(body.as_bytes());
            let size = data.len() as u64;
            Ok(Response::builder()
                .status(status)
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from(data))]),
                    Some(size),
                ))
                .unwrap())
        }
    }

    fn test_operator(disable_create_dir: bool) -> Result<(Operator, RecordingHttpTransport)> {
        let transport = RecordingHttpTransport::default();
        let op = Operator::new(
            WebdavBuilder::default()
                .endpoint("http://localhost:8080")
                .disable_create_dir(disable_create_dir),
        )?
        .with_context(
            OperationContext::new().with_http_transport(HttpTransporter::new(transport.clone())),
        );
        Ok((op, transport))
    }

    #[test]
    fn depth_infinity_is_enabled_by_default() -> Result<()> {
        let service = WebdavBuilder::default()
            .endpoint("http://localhost:8080")
            .build()?;
        let args = options::ListOptions {
            recursive: true,
            ..Default::default()
        };

        assert!(service.capability().list_with_recursive);
        assert!(service.capability().delete_with_recursive);
        assert!(
            service
                .list(&OperationContext::default(), "/", args.into())
                .is_ok()
        );
        Ok(())
    }

    #[test]
    fn disabling_depth_infinity_rejects_native_recursive_list() -> Result<()> {
        let service = WebdavBuilder::default()
            .endpoint("http://localhost:8080")
            .disable_depth_infinity(true)
            .build()?;
        let ctx = OperationContext::default();
        let args = options::ListOptions {
            recursive: true,
            ..Default::default()
        };

        assert!(!service.capability().list_with_recursive);
        // `disable_depth_infinity` only controls recursive `PROPFIND`;
        // collection `DELETE` stays recursive per RFC 4918 §9.6.1.
        assert!(service.capability().delete_with_recursive);
        let err = service.list(&ctx, "/", args.into()).err().unwrap();
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        assert!(service.list(&ctx, "/", OpList::default()).is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn copy_creates_parent_dirs_by_default() -> Result<()> {
        let (op, transport) = test_operator(false)?;

        op.copy("src.txt", "nested/dir/dst.txt").await?;

        assert_eq!(
            transport.methods_and_paths(),
            vec![
                ("PROPFIND".to_string(), "/src.txt".to_string()),
                ("PROPFIND".to_string(), "/nested/dir/".to_string()),
                ("PROPFIND".to_string(), "/nested/".to_string()),
                ("MKCOL".to_string(), "/nested/".to_string()),
                ("MKCOL".to_string(), "/nested/dir/".to_string()),
                ("COPY".to_string(), "/src.txt".to_string()),
            ]
        );
        assert_eq!(
            transport.requests().last().unwrap().destination.as_deref(),
            Some("http://localhost:8080/nested/dir/dst.txt")
        );
        Ok(())
    }

    #[tokio::test]
    async fn copy_skips_parent_dir_creation_when_disabled() -> Result<()> {
        let (op, transport) = test_operator(true)?;

        op.copy("src.txt", "nested/dir/dst.txt").await?;

        assert_eq!(
            transport.methods_and_paths(),
            vec![
                ("PROPFIND".to_string(), "/src.txt".to_string()),
                ("COPY".to_string(), "/src.txt".to_string()),
            ]
        );
        assert_eq!(
            transport.requests().last().unwrap().destination.as_deref(),
            Some("http://localhost:8080/nested/dir/dst.txt")
        );
        Ok(())
    }

    #[tokio::test]
    async fn rename_creates_parent_dirs_by_default() -> Result<()> {
        let (op, transport) = test_operator(false)?;

        op.rename("src.txt", "nested/dir/dst.txt").await?;

        assert_eq!(
            transport.methods_and_paths(),
            vec![
                ("PROPFIND".to_string(), "/src.txt".to_string()),
                ("PROPFIND".to_string(), "/nested/dir/".to_string()),
                ("PROPFIND".to_string(), "/nested/".to_string()),
                ("MKCOL".to_string(), "/nested/".to_string()),
                ("MKCOL".to_string(), "/nested/dir/".to_string()),
                ("MOVE".to_string(), "/src.txt".to_string()),
            ]
        );
        assert_eq!(
            transport.requests().last().unwrap().destination.as_deref(),
            Some("http://localhost:8080/nested/dir/dst.txt")
        );
        Ok(())
    }

    #[tokio::test]
    async fn rename_skips_parent_dir_creation_when_disabled() -> Result<()> {
        let (op, transport) = test_operator(true)?;

        op.rename("src.txt", "nested/dir/dst.txt").await?;

        assert_eq!(
            transport.methods_and_paths(),
            vec![
                ("PROPFIND".to_string(), "/src.txt".to_string()),
                ("MOVE".to_string(), "/src.txt".to_string()),
            ]
        );
        assert_eq!(
            transport.requests().last().unwrap().destination.as_deref(),
            Some("http://localhost:8080/nested/dir/dst.txt")
        );
        Ok(())
    }

    #[tokio::test]
    async fn write_and_explicit_create_dir_respect_disable_create_dir() -> Result<()> {
        let (op, transport) = test_operator(true)?;
        assert!(op.info().capability().create_dir);

        op.write("nested/dir/dst.txt", "hello").await?;
        assert_eq!(
            transport.methods_and_paths(),
            vec![("PUT".to_string(), "/nested/dir/dst.txt".to_string())]
        );

        op.create_dir("nested/dir/").await?;
        assert_eq!(
            transport.methods_and_paths(),
            vec![
                ("PUT".to_string(), "/nested/dir/dst.txt".to_string()),
                ("PROPFIND".to_string(), "/nested/dir/".to_string()),
                ("PROPFIND".to_string(), "/nested/".to_string()),
                ("MKCOL".to_string(), "/nested/".to_string()),
                ("MKCOL".to_string(), "/nested/dir/".to_string()),
            ]
        );
        Ok(())
    }
}
