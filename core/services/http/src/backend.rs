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
use std::sync::Arc;

use http::StatusCode;
use log::debug;

use super::HTTP_SCHEME;
use super::config::HttpConfig;
use super::core::{ErrorContext, HttpCore, parse_error};
use super::reader::*;
use opendal_core::raw::*;
use opendal_core::*;

/// HTTP Read-only service support like [Nginx](https://www.nginx.com/) and [Caddy](https://caddyserver.com/).
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct HttpBuilder {
    pub(super) config: HttpConfig,
}

impl Debug for HttpBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
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
}

impl Builder for HttpBuilder {
    type Config = HttpConfig;

    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", self);

        let endpoint = match &self.config.endpoint {
            Some(v) if !v.is_empty() => v,
            _ => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                    .with_context("service", HTTP_SCHEME));
            }
        };

        let uri = endpoint.parse::<http::Uri>().map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
                .with_context("service", HTTP_SCHEME)
                .set_source(err)
        })?;
        let is_http_scheme = matches!(
            uri.scheme(),
            Some(scheme)
                if *scheme == http::uri::Scheme::HTTP || *scheme == http::uri::Scheme::HTTPS
        );
        if !is_http_scheme || uri.host().is_none_or(str::is_empty) {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "endpoint must be a valid http or https URI with a host",
            )
            .with_context("service", HTTP_SCHEME));
        }
        if uri.query().is_some() || endpoint.contains('#') {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "endpoint must not contain a query or fragment",
            )
            .with_context("service", HTTP_SCHEME));
        }

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

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

        let info = ServiceInfo::new(HTTP_SCHEME, &root, "");
        let capability = Capability {
            stat: true,
            stat_with_if_match: true,
            stat_with_if_none_match: true,
            stat_with_if_modified_since: true,
            stat_with_if_unmodified_since: true,

            read: true,
            read_with_suffix: true,

            read_with_if_match: true,
            read_with_if_none_match: true,
            read_with_if_modified_since: true,
            read_with_if_unmodified_since: true,

            presign: auth.is_none(),
            presign_read: auth.is_none(),
            presign_stat: auth.is_none(),

            shared: true,

            ..Default::default()
        };

        let accessor_info = info;

        let core = Arc::new(HttpCore {
            info: accessor_info,
            capability,
            endpoint: endpoint.to_string(),
            root,
            authorization: auth,
        });

        Ok(HttpBackend { core })
    }
}

/// HttpBackend implements [`Service`] for read-only HTTP directory listings and files.
#[derive(Clone, Debug)]
pub struct HttpBackend {
    pub(crate) core: Arc<HttpCore>,
}

impl Service for HttpBackend {
    type Reader = oio::StreamReader<HttpReader>;
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(MetadataBuilder::dir().build()));
        }

        let resp = self.core.http_head(ctx, path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => parse_into_metadata(path, resp.headers()).map(RpStat::new),
            // HTTP Server like nginx could return FORBIDDEN if auto-index
            // is not enabled, we should ignore them.
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN if path.ends_with('/') => {
                Ok(RpStat::new(MetadataBuilder::dir().build()))
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Head")),
                resp,
            )),
        }
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<HttpReader> = {
            Ok(oio::StreamReader::new(HttpReader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, _ctx: &OperationContext, _path: &str, _args: OpWrite) -> Result<Self::Writer> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    fn list(&self, _ctx: &OperationContext, _path: &str, _args: OpList) -> Result<Self::Lister> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        if self.core.has_authorization() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Http doesn't support presigned request on backend with authorization",
            ));
        }

        let req = match args.operation() {
            PresignOperation::Stat(v) => self.core.http_head_request(path, v)?,
            PresignOperation::Read(range, v) => self.core.http_get_request(path, *range, v)?,
            _ => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "Http doesn't support presigned write",
                ));
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use http::Method;
    use http::Request;
    use http::Response;
    use http::Uri;
    use http::header;

    use super::*;

    #[derive(Debug)]
    struct RecordedRequest {
        uri: Uri,
        authorization: Option<String>,
    }

    #[derive(Debug, Default)]
    struct RecordingTransport {
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    impl HttpTransport for RecordingTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let authorization = req
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            self.requests
                .lock()
                .expect("requests lock must not be poisoned")
                .push(RecordedRequest {
                    uri: req.uri().clone(),
                    authorization,
                });

            let body = if req.method() == Method::HEAD {
                HttpBody::new(futures::stream::empty(), Some(0))
            } else {
                HttpBody::new(futures::stream::iter([Ok(Buffer::from("hello"))]), Some(5))
            };

            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_LENGTH, "5")
                .body(body)
                .expect("response must build"))
        }
    }

    #[test]
    fn build_rejects_invalid_endpoint() {
        let missing_err =
            Operator::new(HttpBuilder::default()).expect_err("missing endpoint must fail");
        assert_eq!(missing_err.kind(), ErrorKind::ConfigInvalid);

        let empty_cfg = HttpConfig {
            endpoint: Some(String::new()),
            ..Default::default()
        };
        let empty_err =
            Operator::new(empty_cfg.into_builder()).expect_err("empty endpoint must fail");
        assert_eq!(empty_err.kind(), ErrorKind::ConfigInvalid);

        let invalid_endpoints = [
            "example.com",
            "127.0.0.1",
            "127.0.0.1:8080",
            "ftp://example.com",
            "file:///tmp",
            "https://",
            "http:///path",
            "https://:8080",
            "https://example.com/bad path",
            "https://example.com#frag",
            "https://example.com/prefix#frag",
            "https://example.com?foo=bar",
            "https://example.com/prefix?foo=bar",
        ];

        for endpoint in invalid_endpoints {
            let err = Operator::new(HttpBuilder::default().endpoint(endpoint))
                .expect_err(&format!("invalid endpoint `{endpoint}` must be rejected"));
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid, "endpoint={endpoint}");
        }
    }

    #[tokio::test]
    async fn from_uri_credentials_send_basic_auth_and_disable_presign() -> Result<()> {
        crate::register_http_service(OperatorRegistry::get());

        let requests = Arc::new(Mutex::new(Vec::new()));
        let op = Operator::from_uri("https://alice:secret@example.com/static")?.with_context(
            OperationContext::new().with_http_transport(HttpTransporter::new(RecordingTransport {
                requests: requests.clone(),
            })),
        );

        let cap = op.info().capability();
        assert!(!cap.presign);
        assert!(!cap.presign_read);
        assert!(!cap.presign_stat);

        let presign_err = op
            .presign_read("file.txt", Duration::from_secs(60))
            .await
            .expect_err("presign must be rejected when URI carries credentials");
        assert_eq!(presign_err.kind(), ErrorKind::Unsupported);

        op.stat("file.txt").await?;
        let data = op.read("file.txt").await?;
        assert_eq!(data.to_bytes(), b"hello".as_slice());

        let expected_auth = format_authorization_by_basic("alice", "secret")?;
        let recorded = requests.lock().expect("requests lock must not be poisoned");
        assert_eq!(recorded.len(), 2);
        for req in recorded.iter() {
            assert_eq!(req.uri.to_string(), "https://example.com/static/file.txt");
            assert_eq!(req.authorization.as_deref(), Some(expected_auth.as_str()));
        }

        Ok(())
    }
}
