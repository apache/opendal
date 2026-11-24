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

use std::sync::Arc;

use http::StatusCode;

use super::core::parse_error;
use super::core::{ErrorContext, VercelArtifactsCore};
use super::reader::*;
use super::writer::VercelArtifactsWriter;
use opendal_core::raw::*;
use opendal_core::*;

#[doc = include_str!("docs.md")]
use std::fmt::Debug;

use super::VERCEL_ARTIFACTS_SCHEME;
use super::config::VercelArtifactsConfig;

/// [Vercel Cache](https://vercel.com/docs/concepts/monorepos/remote-caching) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct VercelArtifactsBuilder {
    pub(super) config: VercelArtifactsConfig,
}

impl Debug for VercelArtifactsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VercelArtifactsBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl VercelArtifactsBuilder {
    /// set the bearer access token for Vercel
    ///
    /// default: no access token, which leads to failure
    pub fn access_token(mut self, access_token: &str) -> Self {
        self.config.access_token = if access_token.is_empty() {
            None
        } else {
            Some(access_token.to_string())
        };
        self
    }

    /// Set the endpoint for the Vercel artifacts API.
    ///
    /// Default: `https://api.vercel.com`
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }
        self
    }

    /// Set the Vercel team ID.
    ///
    /// When set, the `teamId` query parameter is appended to all requests.
    pub fn team_id(mut self, team_id: &str) -> Self {
        if !team_id.is_empty() {
            self.config.team_id = Some(team_id.to_string());
        }
        self
    }

    /// Set the Vercel team slug.
    ///
    /// When set, the `slug` query parameter is appended to all requests.
    pub fn team_slug(mut self, team_slug: &str) -> Self {
        if !team_slug.is_empty() {
            self.config.team_slug = Some(team_slug.to_string());
        }
        self
    }
}

impl Builder for VercelArtifactsBuilder {
    type Config = VercelArtifactsConfig;

    fn build(self) -> Result<impl Service> {
        let info = ServiceInfo::new(VERCEL_ARTIFACTS_SCHEME, "", "");
        let capability = Capability {
            stat: true,

            read: true,
            read_with_suffix: true,

            write: true,

            shared: true,

            ..Default::default()
        };

        let access_token = self
            .config
            .access_token
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                Error::new(ErrorKind::ConfigInvalid, "access_token not set")
                    .with_operation("Builder::build")
                    .with_context("service", VERCEL_ARTIFACTS_SCHEME)
            })?;

        let auth_header = format_authorization_by_bearer(&access_token)?;
        let auth_header = build_header_value(&auth_header).map_err(|err| {
            err.with_operation("Builder::build")
                .with_context("service", VERCEL_ARTIFACTS_SCHEME)
                .with_context("key", "access_token")
        })?;
        auth_header.to_str().map_err(|err| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "access_token contains non-ASCII characters",
            )
            .with_operation("Builder::build")
            .with_context("service", VERCEL_ARTIFACTS_SCHEME)
            .with_context("key", "access_token")
            .set_source(err)
        })?;

        let endpoint = self
            .config
            .endpoint
            .unwrap_or_else(|| "https://api.vercel.com".to_string());

        let mut query = QueryPairsWriter::new("");
        if let Some(team_id) = self.config.team_id.as_deref().filter(|v| !v.is_empty()) {
            query = query.push("teamId", &percent_encode_path(team_id));
        }
        if let Some(slug) = self.config.team_slug.as_deref().filter(|v| !v.is_empty()) {
            query = query.push("slug", &percent_encode_path(slug));
        }
        let query_string = query.finish();

        Ok(VercelArtifactsBackend {
            core: Arc::new(VercelArtifactsCore {
                info,
                capability,
                access_token,
                endpoint,
                query_string,
            }),
        })
    }
}

#[derive(Clone, Debug)]
pub struct VercelArtifactsBackend {
    pub core: Arc<VercelArtifactsCore>,
}

impl Service for VercelArtifactsBackend {
    type Reader = oio::StreamReader<VercelArtifactsReader>;
    type Writer = oio::OneShotWriter<VercelArtifactsWriter>;
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

    async fn stat(&self, ctx: &OperationContext, path: &str, _args: OpStat) -> Result<RpStat> {
        let response = self.core.vercel_artifacts_stat(ctx, path).await?;

        let status = response.status();

        match status {
            StatusCode::OK => {
                let meta = parse_into_metadata(path, response.headers())?;
                Ok(RpStat::new(meta))
            }

            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("HeadArtifact")),
                response,
            )),
        }
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<VercelArtifactsReader> = {
            Ok(oio::StreamReader::new(VercelArtifactsReader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let output: oio::OneShotWriter<VercelArtifactsWriter> = {
            Ok(oio::OneShotWriter::new(VercelArtifactsWriter::new(
                self.core.clone(),
                ctx.clone(),
                args,
                path.to_string(),
            )))
        }?;

        Ok(output)
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
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::stream;
    use http::{Request, Response, StatusCode};

    use super::*;

    #[derive(Clone, Default)]
    struct RecordingTransport {
        uris: Arc<Mutex<Vec<String>>>,
    }

    impl HttpTransport for RecordingTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.uris.lock().unwrap().push(req.uri().to_string());
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_LENGTH, "4")
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from("data"))]),
                    Some(4),
                ))
                .unwrap())
        }
    }

    #[test]
    fn build_rejects_empty_and_invalid_access_token() {
        let err = VercelArtifactsBuilder::default()
            .build()
            .expect_err("missing access_token must fail");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        let err = VercelArtifactsBuilder::default()
            .access_token("")
            .build()
            .expect_err("empty builder access_token must fail");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        let cfg = VercelArtifactsConfig::from_iter([("access_token".to_string(), "".to_string())])
            .unwrap();
        let err = cfg
            .into_builder()
            .build()
            .expect_err("empty config access_token must fail");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        for invalid in ["bad\r\ntoken", "bad\ntoken", "tok\u{00e9}n"] {
            let err = VercelArtifactsBuilder::default()
                .access_token(invalid)
                .build()
                .expect_err("invalid header characters in access_token must fail");
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }
    }

    #[tokio::test]
    async fn build_encodes_team_query_params() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let cfg = VercelArtifactsConfig::from_iter(
                [
                    ("access_token", "secret-token"),
                    ("endpoint", "https://cache.example.com"),
                    ("team_id", "team&id=1#frag"),
                    ("team_slug", "my team/slug?x=1"),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string())),
            )
            .unwrap();

            let transport = RecordingTransport::default();
            let op = Operator::new(cfg.into_builder())
                .unwrap()
                .with_context(
                    OperationContext::new()
                        .with_http_transport(HttpTransporter::new(transport.clone())),
                );

            let meta = op.stat("abc123").await.unwrap();
            assert_eq!(meta.content_length(), 4);

            let uris = transport.uris.lock().unwrap().clone();
            assert_eq!(
                uris,
                vec![
                    "https://cache.example.com/v8/artifacts/abc123?teamId=team%26id%3D1%23frag&slug=my%20team/slug%3Fx%3D1"
                ]
            );
        })
        .await
        .expect("test timed out");
    }
}
