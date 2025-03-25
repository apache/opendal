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

use std::env;
use std::sync::Arc;

use super::core::*;
use super::error::parse_error;
use super::writer::GhacWriter;
use crate::raw::*;
use crate::services::ghac::core::GhacCore;
use crate::services::GhacConfig;
use crate::*;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use sha2::Digest;

fn value_or_env(
    explicit_value: Option<String>,
    env_var_name: &str,
    operation: &'static str,
) -> Result<String> {
    if let Some(value) = explicit_value {
        return Ok(value);
    }

    env::var(env_var_name).map_err(|err| {
        let text = format!(
            "{} not found, maybe not in github action environment?",
            env_var_name
        );
        Error::new(ErrorKind::ConfigInvalid, text)
            .with_operation(operation)
            .set_source(err)
    })
}

impl Configurator for GhacConfig {
    type Builder = GhacBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        GhacBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// GitHub Action Cache Services support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct GhacBuilder {
    config: GhacConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl GhacBuilder {
    /// set the working directory root of backend
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// set the version that used by cache.
    ///
    /// The version is the unique value that provides namespacing.
    /// It's better to make sure this value is only used by this backend.
    ///
    /// If not set, we will use `opendal` as default.
    pub fn version(mut self, version: &str) -> Self {
        if !version.is_empty() {
            self.config.version = Some(version.to_string())
        }

        self
    }

    /// Set the endpoint for ghac service.
    ///
    /// For example, this is provided as the `ACTIONS_CACHE_URL` environment variable by the GHA runner.
    ///
    /// Default: the value of the `ACTIONS_CACHE_URL` environment variable.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.to_string())
        }
        self
    }

    /// Set the runtime token for ghac service.
    ///
    /// For example, this is provided as the `ACTIONS_RUNTIME_TOKEN` environment variable by the GHA
    /// runner.
    ///
    /// Default: the value of the `ACTIONS_RUNTIME_TOKEN` environment variable.
    pub fn runtime_token(mut self, runtime_token: &str) -> Self {
        if !runtime_token.is_empty() {
            self.config.runtime_token = Some(runtime_token.to_string())
        }
        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    #[allow(deprecated)]
    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for GhacBuilder {
    const SCHEME: Scheme = Scheme::Ghac;
    type Config = GhacConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", root);

        let service_version = get_cache_service_version();
        debug!("backend use service version {:?}", service_version);

        let mut version = self
            .config
            .version
            .clone()
            .unwrap_or_else(|| "opendal".to_string());
        debug!("backend use version {version}");
        // ghac requires to use hex digest of Sha256 as version.
        if matches!(service_version, GhacVersion::V2) {
            let hash = sha2::Sha256::digest(&version);
            version = format!("{:x}", hash);
        }

        let cache_url = self
            .config
            .endpoint
            .unwrap_or_else(|| get_cache_service_url(service_version));
        if cache_url.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "cache url for ghac not found, maybe not in github action environment?".to_string(),
            ));
        }

        let core = GhacCore {
            info: {
                let am = AccessorInfo::default();
                am.set_scheme(Scheme::Ghac)
                    .set_root(&root)
                    .set_name(&version)
                    .set_native_capability(Capability {
                        stat: true,
                        stat_has_cache_control: true,
                        stat_has_content_length: true,
                        stat_has_content_type: true,
                        stat_has_content_encoding: true,
                        stat_has_content_range: true,
                        stat_has_etag: true,
                        stat_has_content_md5: true,
                        stat_has_last_modified: true,
                        stat_has_content_disposition: true,

                        read: true,

                        write: true,
                        write_can_multi: true,

                        shared: true,

                        ..Default::default()
                    });

                // allow deprecated api here for compatibility
                #[allow(deprecated)]
                if let Some(client) = self.http_client {
                    am.update_http_client(|_| client);
                }

                am.into()
            },
            root,

            cache_url,
            catch_token: value_or_env(
                self.config.runtime_token,
                ACTIONS_RUNTIME_TOKEN,
                "Builder::build",
            )?,
            version,

            service_version,
        };

        Ok(GhacBackend {
            core: Arc::new(core),
        })
    }
}

/// Backend for github action cache services.
#[derive(Debug, Clone)]
pub struct GhacBackend {
    core: Arc<GhacCore>,
}

impl Access for GhacBackend {
    type Reader = HttpBody;
    type Writer = GhacWriter;
    type Lister = ();
    type Deleter = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    /// Some self-hosted GHES instances are backed by AWS S3 services which only returns
    /// signed url with `GET` method. So we will use `GET` with empty range to simulate
    /// `HEAD` instead.
    ///
    /// In this way, we can support both self-hosted GHES and `github.com`.
    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let location = self.core.ghac_get_download_url(path).await?;

        let req = Request::get(location)
            .header(header::RANGE, "bytes=0-0")
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let resp = self.core.info.http_client().send(req).await?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT | StatusCode::RANGE_NOT_SATISFIABLE => {
                let mut meta = parse_into_metadata(path, resp.headers())?;
                // Correct content length via returning content range.
                meta.set_content_length(
                    meta.content_range()
                        .expect("content range must be valid")
                        .size()
                        .expect("content range must contains size"),
                );

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let location = self.core.ghac_get_download_url(path).await?;

        let mut req = Request::get(location);

        if !args.range().is_full() {
            req = req.header(header::RANGE, args.range().to_header());
        }
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        let resp = self.core.info.http_client().fetch(req).await?;

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

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let url = self.core.ghac_get_upload_url(path).await?;

        Ok((
            RpWrite::default(),
            GhacWriter::new(self.core.clone(), path.to_string(), url)?,
        ))
    }
}
