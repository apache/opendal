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

use bytes::Buf;
use http::Response;
use http::StatusCode;
use log::debug;

use super::core::Entry;
use super::core::GithubCore;
use super::delete::GithubDeleter;
use super::error::parse_error;
use super::lister::GithubLister;
use super::writer::GithubWriter;
use super::writer::GithubWriters;
use crate::raw::*;
use crate::services::GithubConfig;
use crate::*;

impl Configurator for GithubConfig {
    type Builder = GithubBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        GithubBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// [github contents](https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#create-or-update-file-contents) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct GithubBuilder {
    config: GithubConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl Debug for GithubBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("GithubBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl GithubBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Github access_token.
    ///
    /// required.
    pub fn token(mut self, token: &str) -> Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_string());
        }
        self
    }

    /// Set Github repo owner.
    pub fn owner(mut self, owner: &str) -> Self {
        self.config.owner = owner.to_string();

        self
    }

    /// Set Github repo name.
    pub fn repo(mut self, repo: &str) -> Self {
        self.config.repo = repo.to_string();

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

impl Builder for GithubBuilder {
    const SCHEME: Scheme = Scheme::Github;
    type Config = GithubConfig;

    /// Builds the backend and returns the result of GithubBackend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle owner.
        if self.config.owner.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "owner is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Github));
        }

        debug!("backend use owner {}", &self.config.owner);

        // Handle repo.
        if self.config.repo.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "repo is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Github));
        }

        debug!("backend use repo {}", &self.config.repo);

        Ok(GithubBackend {
            core: Arc::new(GithubCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::Github)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_has_content_length: true,
                            stat_has_etag: true,

                            read: true,

                            create_dir: true,

                            write: true,
                            write_can_empty: true,

                            delete: true,

                            list: true,
                            list_with_recursive: true,
                            list_has_content_length: true,
                            list_has_etag: true,

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
                token: self.config.token.clone(),
                owner: self.config.owner.clone(),
                repo: self.config.repo.clone(),
            }),
        })
    }
}

/// Backend for Github services.
#[derive(Debug, Clone)]
pub struct GithubBackend {
    core: Arc<GithubCore>,
}

impl Access for GithubBackend {
    type Reader = HttpBody;
    type Writer = GithubWriters;
    type Lister = oio::PageLister<GithubLister>;
    type Deleter = oio::OneShotDeleter<GithubDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let empty_bytes = Buffer::new();

        let resp = self
            .core
            .upload(&format!("{}.gitkeep", path), empty_bytes)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::CREATED => Ok(RpCreateDir::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.stat(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let body = resp.into_body();
                let resp: Entry =
                    serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;

                let m = if resp.type_field == "dir" {
                    Metadata::new(EntryMode::DIR)
                } else {
                    Metadata::new(EntryMode::FILE)
                        .with_content_length(resp.size)
                        .with_etag(resp.sha)
                };

                Ok(RpStat::new(m))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.get(path, args.range()).await?;

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

    async fn write(&self, path: &str, _args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = GithubWriter::new(self.core.clone(), path.to_string());

        let w = oio::OneShotWriter::new(writer);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(GithubDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = GithubLister::new(self.core.clone(), path, args.recursive());
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}
