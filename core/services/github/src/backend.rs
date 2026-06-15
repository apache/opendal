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

use bytes::Buf;
use http::Response;
use http::StatusCode;
use log::debug;

use super::GITHUB_SCHEME;
use super::config::GithubConfig;
use super::core::Entry;
use super::core::GithubCore;
use super::deleter::GithubDeleter;
use super::error::parse_error;
use super::lister::GithubLister;
use super::writer::GithubWriter;
use super::writer::GithubWriters;
use opendal_core::raw::*;
use opendal_core::*;

/// [github contents](https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#create-or-update-file-contents) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct GithubBuilder {
    pub(super) config: GithubConfig,
}

impl Debug for GithubBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GithubBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
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
}

impl Builder for GithubBuilder {
    type Config = GithubConfig;

    /// Builds the backend and returns the result of GithubBackend.
    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle owner.
        if self.config.owner.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "owner is empty")
                .with_operation("Builder::build")
                .with_context("service", GITHUB_SCHEME));
        }

        debug!("backend use owner {}", &self.config.owner);

        // Handle repo.
        if self.config.repo.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "repo is empty")
                .with_operation("Builder::build")
                .with_context("service", GITHUB_SCHEME));
        }

        debug!("backend use repo {}", &self.config.repo);

        Ok(GithubBackend {
            core: Arc::new(GithubCore {
                info: ServiceInfo::new(GITHUB_SCHEME, &root, ""),
                capability: Capability {
                    stat: true,

                    read: true,
                    read_with_suffix: true,

                    create_dir: true,

                    write: true,
                    write_can_empty: true,

                    delete: true,

                    list: true,
                    list_with_recursive: true,

                    shared: true,

                    ..Default::default()
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

/// Reader returned by this backend.
pub struct GithubReader {
    backend: GithubBackend,
    ctx: OperationContext,
    path: String,
}

impl GithubReader {
    fn new(backend: GithubBackend, ctx: OperationContext, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for GithubReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let resp = backend.core.get(&self.ctx, path, range).await?;

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

impl Service for GithubBackend {
    type Reader = oio::StreamReader<GithubReader>;
    type Writer = GithubWriters;
    type Lister = oio::PageLister<GithubLister>;
    type Deleter = oio::OneShotDeleter<GithubDeleter>;
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
        let empty_bytes = Buffer::new();

        let resp = self
            .core
            .upload(ctx, &format!("{path}.gitkeep"), empty_bytes)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::CREATED => Ok(RpCreateDir::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.stat(ctx, path).await?;

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
    async fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        let (rp, output): (_, oio::StreamReader<GithubReader>) = {
            Ok((
                RpRead::default(),
                oio::StreamReader::new(GithubReader::new(self.clone(), ctx.clone(), path, args)),
            ))
        }?;

        Ok((rp, output))
    }

    async fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        _args: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        let (rp, output): (_, GithubWriters) = {
            let writer = GithubWriter::new(self.core.clone(), ctx.clone(), path.to_string());

            let w = oio::OneShotWriter::new(writer);

            Ok((RpWrite::default(), w))
        }?;

        Ok((rp, output))
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, output): (_, oio::OneShotDeleter<GithubDeleter>) = {
            Ok((
                RpDelete::default(),
                oio::OneShotDeleter::new(GithubDeleter::new(self.core.clone(), ctx.clone())),
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
        let (rp, output): (_, oio::PageLister<GithubLister>) = {
            let l = GithubLister::new(self.core.clone(), ctx.clone(), path, args.recursive());
            Ok((RpList::default(), oio::PageLister::new(l)))
        }?;

        Ok((rp, output))
    }

    async fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
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
