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

use http::Response;
use http::StatusCode;
use log::debug;
use mea::rwlock::RwLock;

use super::SEAFILE_SCHEME;
use super::config::SeafileConfig;
use super::core::SeafileCore;
use super::core::SeafileSigner;
use super::core::parse_dir_detail;
use super::core::parse_file_detail;
use super::deleter::SeafileDeleter;
use super::error::parse_error;
use super::lister::SeafileLister;
use super::writer::SeafileWriter;
use super::writer::SeafileWriters;
use opendal_core::raw::*;
use opendal_core::*;

/// [seafile](https://www.seafile.com) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct SeafileBuilder {
    pub(super) config: SeafileConfig,
}

impl Debug for SeafileBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeafileBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl SeafileBuilder {
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

    /// endpoint of this backend.
    ///
    /// It is required. e.g. `http://127.0.0.1:80`
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };

        self
    }

    /// username of this backend.
    ///
    /// It is required. e.g. `me@example.com`
    pub fn username(mut self, username: &str) -> Self {
        self.config.username = if username.is_empty() {
            None
        } else {
            Some(username.to_string())
        };

        self
    }

    /// password of this backend.
    ///
    /// It is required. e.g. `asecret`
    pub fn password(mut self, password: &str) -> Self {
        self.config.password = if password.is_empty() {
            None
        } else {
            Some(password.to_string())
        };

        self
    }

    /// Set repo name of this backend.
    ///
    /// It is required. e.g. `myrepo`
    pub fn repo_name(mut self, repo_name: &str) -> Self {
        self.config.repo_name = repo_name.to_string();

        self
    }
}

impl Builder for SeafileBuilder {
    type Config = SeafileConfig;

    /// Builds the backend and returns the result of SeafileBackend.
    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle bucket.
        if self.config.repo_name.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "repo_name is empty")
                .with_operation("Builder::build")
                .with_context("service", SEAFILE_SCHEME));
        }

        debug!("backend use repo_name {}", &self.config.repo_name);

        let endpoint = match &self.config.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", SEAFILE_SCHEME)),
        }?;

        let username = match &self.config.username {
            Some(username) => Ok(username.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "username is empty")
                .with_operation("Builder::build")
                .with_context("service", SEAFILE_SCHEME)),
        }?;

        let password = match &self.config.password {
            Some(password) => Ok(password.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "password is empty")
                .with_operation("Builder::build")
                .with_context("service", SEAFILE_SCHEME)),
        }?;

        Ok(SeafileBackend {
            core: Arc::new(SeafileCore {
                info: ServiceInfo::new(SEAFILE_SCHEME, &root, ""),
                capability: Capability {
                    create_dir: true,
                    stat: true,

                    read: true,

                    write: true,
                    write_can_empty: true,

                    delete: true,

                    list: true,

                    shared: true,

                    ..Default::default()
                },
                root,
                endpoint,
                username,
                password,
                repo_name: self.config.repo_name.clone(),
                signer: Arc::new(RwLock::new(SeafileSigner::default())),
            }),
        })
    }
}

/// Backend for seafile services.
#[derive(Debug, Clone)]
pub struct SeafileBackend {
    core: Arc<SeafileCore>,
}

/// Reader returned by this backend.
pub struct SeafileReader {
    backend: SeafileBackend,
    ctx: OperationContext,
    path: String,
}

impl SeafileReader {
    fn new(backend: SeafileBackend, ctx: OperationContext, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for SeafileReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let resp = backend.core.download_file(&self.ctx, path, range).await?;

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

impl Service for SeafileBackend {
    type Reader = oio::StreamReader<SeafileReader>;
    type Writer = SeafileWriters;
    type Lister = oio::PageLister<SeafileLister>;
    type Deleter = oio::OneShotDeleter<SeafileDeleter>;
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
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.core.create_dir(ctx, path).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, _args: OpStat) -> Result<RpStat> {
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let metadata = if path.ends_with('/') {
            let dir_detail = self.core.dir_detail(ctx, path).await?;
            parse_dir_detail(dir_detail)
        } else {
            let file_detail = self.core.file_detail(ctx, path).await?;

            parse_file_detail(file_detail)
        };

        metadata.map(RpStat::new)
    }
    async fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        let (rp, output): (_, oio::StreamReader<SeafileReader>) = {
            Ok((
                RpRead::default(),
                oio::StreamReader::new(SeafileReader::new(self.clone(), ctx.clone(), path, args)),
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
        let (rp, output): (_, SeafileWriters) = {
            let w = SeafileWriter::new(self.core.clone(), ctx.clone(), args, path.to_string());
            let w = oio::OneShotWriter::new(w);

            Ok((RpWrite::default(), w))
        }?;

        Ok((rp, output))
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, output): (_, oio::OneShotDeleter<SeafileDeleter>) = {
            Ok((
                RpDelete::default(),
                oio::OneShotDeleter::new(SeafileDeleter::new(self.core.clone(), ctx.clone())),
            ))
        }?;

        Ok((rp, output))
    }

    async fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        _args: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        let (rp, output): (_, oio::PageLister<SeafileLister>) = {
            let l = SeafileLister::new(self.core.clone(), ctx.clone(), path);
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
