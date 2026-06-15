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

use super::VERCEL_BLOB_SCHEME;
use super::config::VercelBlobConfig;
use super::core::Blob;
use super::core::VercelBlobCore;
use super::core::parse_blob;
use super::deleter::VercelBlobDeleter;
use super::error::parse_error;
use super::lister::VercelBlobLister;
use super::writer::VercelBlobWriter;
use super::writer::VercelBlobWriters;
use opendal_core::raw::*;
use opendal_core::*;

/// [VercelBlob](https://vercel.com/docs/storage/vercel-blob) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct VercelBlobBuilder {
    pub(super) config: VercelBlobConfig,
}

impl Debug for VercelBlobBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VercelBlobBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl VercelBlobBuilder {
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

    /// Vercel Blob token.
    ///
    /// Get from Vercel environment variable `BLOB_READ_WRITE_TOKEN`.
    /// It is required.
    pub fn token(mut self, token: &str) -> Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_string());
        }
        self
    }
}

impl Builder for VercelBlobBuilder {
    type Config = VercelBlobConfig;

    /// Builds the backend and returns the result of VercelBlobBackend.
    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle token.
        let Some(token) = self.config.token.clone() else {
            return Err(Error::new(ErrorKind::ConfigInvalid, "token is empty")
                .with_operation("Builder::build")
                .with_context("service", VERCEL_BLOB_SCHEME));
        };

        Ok(VercelBlobBackend {
            core: Arc::new(VercelBlobCore {
                info: ServiceInfo::new(VERCEL_BLOB_SCHEME, &root, ""),
                capability: Capability {
                    stat: true,

                    read: true,
                    read_with_suffix: true,

                    write: true,
                    write_can_empty: true,
                    write_can_multi: true,
                    write_multi_min_size: Some(5 * 1024 * 1024),

                    copy: true,

                    list: true,
                    list_with_limit: true,

                    delete: true,

                    shared: true,

                    ..Default::default()
                },
                root,
                token,
            }),
        })
    }
}

/// Backend for VercelBlob services.
#[derive(Debug, Clone)]
pub struct VercelBlobBackend {
    core: Arc<VercelBlobCore>,
}

/// Reader returned by this backend.
pub struct VercelBlobReader {
    backend: VercelBlobBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl VercelBlobReader {
    fn new(backend: VercelBlobBackend, ctx: OperationContext, path: &str, args: OpRead) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::StreamRead for VercelBlobReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend.core.download(&self.ctx, path, range, &args).await?;

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

impl Service for VercelBlobBackend {
    type Reader = oio::StreamReader<VercelBlobReader>;
    type Writer = VercelBlobWriters;
    type Lister = oio::PageLister<VercelBlobLister>;
    type Deleter = oio::OneShotDeleter<VercelBlobDeleter>;
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.head(ctx, path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let resp: Blob =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                parse_blob(&resp).map(RpStat::new)
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
        let (rp, output): (_, oio::StreamReader<VercelBlobReader>) = {
            Ok((
                RpRead::default(),
                oio::StreamReader::new(VercelBlobReader::new(
                    self.clone(),
                    ctx.clone(),
                    path,
                    args,
                )),
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
        let (rp, output): (_, VercelBlobWriters) = {
            let concurrent = args.concurrent();
            let writer =
                VercelBlobWriter::new(self.core.clone(), ctx.clone(), args, path.to_string());

            let w = oio::MultipartWriter::new(ctx.executor().clone(), writer, concurrent);

            Ok((RpWrite::default(), w))
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
            let resp = self.core.copy(ctx, from, to).await?;

            let status = resp.status();

            match status {
                StatusCode::OK => Ok((RpCopy::default(), ())),
                _ => Err(parse_error(resp)),
            }
        }?;

        Ok((rp, output))
    }

    async fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        let (rp, output): (_, oio::PageLister<VercelBlobLister>) = {
            let l = VercelBlobLister::new(self.core.clone(), ctx.clone(), path, args.limit());
            Ok((RpList::default(), oio::PageLister::new(l)))
        }?;

        Ok((rp, output))
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, output): (_, oio::OneShotDeleter<VercelBlobDeleter>) = {
            Ok((
                RpDelete::default(),
                oio::OneShotDeleter::new(VercelBlobDeleter::new(self.core.clone(), ctx.clone())),
            ))
        }?;

        Ok((rp, output))
    }
}
