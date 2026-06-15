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

use bytes::Buf;
use http::Response;
use http::StatusCode;
use serde::Deserialize;

use super::core::IpmfsCore;
use super::deleter::IpmfsDeleter;
use super::error::parse_error;
use super::lister::IpmfsLister;
use super::writer::IpmfsWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// IPFS Mutable File System (IPMFS) backend.
#[doc = include_str!("docs.md")]
#[derive(Clone, Debug)]
pub struct IpmfsBackend {
    pub core: Arc<IpmfsCore>,
}

/// Reader returned by this backend.
pub struct IpmfsReader {
    backend: IpmfsBackend,
    ctx: OperationContext,
    path: String,
}

impl IpmfsReader {
    fn new(backend: IpmfsBackend, ctx: OperationContext, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for IpmfsReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let resp = backend.core.ipmfs_read(&self.ctx, path, range).await?;

        let status = resp.status();

        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (RpRead::default(), resp.into_body()),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(Response::from_parts(part, buf)));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

impl Service for IpmfsBackend {
    type Reader = oio::StreamReader<IpmfsReader>;
    type Writer = oio::OneShotWriter<IpmfsWriter>;
    type Lister = oio::PageLister<IpmfsLister>;
    type Deleter = oio::OneShotDeleter<IpmfsDeleter>;
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
        let resp = self.core.ipmfs_mkdir(ctx, path).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(RpCreateDir::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.core.ipmfs_stat(ctx, path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let res: IpfsStatResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                let mode = match res.file_type.as_str() {
                    "file" => EntryMode::FILE,
                    "directory" => EntryMode::DIR,
                    _ => EntryMode::Unknown,
                };

                let mut meta = Metadata::new(mode);
                meta.set_content_length(res.size);

                Ok(RpStat::new(meta))
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
        let (rp, output): (_, oio::StreamReader<IpmfsReader>) = {
            Ok((
                RpRead::default(),
                oio::StreamReader::new(IpmfsReader::new(self.clone(), ctx.clone(), path, args)),
            ))
        }?;

        Ok((rp, output))
    }

    async fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        _: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        let (rp, output): (_, oio::OneShotWriter<IpmfsWriter>) = {
            Ok((
                RpWrite::default(),
                oio::OneShotWriter::new(IpmfsWriter::new(
                    self.core.clone(),
                    ctx.clone(),
                    path.to_string(),
                )),
            ))
        }?;

        Ok((rp, output))
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, output): (_, oio::OneShotDeleter<IpmfsDeleter>) = {
            Ok((
                RpDelete::default(),
                oio::OneShotDeleter::new(IpmfsDeleter::new(self.core.clone(), ctx.clone())),
            ))
        }?;

        Ok((rp, output))
    }

    async fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        _: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        let (rp, output): (_, oio::PageLister<IpmfsLister>) = {
            let l = IpmfsLister::new(self.core.clone(), ctx.clone(), &self.core.root, path);
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

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct IpfsStatResponse {
    #[serde(rename = "Size")]
    size: u64,
    #[serde(rename = "Type")]
    file_type: String,
}
