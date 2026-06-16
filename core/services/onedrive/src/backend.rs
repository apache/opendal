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

use http::Response;
use http::StatusCode;

use opendal_core::raw::*;
use opendal_core::*;

use super::core::OneDriveCore;
use super::deleter::OneDriveDeleter;
use super::error::parse_error;
use super::lister::OneDriveLister;
use super::writer::OneDriveWriter;

#[derive(Clone, Debug)]
pub struct OnedriveBackend {
    pub core: Arc<OneDriveCore>,
}

/// Reader returned by this backend.
pub struct OnedriveReader {
    backend: OnedriveBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl OnedriveReader {
    fn new(backend: OnedriveBackend, ctx: OperationContext, path: &str, args: OpRead) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::StreamRead for OnedriveReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let response = backend
            .core
            .onedrive_get_content(&self.ctx, path, range, &args)
            .await?;
        let (rp, stream) = match response.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                RpRead::new(parse_into_metadata(path, response.headers())?),
                response.into_body(),
            ),
            _ => {
                let (part, mut body) = response.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(Response::from_parts(part, buf)));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

impl Service for OnedriveBackend {
    type Reader = oio::StreamReader<OnedriveReader>;
    type Writer = oio::OneShotWriter<OneDriveWriter>;
    type Lister = oio::PageLister<OneDriveLister>;
    type Deleter = oio::OneShotDeleter<OneDriveDeleter>;
    type Copier = oio::OneShotCopier;

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
        if path == "/" {
            // skip, the root path exists in the personal OneDrive.
            return Ok(RpCreateDir::default());
        }

        let response = self.core.onedrive_create_dir(ctx, path).await?;
        match response.status() {
            StatusCode::CREATED | StatusCode::OK => Ok(RpCreateDir::default()),
            _ => Err(parse_error(response)),
        }
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let meta = self.core.onedrive_stat(ctx, path, args).await?;

        Ok(RpStat::new(meta))
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<OnedriveReader> = {
            Ok(oio::StreamReader::new(OnedriveReader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let output: oio::OneShotWriter<OneDriveWriter> = {
            Ok(oio::OneShotWriter::new(OneDriveWriter::new(
                self.core.clone(),
                ctx.clone(),
                args,
                path.to_string(),
            )))
        }?;

        Ok(output)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<OneDriveDeleter> = {
            Ok(oio::OneShotDeleter::new(OneDriveDeleter::new(
                self.core.clone(),
                ctx.clone(),
            )))
        }?;

        Ok(output)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpCopy,
        _opts: OpCopier,
    ) -> Result<Self::Copier> {
        let core = self.core.clone();
        let ctx = ctx.clone();
        let from = from.to_string();
        let to = to.to_string();

        Ok(oio::OneShotCopier::new(async move {
            let monitor_url = core.initialize_copy(&ctx, &from, &to).await?;
            core.wait_until_complete(&ctx, monitor_url).await?;
            Ok(Metadata::default())
        }))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        if from == to {
            return Ok(RpRename::default());
        }

        self.core.onedrive_move(ctx, from, to).await?;

        Ok(RpRename::default())
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let output: oio::PageLister<OneDriveLister> = {
            let l = OneDriveLister::new(
                path.to_string(),
                self.core.clone(),
                ctx.clone(),
                self.core.capability,
                &args,
            );
            Ok(oio::PageLister::new(l))
        }?;

        Ok(output)
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
