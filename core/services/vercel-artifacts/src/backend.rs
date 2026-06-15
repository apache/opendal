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

use super::core::VercelArtifactsCore;
use super::error::parse_error;
use super::writer::VercelArtifactsWriter;
use opendal_core::raw::*;
use opendal_core::*;

#[doc = include_str!("docs.md")]
#[derive(Clone, Debug)]
pub struct VercelArtifactsBackend {
    pub core: Arc<VercelArtifactsCore>,
}

/// Reader returned by this backend.
pub struct VercelArtifactsReader {
    backend: VercelArtifactsBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl VercelArtifactsReader {
    fn new(
        backend: VercelArtifactsBackend,
        ctx: OperationContext,
        path: &str,
        args: OpRead,
    ) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::StreamRead for VercelArtifactsReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let response = backend
            .core
            .vercel_artifacts_get(&self.ctx, path, range, &args)
            .await?;

        let status = response.status();

        let (rp, stream) = match status {
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

impl Service for VercelArtifactsBackend {
    type Reader = oio::StreamReader<VercelArtifactsReader>;
    type Writer = oio::OneShotWriter<VercelArtifactsWriter>;
    type Lister = ();
    type Deleter = ();
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, _args: OpStat) -> Result<RpStat> {
        let response = self.core.vercel_artifacts_stat(ctx, path).await?;

        let status = response.status();

        match status {
            StatusCode::OK => {
                let meta = parse_into_metadata(path, response.headers())?;
                Ok(RpStat::new(meta))
            }

            _ => Err(parse_error(response)),
        }
    }
    async fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        let (rp, output): (_, oio::StreamReader<VercelArtifactsReader>) = {
            Ok((
                RpRead::default(),
                oio::StreamReader::new(VercelArtifactsReader::new(
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
        let (rp, output): (_, oio::OneShotWriter<VercelArtifactsWriter>) = {
            Ok((
                RpWrite::default(),
                oio::OneShotWriter::new(VercelArtifactsWriter::new(
                    self.core.clone(),
                    ctx.clone(),
                    args,
                    path.to_string(),
                )),
            ))
        }?;

        Ok((rp, output))
    }
}
