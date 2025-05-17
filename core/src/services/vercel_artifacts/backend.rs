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

use super::core::VercelArtifactsCore;
use super::error::parse_error;
use super::writer::VercelArtifactsWriter;
use crate::raw::*;
use crate::*;

#[doc = include_str!("docs.md")]
#[derive(Clone)]
pub struct VercelArtifactsBackend {
    pub core: Arc<VercelArtifactsCore>,
}

impl Debug for VercelArtifactsBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VercelArtifactsBackend")
            .field("core", &self.core)
            .finish()
    }
}

impl Access for VercelArtifactsBackend {
    type Reader = HttpBody;
    type Writer = oio::OneShotWriter<VercelArtifactsWriter>;
    type Lister = ();
    type Deleter = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let response = self.core.vercel_artifacts_stat(path).await?;

        let status = response.status();

        match status {
            StatusCode::OK => {
                let meta = parse_into_metadata(path, response.headers())?;
                Ok(RpStat::new(meta))
            }

            _ => Err(parse_error(response)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let response = self
            .core
            .vercel_artifacts_get(path, args.range(), &args)
            .await?;

        let status = response.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                Ok((RpRead::new(), response.into_body()))
            }
            _ => {
                let (part, mut body) = response.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(VercelArtifactsWriter::new(
                self.core.clone(),
                args,
                path.to_string(),
            )),
        ))
    }
}
