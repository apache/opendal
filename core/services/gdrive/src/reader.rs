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

use super::backend::*;
use super::core::{ErrorContext, parse_error};
use http::Response;
use http::StatusCode;
use opendal_core::raw::*;
use opendal_core::*;

/// Reader returned by this backend.
pub struct GdriveReader {
    backend: GdriveBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl GdriveReader {
    pub(super) fn new(
        backend: GdriveBackend,
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

impl oio::StreamRead for GdriveReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let core = &self.backend.core;
        let path = self.path.as_str();
        let abs_path = build_absolute_path(&core.root, path);
        let version = self.args.version();
        let resp = match core.gdrive_get(&self.ctx, path, range, version).await {
            Ok(resp) => resp,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                core.refresh_path(&abs_path).await;
                core.gdrive_get(&self.ctx, path, range, version).await?
            }
            Err(err) => return Err(err),
        };

        let resp = if resp.status() == StatusCode::NOT_FOUND {
            core.refresh_path(&abs_path).await;
            core.gdrive_get(&self.ctx, path, range, version).await?
        } else {
            resp
        };

        let (rp, stream) = match resp.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                RpRead::new(parse_into_metadata(path, resp.headers())?),
                resp.into_body(),
            ),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("DownloadFile")),
                    Response::from_parts(part, buf),
                ));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}
