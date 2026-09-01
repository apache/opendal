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
pub struct GithubReader {
    backend: GithubBackend,
    ctx: OperationContext,
    path: String,
}

impl GithubReader {
    pub(super) fn new(
        backend: GithubBackend,
        ctx: OperationContext,
        path: &str,
        _: OpRead,
    ) -> Self {
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
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let (part, mut body) = resp.into_parts();
                let meta = parse_into_metadata(path, &part.headers)?;

                // GitHub ignores the Range header on authenticated requests
                // and returns the full content with 200, so slice the body
                // client-side when the server did not honor the range.
                if status == StatusCode::PARTIAL_CONTENT || range.is_full() {
                    (
                        RpRead::new(meta),
                        Box::new(body) as Box<dyn oio::ReadStreamDyn>,
                    )
                } else {
                    let bs = body.to_buffer().await?;
                    let total_size = bs.len() as u64;
                    let sliced = bs.slice(range.to_content_range(bs.len())?);
                    let meta = Metadata::new(EntryMode::FILE).with_content_length(total_size);
                    (
                        RpRead::new(meta),
                        Box::new(sliced) as Box<dyn oio::ReadStreamDyn>,
                    )
                }
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("GetRawContent")),
                    Response::from_parts(part, buf),
                ));
            }
        };

        Ok((rp, stream))
    }
}
