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

use async_trait::async_trait;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;

use super::error::parse_error;
use super::writer::VercelArtifactsWriter;
use crate::raw::*;
use crate::*;

#[derive(Clone)]
pub struct VercelArtifactsBackend {
    pub(crate) access_token: String,
    pub(crate) client: HttpClient,
}

impl Debug for VercelArtifactsBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("VercelArtifactsBackend");
        de.field("access_token", &self.access_token);
        de.finish()
    }
}

#[async_trait]
impl Accessor for VercelArtifactsBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = VercelArtifactsWriter;
    type BlockingWriter = ();
    type Appender = ();
    type Pager = ();
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(Scheme::VercelArtifacts)
            .set_capability(Capability {
                read: true,
                read_can_next: true,

                write: true,

                ..Default::default()
            });

        ma
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.vercel_artifacts_get(path, args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }

            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.content_length().is_none() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write without content length is not supported",
            ));
        }

        Ok((
            RpWrite::default(),
            VercelArtifactsWriter::new(self.clone(), args, path.to_string()),
        ))
    }
}

impl VercelArtifactsBackend {
    async fn vercel_artifacts_get(
        &self,
        path: &str,
        args: OpRead,
    ) -> Result<Response<IncomingAsyncBody>> {
        let url: String = format!("https://api.vercel.com/v8/artifacts/{}", path);

        let mut req = Request::get(&url);

        if !args.range().is_full() {
            req = req.header(header::RANGE, args.range().to_header());
        }

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn vercel_artifacts_put(
        &self,
        hash: &str,
        size: u64,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let url = format!("https://api.vercel.com/v8/artifacts/{}", hash);

        let mut req = Request::put(&url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        // Borrowed from [vercel/remote-cache](https://github.com/vercel/remote-cache/blob/46cbc71346c84ec6c3022ec660ade52a25a20013/packages/remote/src/artifact-request.ts#LL41C34-L41C58)
        req = req.header(header::CONTENT_TYPE, "application/octet-stream");
        req = req.header(header::AUTHORIZATION, auth_header_content);
        req = req.header(header::CONTENT_LENGTH, size);

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }
}
