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
use crate::services::vercel_artifacts::reader::VercelArtifactsReader;
use crate::*;

#[doc = include_str!("docs.md")]
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
    type Reader = VercelArtifactsReader;
    type Writer = oio::OneShotWriter<VercelArtifactsWriter>;
    type Lister = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(Scheme::VercelArtifacts)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                write: true,

                ..Default::default()
            });

        ma
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let res = self.vercel_artifacts_stat(path).await?;

        let status = res.status();

        match parts.status {
            StatusCode::OK => {
                let meta = parse_into_metadata(path, res.headers())?;
                Ok(RpStat::new(meta))
            }

            _ => Err(parse_error(res).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        Ok((
            RpRead::default(),
            VercelArtifactsReader::new(self.clone(), path, args),
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(VercelArtifactsWriter::new(
                self.clone(),
                args,
                path.to_string(),
            )),
        ))
    }
}

impl VercelArtifactsBackend {
    pub async fn vercel_artifacts_get(
        &self,
        hash: &str,
        range: BytesRange,
        _: &OpRead,
    ) -> Result<Response<oio::Buffer>> {
        let url: String = format!(
            "https://api.vercel.com/v8/artifacts/{}",
            percent_encode_path(hash)
        );

        let mut req = Request::get(&url);

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
    }

    pub async fn vercel_artifacts_put(
        &self,
        hash: &str,
        size: u64,
        body: RequestBody,
    ) -> Result<Response<oio::Buffer>> {
        let url = format!(
            "https://api.vercel.com/v8/artifacts/{}",
            percent_encode_path(hash)
        );

        let mut req = Request::put(&url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::CONTENT_TYPE, "application/octet-stream");
        req = req.header(header::AUTHORIZATION, auth_header_content);
        req = req.header(header::CONTENT_LENGTH, size);

        let req = req.body(body).map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
    }

    pub async fn vercel_artifacts_stat(&self, hash: &str) -> Result<Response<oio::Buffer>> {
        let url = format!(
            "https://api.vercel.com/v8/artifacts/{}",
            percent_encode_path(hash)
        );

        let mut req = Request::head(&url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);
        req = req.header(header::CONTENT_LENGTH, 0);

        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
    }
}
