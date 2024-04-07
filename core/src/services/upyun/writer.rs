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

use http::StatusCode;


use super::core::UpyunCore;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub type UpyunWriters = oio::MultipartWriter<UpyunWriter>;

pub struct UpyunWriter {
    core: Arc<UpyunCore>,
    op: OpWrite,
    path: String,
}

impl UpyunWriter {
    pub fn new(core: Arc<UpyunCore>, op: OpWrite, path: String) -> Self {
        UpyunWriter { core, op, path }
    }
}

impl oio::MultipartWrite for UpyunWriter {
    async fn write_once(&self, size: u64, body: RequestBody) -> Result<()> {
        let req = self
            .core
            .upload(&self.path, Some(size), &self.op, body)
            .await?;

        let (parts, body) = self.core.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    async fn initiate_part(&self) -> Result<String> {
        self.core
            .initiate_multipart_upload(&self.path, &self.op)
            .await
    }

    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: RequestBody,
    ) -> Result<oio::MultipartPart> {
        let req = self
            .core
            .upload_part(&self.path, upload_id, part_number, size, body)
            .await?;

        let (parts, body) = self.core.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::NO_CONTENT | StatusCode::CREATED => {
                body.consume().await?;
                Ok(oio::MultipartPart {
                    part_number,
                    etag: "".to_string(),
                })
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    async fn complete_part(&self, upload_id: &str, _parts: &[oio::MultipartPart]) -> Result<()> {
        self.core
            .complete_multipart_upload(&self.path, upload_id)
            .await
    }

    async fn abort_part(&self, _upload_id: &str) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "Upyun does not support abort multipart upload",
        ))
    }
}
