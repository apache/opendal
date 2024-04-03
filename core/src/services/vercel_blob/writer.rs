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
use http::StatusCode;

use super::core::InitiateMultipartUploadResponse;
use super::core::Part;
use super::core::UploadPartResponse;
use super::core::VercelBlobCore;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub type VercelBlobWriters = oio::MultipartWriter<VercelBlobWriter>;

pub struct VercelBlobWriter {
    core: Arc<VercelBlobCore>,
    op: OpWrite,
    path: String,
}

impl VercelBlobWriter {
    pub fn new(core: Arc<VercelBlobCore>, op: OpWrite, path: String) -> Self {
        VercelBlobWriter { core, op, path }
    }
}

impl oio::MultipartWrite for VercelBlobWriter {
    async fn write_once(&self, size: u64, body: RequestBody) -> Result<()> {
        let req = self
            .core
            .get_put_request(&self.path, Some(size), &self.op, body)
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
        let part_number = part_number + 1;

        let etag = self
            .core
            .upload_part(&self.path, upload_id, part_number, size, body)
            .await?;

        Ok(oio::MultipartPart { part_number, etag })
    }

    async fn complete_part(&self, upload_id: &str, parts: &[oio::MultipartPart]) -> Result<()> {
        let parts = parts
            .iter()
            .map(|p| Part {
                part_number: p.part_number,
                etag: p.etag.clone(),
            })
            .collect::<Vec<Part>>();

        self.core
            .complete_multipart_upload(&self.path, upload_id, parts)
            .await
    }

    async fn abort_part(&self, _upload_id: &str) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "VercelBlob does not support abort multipart upload",
        ))
    }
}
