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

use super::core::*;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub type OssWriters = TwoWays<oio::MultipartWriter<OssWriter>, oio::AppendWriter<OssWriter>>;

pub struct OssWriter {
    core: Arc<OssCore>,

    op: OpWrite,
    path: String,
}

impl OssWriter {
    pub fn new(core: Arc<OssCore>, path: &str, op: OpWrite) -> Self {
        OssWriter {
            core,
            path: path.to_string(),
            op,
        }
    }
}

impl oio::MultipartWrite for OssWriter {
    async fn write_once(&self, size: u64, body: RequestBody) -> Result<()> {
        self.core
            .oss_put_object(&self.path, Some(size), &self.op, body)
            .await
    }

    async fn initiate_part(&self) -> Result<String> {
        self.core
            .oss_initiate_upload(
                &self.path,
                self.op.content_type(),
                self.op.content_disposition(),
                self.op.cache_control(),
                false,
            )
            .await
    }

    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: RequestBody,
    ) -> Result<oio::MultipartPart> {
        // OSS requires part number must between [1..=10000]
        let part_number = part_number + 1;

        let etag = self
            .core
            .oss_upload_part_request(&self.path, upload_id, part_number, false, size, body)
            .await?;
        Ok(oio::MultipartPart { part_number, etag })
    }

    async fn complete_part(&self, upload_id: &str, parts: &[oio::MultipartPart]) -> Result<()> {
        let parts = parts
            .iter()
            .map(|p| MultipartUploadPart {
                part_number: p.part_number,
                etag: p.etag.clone(),
            })
            .collect();

        self.core
            .oss_complete_multipart_upload_request(&self.path, upload_id, false, parts)
            .await
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        self.core
            .oss_abort_multipart_upload(&self.path, upload_id)
            .await
    }
}

impl oio::AppendWrite for OssWriter {
    async fn offset(&self) -> Result<u64> {
        let resp = self.core.oss_head_object(&self.path, None, None).await;

        match resp {
            Ok(meta) => Ok(meta.content_length()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(0),
            Err(err) => Err(err),
        }
    }

    async fn append(&self, offset: u64, size: u64, body: RequestBody) -> Result<()> {
        let mut req = self
            .core
            .oss_append_object_request(&self.path, offset, size, &self.op, body)?;

        self.core.sign(&mut req).await?;

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
}
