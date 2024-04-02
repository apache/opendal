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

use super::core::*;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub type S3Writers = oio::MultipartWriter<S3Writer>;

pub struct S3Writer {
    core: Arc<S3Core>,

    op: OpWrite,
    path: String,
}

impl S3Writer {
    pub fn new(core: Arc<S3Core>, path: &str, op: OpWrite) -> Self {
        S3Writer {
            core,
            path: path.to_string(),
            op,
        }
    }
}

impl oio::MultipartWrite for S3Writer {
    async fn write_once(&self, size: u64, body: RequestBody) -> Result<()> {
        let mut req = self
            .core
            .s3_put_object_request(&self.path, Some(size), &self.op, body)?;

        self.core.sign(&mut req).await?;

        let resp = self.core.send(req).await?;

        let status = resp.status();

        match parts.status {
            StatusCode::CREATED | StatusCode::OK => Ok(()),
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    async fn initiate_part(&self) -> Result<String> {
        let resp = self
            .core
            .s3_initiate_multipart_upload(&self.path, &self.op)
            .await?;

        let status = resp.status();

        match parts.status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let result: InitiateMultipartUploadResult =
                    quick_xml::de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

                Ok(result.upload_id)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: RequestBody,
    ) -> Result<oio::MultipartPart> {
        // AWS S3 requires part number must between [1..=10000]
        let part_number = part_number + 1;

        let mut req =
            self.core
                .s3_upload_part_request(&self.path, upload_id, part_number, size, body)?;

        self.core.sign(&mut req).await?;

        let resp = self.core.send(req).await?;

        let status = resp.status();

        match parts.status {
            StatusCode::OK => {
                let etag = parse_etag(resp.headers())?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "ETag not present in returning response",
                        )
                    })?
                    .to_string();

                Ok(oio::MultipartPart { part_number, etag })
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    async fn complete_part(&self, upload_id: &str, parts: &[oio::MultipartPart]) -> Result<()> {
        let parts = parts
            .iter()
            .map(|p| CompleteMultipartUploadRequestPart {
                part_number: p.part_number,
                etag: p.etag.clone(),
            })
            .collect();

        let resp = self
            .core
            .s3_complete_multipart_upload(&self.path, upload_id, parts)
            .await?;

        let status = resp.status();

        match parts.status {
            StatusCode::OK => Ok(()),
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        let resp = self
            .core
            .s3_abort_multipart_upload(&self.path, upload_id)
            .await?;
        match resp.status() {
            // s3 returns code 204 if abort succeeds.
            StatusCode::NO_CONTENT => Ok(()),
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }
}
