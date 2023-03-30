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

use async_trait::async_trait;
use bytes::Bytes;
use http::StatusCode;

use super::backend::CompleteMultipartUploadRequestPart;
use super::backend::S3Backend;
use super::error::parse_error;
use crate::ops::OpWrite;
use crate::raw::*;
use crate::*;

pub struct S3Writer {
    backend: S3Backend,

    op: OpWrite,
    path: String,

    upload_id: Option<String>,
    parts: Vec<CompleteMultipartUploadRequestPart>,
}

impl S3Writer {
    pub fn new(backend: S3Backend, op: OpWrite, path: String, upload_id: Option<String>) -> Self {
        S3Writer {
            backend,
            op,
            path,
            upload_id,
            parts: vec![],
        }
    }
}

#[async_trait]
impl oio::Write for S3Writer {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        debug_assert!(
            self.upload_id.is_none(),
            "Writer initiated with upload id, but users trying to call write, must be buggy"
        );

        let mut req = self.backend.s3_put_object_request(
            &self.path,
            Some(bs.len()),
            self.op.content_type(),
            self.op.content_disposition(),
            AsyncBody::Bytes(bs),
        )?;

        self.backend
            .signer
            .sign(&mut req)
            .map_err(new_request_sign_error)?;

        let resp = self.backend.client.send_async(req).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn append(&mut self, bs: Bytes) -> Result<()> {
        let upload_id = self.upload_id.as_ref().expect(
            "Writer doesn't have upload id, but users trying to call append, must be buggy",
        );
        // AWS S3 requires part number must between [1..=10000]
        let part_number = self.parts.len() + 1;

        let mut req = self.backend.s3_upload_part_request(
            &self.path,
            upload_id,
            part_number,
            Some(bs.len() as u64),
            AsyncBody::Bytes(bs),
        )?;

        self.backend
            .signer
            .sign(&mut req)
            .map_err(new_request_sign_error)?;

        let resp = self.backend.client.send_async(req).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let etag = parse_etag(resp.headers())?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "ETag not present in returning response",
                        )
                    })?
                    .to_string();

                resp.into_body().consume().await?;

                self.parts
                    .push(CompleteMultipartUploadRequestPart { part_number, etag });

                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn close(&mut self) -> Result<()> {
        let upload_id = if let Some(upload_id) = &self.upload_id {
            upload_id
        } else {
            return Ok(());
        };

        let resp = self
            .backend
            .s3_complete_multipart_upload(&self.path, upload_id, &self.parts)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                resp.into_body().consume().await?;

                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}
