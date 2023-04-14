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

use async_trait::async_trait;
use bytes::Bytes;
use http::StatusCode;

use super::core::*;
use super::error::parse_error;
use crate::ops::{OpAppend, OpWrite};
use crate::raw::*;
use crate::*;

pub struct WasabiWriter {
    core: Arc<WasabiCore>,

    op_write: OpWrite,
    // the `enabled` field in `OpAppend` is used to differentiate
    // direct write controlled by `OpWrite` (regardless of single upload or multipart upload,
    // indicated by `append` field in `OpWrite`) from append write
    op_append: OpAppend,
    path: String,

    upload_id: Option<String>,
    parts: Vec<CompleteMultipartUploadRequestPart>,
}

impl WasabiWriter {
    pub fn new(
        core: Arc<WasabiCore>,
        op_write: OpWrite,
        op_append: OpAppend,
        path: String,
        upload_id: Option<String>,
    ) -> Self {
        WasabiWriter {
            core,

            op_write,
            op_append,
            path,
            upload_id,
            parts: vec![],
        }
    }
}

#[async_trait]
impl oio::Write for WasabiWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        debug_assert!(
            self.upload_id.is_none(),
            "Writer initiated with upload id, but users trying to call write, must be buggy"
        );

        let resp = if self.op_append.enabled() {
            self.core
                .append_object(&self.path, Some(bs.len()), AsyncBody::Bytes(bs))
                .await?
        } else {
            self.core
                .put_object(
                    &self.path,
                    Some(bs.len()),
                    self.op_write.content_type(),
                    self.op_write.content_disposition(),
                    self.op_write.cache_control(),
                    AsyncBody::Bytes(bs),
                )
                .await?
        };

        match resp.status() {
            StatusCode::CREATED | StatusCode::OK | StatusCode::NO_CONTENT => {
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

        let mut req = self.core.upload_part_request(
            &self.path,
            upload_id,
            part_number,
            Some(bs.len() as u64),
            AsyncBody::Bytes(bs),
        )?;

        self.core.sign(&mut req).await?;

        let resp = self.core.send(req).await?;

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

    async fn abort(&mut self) -> Result<()> {
        let upload_id = if let Some(upload_id) = &self.upload_id {
            upload_id
        } else {
            return Ok(());
        };

        let resp = self
            .core
            .s3_abort_multipart_upload(&self.path, upload_id)
            .await?;
        match resp.status() {
            StatusCode::OK => {
                resp.into_body().consume().await?;
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
            .core
            .complete_multipart_upload(&self.path, upload_id, &self.parts)
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
