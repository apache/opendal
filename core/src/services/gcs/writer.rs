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

use super::backend::GcsBackend;
use super::error::parse_error;
use crate::ops::OpWrite;
use crate::raw::*;
use crate::*;

pub struct GcsWriter {
    backend: GcsBackend,

    op: OpWrite,
    path: String,
    upload_location: Option<String>,
    already_uploaded_chunk: u64,
    last_chunk_uploaded: bool,
    prev_upload_chunk: Option<Bytes>,
}

impl GcsWriter {
    pub fn new(
        backend: GcsBackend,
        op: OpWrite,
        path: String,
        upload_location: Option<String>,
    ) -> Self {
        GcsWriter {
            backend,
            op,
            path,
            upload_location,
            already_uploaded_chunk: 0,
            last_chunk_uploaded: false,
            prev_upload_chunk: None,
        }
    }
}

#[async_trait]
impl oio::Write for GcsWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let mut req = self.backend.gcs_insert_object_request(
            &self.path,
            Some(bs.len()),
            self.op.content_type(),
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
        let upload_location = if let Some(upload_location) = &self.upload_location {
            upload_location
        } else {
            return Ok(());
        };

        let copied = bs.slice(0..bs.len());
        let chunk_size = bs.len() as u64;
        let is_last_chunk = chunk_size / 256 / 1024 == 0;
        let mut req = self.backend.gcs_upload_chunks_in_resumable_upload(
            upload_location,
            chunk_size,
            self.already_uploaded_chunk,
            is_last_chunk,
            AsyncBody::Bytes(bs),
        )?;

        self.backend
            .signer
            .sign(&mut req)
            .map_err(new_request_sign_error)?;

        let resp = self.backend.client.send_async(req).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PERMANENT_REDIRECT => {
                if is_last_chunk {
                    self.last_chunk_uploaded = true
                } else {
                    self.already_uploaded_chunk += chunk_size;
                    self.prev_upload_chunk = Some(copied);
                }
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn close(&mut self) -> Result<()> {
        if self.last_chunk_uploaded {
            return Ok(());
        }

        let upload_location = if let Some(upload_location) = &self.upload_location {
            upload_location
        } else {
            return Ok(());
        };

        let bs = self
            .prev_upload_chunk
            .as_ref()
            .expect("failed to get the previously uploaded chunk");

        let copied_prev_chunk = bs.slice(0..bs.len());

        let resp = self
            .backend
            .gcs_complete_resumable_upload(
                upload_location,
                self.already_uploaded_chunk,
                copied_prev_chunk,
            )
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
