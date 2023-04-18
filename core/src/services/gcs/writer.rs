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

use super::core::GcsCore;
use super::error::parse_error;
use crate::ops::OpWrite;
use crate::raw::*;
use crate::*;

pub struct GcsWriter {
    core: Arc<GcsCore>,

    op: OpWrite,
    path: String,
    location: Option<String>,
    written_bytes: u64,
    is_last_part_written: bool,
    last: Option<Bytes>,
}

impl GcsWriter {
    pub fn new(
        core: Arc<GcsCore>,
        op: OpWrite,
        path: String,
        upload_location: Option<String>,
    ) -> Self {
        GcsWriter {
            core,
            op,
            path,
            location: upload_location,
            written_bytes: 0,
            is_last_part_written: false,
            last: None,
        }
    }
}

#[async_trait]
impl oio::Write for GcsWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let mut req = self.core.gcs_insert_object_request(
            &percent_encode_path(&self.path),
            Some(bs.len()),
            self.op.content_type(),
            AsyncBody::Bytes(bs),
        )?;

        self.core.sign(&mut req).await?;

        let resp = self.core.send(req).await?;

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
        let location = if let Some(location) = &self.location {
            location
        } else {
            return Ok(());
        };

        let result = if let Some(last) = &self.last {
            let bytes_to_upload = last.slice(0..last.len());
            let part_size = bytes_to_upload.len() as u64;
            let is_last_part = part_size % (256 * 1024) != 0;
            let mut req = self.core.gcs_upload_in_resumable_upload(
                location,
                part_size,
                self.written_bytes,
                is_last_part,
                AsyncBody::Bytes(bytes_to_upload),
            )?;

            self.core.sign(&mut req).await?;

            let resp = self.core.send(req).await?;

            let status = resp.status();

            match status {
                StatusCode::OK | StatusCode::PERMANENT_REDIRECT => {
                    if is_last_part {
                        self.is_last_part_written = true
                    } else {
                        self.written_bytes += part_size;
                    }
                    Ok(())
                }
                _ => Err(parse_error(resp).await?),
            }
        } else {
            Ok(())
        };

        self.last = Some(bs.slice(0..bs.len()));
        return result;
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.is_last_part_written {
            return Ok(());
        }

        let location = if let Some(location) = &self.location {
            location
        } else {
            return Ok(());
        };

        let bs = self
            .last
            .as_ref()
            .expect("failed to get the previously uploaded part");

        let resp = self
            .core
            .gcs_complete_resumable_upload(location, self.written_bytes, bs.slice(0..bs.len()))
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
