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
use crate::raw::*;
use crate::*;

pub struct GcsWriter {
    core: Arc<GcsCore>,
    path: String,
    op: OpWrite,

    location: Option<String>,
    written: u64,
    buffer: oio::VectorCursor,
    write_fixed_size: usize,
}

impl GcsWriter {
    pub fn new(core: Arc<GcsCore>, path: &str, op: OpWrite) -> Self {
        let write_fixed_size = core.write_fixed_size;
        GcsWriter {
            core,
            path: path.to_string(),
            op,

            location: None,
            written: 0,
            buffer: oio::VectorCursor::new(),
            write_fixed_size,
        }
    }

    async fn write_oneshot(&self, size: u64, body: AsyncBody) -> Result<()> {
        let mut req = self.core.gcs_insert_object_request(
            &percent_encode_path(&self.path),
            Some(size),
            self.op.content_type(),
            body,
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

    async fn initiate_upload(&self) -> Result<String> {
        let resp = self.core.gcs_initiate_resumable_upload(&self.path).await?;
        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = parse_location(resp.headers())?;
                if let Some(location) = bs {
                    Ok(location.to_string())
                } else {
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        "location is not in the response header",
                    ))
                }
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write_part(&self, location: &str, bs: Bytes) -> Result<()> {
        let mut req = self.core.gcs_upload_in_resumable_upload(
            location,
            bs.len() as u64,
            self.written,
            false,
            AsyncBody::Bytes(bs),
        )?;

        self.core.sign(&mut req).await?;

        let resp = self.core.send(req).await?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PERMANENT_REDIRECT => Ok(()),
            _ => Err(parse_error(resp).await?),
        }
    }
}

#[async_trait]
impl oio::Write for GcsWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let location = match &self.location {
            Some(location) => location,
            None => {
                if self.op.content_length().unwrap_or_default() == bs.len() as u64
                    && self.written == 0
                {
                    return self
                        .write_oneshot(bs.len() as u64, AsyncBody::Bytes(bs))
                        .await;
                } else {
                    let location = self.initiate_upload().await?;
                    self.location = Some(location);
                    self.location.as_deref().unwrap()
                }
            }
        };

        // Ignore empty bytes
        if bs.is_empty() {
            return Ok(());
        }

        self.buffer.push(bs);
        // Return directly if the buffer is not full
        if self.buffer.len() <= self.write_fixed_size {
            return Ok(());
        }

        let bs = self.buffer.peak_exact(self.write_fixed_size);

        match self.write_part(location, bs).await {
            Ok(_) => {
                self.buffer.take(self.write_fixed_size);
                self.written += self.write_fixed_size as u64;
                Ok(())
            }
            Err(e) => {
                // If the upload fails, we should pop the given bs to make sure
                // write is re-enter safe.
                self.buffer.pop();
                Err(e)
            }
        }
    }

    async fn sink(&mut self, size: u64, s: oio::Streamer) -> Result<()> {
        self.write_oneshot(size, AsyncBody::Stream(s)).await
    }

    async fn abort(&mut self) -> Result<()> {
        let location = if let Some(location) = &self.location {
            location
        } else {
            return Ok(());
        };

        let resp = self.core.gcs_abort_resumable_upload(location).await?;

        match resp.status().as_u16() {
            // gcs returns 499 if the upload aborted successfully
            // reference: https://cloud.google.com/storage/docs/performing-resumable-uploads#cancel-upload-json
            499 => {
                resp.into_body().consume().await?;
                self.location = None;
                self.buffer.clear();
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn close(&mut self) -> Result<()> {
        let location = if let Some(location) = &self.location {
            location
        } else {
            return Ok(());
        };

        let bs = self.buffer.peak_exact(self.buffer.len());

        let resp = self
            .core
            .gcs_complete_resumable_upload(location, self.written, bs)
            .await?;

        let status = resp.status();
        match status {
            StatusCode::OK => {
                resp.into_body().consume().await?;

                self.location = None;
                self.buffer.clear();
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}
