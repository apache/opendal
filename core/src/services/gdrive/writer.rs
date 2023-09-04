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

use super::core::GdriveCore;
use super::error::parse_error;
use crate::raw::*;
use crate::services::gdrive::core::GdriveFile;
use crate::*;

pub struct GdriveWriter {
    core: Arc<GdriveCore>,

    path: String,

    file_id: Option<String>,
}

impl GdriveWriter {
    pub fn new(core: Arc<GdriveCore>, path: String, file_id: Option<String>) -> Self {
        GdriveWriter {
            core,
            path,

            file_id,
        }
    }

    /// Write a single chunk of data to the object.
    ///
    /// This is used for small objects.
    /// And should overwrite the object if it already exists.
    pub async fn write_create(&mut self, size: u64, body: Bytes) -> Result<()> {
        let resp = self
            .core
            .gdrive_upload_simple_request(&self.path, size, body)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::CREATED => {
                let bs = resp.into_body().bytes().await?;

                let file = serde_json::from_slice::<GdriveFile>(&bs)
                    .map_err(new_json_deserialize_error)?;

                self.file_id = Some(file.id);

                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    pub async fn write_overwrite(&self, size: u64, body: Bytes) -> Result<()> {
        let file_id = self.file_id.as_ref().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "file_id is required for overwrite")
        })?;
        let resp = self
            .core
            .gdrive_upload_overwrite_simple_request(file_id, size, body)
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

#[async_trait]
impl oio::Write for GdriveWriter {
    async fn write(&mut self, bs: Bytes) -> Result<u64> {
        let size = bs.len() as u64;
        if self.file_id.is_none() {
            self.write_create(size, bs).await?;
        } else {
            self.write_overwrite(size, bs).await?;
        }

        Ok(size)
    }

    async fn sink(&mut self, _size: u64, _s: oio::Streamer) -> Result<u64> {
        Err(Error::new(ErrorKind::Unsupported, "sink is not supported"))
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
