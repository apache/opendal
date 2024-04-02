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

use super::core::GcsCore;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub type GcsWriters = oio::RangeWriter<GcsWriter>;

pub struct GcsWriter {
    core: Arc<GcsCore>,
    path: String,
    op: OpWrite,
}

impl GcsWriter {
    pub fn new(core: Arc<GcsCore>, path: &str, op: OpWrite) -> Self {
        GcsWriter {
            core,
            path: path.to_string(),
            op,
        }
    }
}

impl oio::RangeWrite for GcsWriter {
    async fn write_once(&self, size: u64, body: RequestBody) -> Result<()> {
        let mut req = self.core.gcs_insert_object_request(
            &percent_encode_path(&self.path),
            Some(size),
            &self.op,
            body,
        )?;

        self.core.sign(&mut req).await?;

        let (parts, body) = self.core.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::CREATED | StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    async fn initiate_range(&self) -> Result<String> {
        self.core.gcs_initiate_resumable_upload(&self.path).await
    }

    async fn write_range(
        &self,
        location: &str,
        written: u64,
        size: u64,
        body: RequestBody,
    ) -> Result<()> {
        self.core
            .gcs_upload_in_resumable_upload(location, size, written, body)
            .await
    }

    async fn complete_range(
        &self,
        location: &str,
        written: u64,
        size: u64,
        body: RequestBody,
    ) -> Result<()> {
        self.core
            .gcs_complete_resumable_upload(location, written, size, body)
            .await
    }

    async fn abort_range(&self, location: &str) -> Result<()> {
        self.core.gcs_abort_resumable_upload(location).await
    }
}
