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

use super::core::B2Core;
use super::core::StartLargeFileResponse;
use super::core::UploadPartResponse;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub type B2Writers = oio::MultipartWriter<B2Writer>;

pub struct B2Writer {
    core: Arc<B2Core>,

    op: OpWrite,
    path: String,
}

impl B2Writer {
    pub fn new(core: Arc<B2Core>, path: &str, op: OpWrite) -> Self {
        B2Writer {
            core,
            path: path.to_string(),
            op,
        }
    }
}

impl oio::MultipartWrite for B2Writer {
    async fn write_once(&self, size: u64, body: RequestBody) -> Result<()> {
        self.core
            .upload_file(&self.path, Some(size), &self.op, body)
            .await
    }

    async fn initiate_part(&self) -> Result<String> {
        self.core.start_large_file(&self.path, &self.op).await
    }

    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: RequestBody,
    ) -> Result<oio::MultipartPart> {
        // B2 requires part number must between [1..=10000]
        let part_number = part_number + 1;

        let result = self
            .core
            .upload_part(upload_id, part_number, size, body)
            .await?;
        Ok(oio::MultipartPart {
            etag: result.content_sha1,
            part_number,
        })
    }

    async fn complete_part(&self, upload_id: &str, parts: &[oio::MultipartPart]) -> Result<()> {
        let part_sha1_array = parts
            .iter()
            .map(|p| {
                let binding = p.etag.clone();
                let sha1 = binding.strip_prefix("unverified:");
                let Some(sha1) = sha1 else {
                    return "".to_string();
                };
                sha1.to_string()
            })
            .collect();

        self.core
            .finish_large_file(upload_id, part_sha1_array)
            .await
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        self.core.cancel_large_file(upload_id).await
    }
}
