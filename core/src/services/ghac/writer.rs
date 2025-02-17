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

use super::core::*;
use super::error::parse_error;
use crate::raw::*;
use crate::*;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE};
use http::Request;
use std::sync::Arc;

pub struct GhacWriter {
    core: Arc<GhacCore>,

    path: String,
    url: String,
    size: u64,
}

impl GhacWriter {
    pub fn new(core: Arc<GhacCore>, path: String, url: String) -> Self {
        GhacWriter {
            core,
            path,
            url,
            size: 0,
        }
    }
}

impl oio::Write for GhacWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let size = bs.len() as u64;
        let offset = self.size;

        let mut req = Request::patch(&self.url);
        req = req.header(AUTHORIZATION, format!("Bearer {}", self.core.catch_token));
        req = req.header(ACCEPT, CACHE_HEADER_ACCEPT);
        req = req.header(CONTENT_LENGTH, size);
        req = req.header(CONTENT_TYPE, "application/octet-stream");
        req = req.header(
            CONTENT_RANGE,
            BytesContentRange::default()
                .with_range(offset, offset + size - 1)
                .to_header(),
        );
        let req = req.body(bs).map_err(new_request_build_error)?;

        let resp = self.core.http_client.send(req).await?;
        if !resp.status().is_success() {
            return Err(parse_error(resp).map(|err| err.with_operation("Backend::ghac_upload")));
        }
        self.size += size;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.core
            .ghac_finalize_upload(&self.path, &self.url, self.size)
            .await
    }
}
