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
use http::StatusCode;

use super::core::SwiftCore;
use super::error::parse_error;
use crate::raw::oio::WriteBuf;
use crate::raw::*;
use crate::*;

pub type SwiftWriters =
    oio::TwoWaysWriter<oio::OneShotWriter<SwiftWriter>, oio::AppendObjectWriter<SwiftWriter>>;

pub struct SwiftWriter {
    core: Arc<SwiftCore>,
    path: String,
}

impl SwiftWriter {
    pub fn new(core: Arc<SwiftCore>, _op: OpWrite, path: String) -> Self {
        SwiftWriter { core, path }
    }
}

#[async_trait]
impl oio::OneShotWrite for SwiftWriter {
    async fn write_once(&self, bs: &dyn WriteBuf) -> Result<()> {
        let bs = bs.bytes(bs.remaining());

        let resp = self
            .core
            .swift_create_file(&self.path, false, 0, AsyncBody::Bytes(bs))
            .await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}

#[async_trait]
impl oio::AppendObjectWrite for SwiftWriter {
    async fn offset(&self) -> Result<u64> {
        let resp = self.core.swift_get_metadata(&self.path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(parse_content_length(resp.headers())?.unwrap_or_default()),
            StatusCode::NOT_FOUND => Ok(0),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn append(&self, offset: u64, size: u64, body: AsyncBody) -> Result<()> {
        // create the manifest file for Dynamic Large Object (DLO).
        if offset == 0 {
            let resp = self
                .core
                .swift_create_file(&self.path, true, offset, AsyncBody::Empty)
                .await?;

            let status = resp.status();

            if !matches!(status, StatusCode::OK | StatusCode::CREATED) {
                return Err(parse_error(resp)
                    .await?
                    .with_operation("Backend::swift_create_file"));
            }
        }

        let end_offset = offset + size;
        let resp = self
            .core
            .swift_create_file(&self.path, true, end_offset, body)
            .await?;

        return match resp.status() {
            StatusCode::OK | StatusCode::CREATED => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp)
                .await?
                .with_operation("Backend::swift_create_file")),
        };
    }
}
