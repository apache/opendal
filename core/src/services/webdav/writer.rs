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

use super::backend::WebdavBackend;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct WebdavWriter {
    backend: WebdavBackend,

    op: OpWrite,
    path: String,
}

impl WebdavWriter {
    pub fn new(backend: WebdavBackend, op: OpWrite, path: String) -> Self {
        WebdavWriter { backend, op, path }
    }

    async fn write_oneshot(&mut self, size: u64, body: AsyncBody) -> Result<()> {
        let resp = self
            .backend
            .webdav_put(
                &self.path,
                Some(size),
                self.op.content_type(),
                self.op.content_disposition(),
                body,
            )
            .await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK | StatusCode::NO_CONTENT => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}

#[async_trait]
impl oio::Write for WebdavWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.write_oneshot(bs.len() as u64, AsyncBody::Bytes(bs))
            .await
    }

    async fn sink(&mut self, size: u64, s: oio::Streamer) -> Result<()> {
        self.write_oneshot(size, AsyncBody::Stream(s)).await
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
