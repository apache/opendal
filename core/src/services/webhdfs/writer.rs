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
use http::StatusCode;

use super::backend::WebhdfsBackend;
use super::error::parse_error;
use crate::raw::oio::WriteBuf;
use crate::raw::*;
use crate::*;

pub type WebhdfsWriters =
    TwoWays<oio::OneShotWriter<WebhdfsWriter>, oio::AppendObjectWriter<WebhdfsWriter>>;

pub struct WebhdfsWriter {
    backend: WebhdfsBackend,

    op: OpWrite,
    path: String,
}

impl WebhdfsWriter {
    pub fn new(backend: WebhdfsBackend, op: OpWrite, path: String) -> Self {
        WebhdfsWriter { backend, op, path }
    }
}

#[async_trait]
impl oio::OneShotWrite for WebhdfsWriter {
    /// Using `bytes` instead of `vectored_bytes` to allow request to be redirected.
    async fn write_once(&self, bs: &dyn WriteBuf) -> Result<()> {
        let bs = bs.bytes(bs.remaining());

        let req = self.backend.webhdfs_create_object_request(
            &self.path,
            Some(bs.len()),
            &self.op,
            AsyncBody::Bytes(bs),
        )?;

        let resp = self.backend.client.send(req).await?;

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
impl oio::AppendObjectWrite for WebhdfsWriter {
    async fn offset(&self) -> Result<u64> {
        Ok(0)
    }

    async fn append(&self, _offset: u64, size: u64, body: AsyncBody) -> Result<()> {
        let resp = self.backend.webhdfs_get_file_status(&self.path).await?;

        let status = resp.status();

        let location;

        match status {
            StatusCode::OK => {
                location = self.backend.webhdfs_init_append_request(&self.path).await?;
            }
            StatusCode::NOT_FOUND => {
                let req = self.backend.webhdfs_create_object_request(
                    &self.path,
                    None,
                    &self.op,
                    AsyncBody::Empty,
                )?;

                let resp = self.backend.client.send(req).await?;

                let status = resp.status();

                match status {
                    StatusCode::CREATED | StatusCode::OK => {
                        resp.into_body().consume().await?;

                        location = self.backend.webhdfs_init_append_request(&self.path).await?;
                    }
                    _ => return Err(parse_error(resp).await?),
                }
            }
            _ => return Err(parse_error(resp).await?),
        }

        let req = self
            .backend
            .webhdfs_append_request(&location, size, body)
            .await?;

        let resp = self.backend.client.send(req).await?;

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
