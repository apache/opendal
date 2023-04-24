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

use super::core::*;
use super::error::parse_error;
use crate::ops::OpWrite;
use crate::raw::*;
use crate::*;

pub struct WasabiWriter {
    core: Arc<WasabiCore>,

    op: OpWrite,
    path: String,
}

impl WasabiWriter {
    pub fn new(core: Arc<WasabiCore>, op: OpWrite, path: String) -> Self {
        WasabiWriter { core, op, path }
    }
}

#[async_trait]
impl oio::Write for WasabiWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        if self.op.content_length().unwrap_or_default() == bs.len() as u64 {
            let resp = self
                .core
                .put_object(
                    &self.path,
                    Some(bs.len()),
                    self.op.content_type(),
                    self.op.content_disposition(),
                    self.op.cache_control(),
                    AsyncBody::Bytes(bs),
                )
                .await?;

            match resp.status() {
                StatusCode::CREATED | StatusCode::OK => {
                    resp.into_body().consume().await?;
                    Ok(())
                }
                _ => Err(parse_error(resp).await?),
            }
        } else {
            let resp = self
                .core
                .append_object(&self.path, Some(bs.len()), AsyncBody::Bytes(bs))
                .await?;

            match resp.status() {
                StatusCode::CREATED | StatusCode::OK | StatusCode::NO_CONTENT => {
                    resp.into_body().consume().await?;
                    Ok(())
                }
                _ => Err(parse_error(resp).await?),
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
