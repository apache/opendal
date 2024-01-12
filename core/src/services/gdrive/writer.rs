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

use super::core::GdriveCore;
use super::error::parse_error;
use crate::raw::oio::WriteBuf;
use crate::raw::*;
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
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl oio::OneShotWrite for GdriveWriter {
    async fn write_once(&self, bs: &dyn WriteBuf) -> Result<()> {
        let bs = bs.bytes(bs.remaining());
        let size = bs.len();

        let resp = if let Some(file_id) = &self.file_id {
            self.core
                .gdrive_upload_overwrite_simple_request(file_id, size as u64, bs)
                .await
        } else {
            self.core
                .gdrive_upload_simple_request(&self.path, size as u64, bs)
                .await
        }?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::CREATED => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}
