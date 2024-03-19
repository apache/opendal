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

use super::error::{parse_error, parse_error_msg};
use crate::raw::{oio, OpRead, RpRead};
use crate::services::webhdfs::backend::WebhdfsBackend;
use bytes::Buf;
use http::StatusCode;
use std::future::Future;
use std::sync::Arc;

pub struct WebhdfsReader {
    core: WebhdfsBackend,

    path: String,
    op: OpRead,
}

impl WebhdfsReader {
    pub fn new(core: WebhdfsBackend, path: &str, op: OpRead) -> Self {
        WebhdfsReader {
            core,
            path: path.to_string(),
            op: op,
        }
    }
}

impl oio::Read for WebhdfsReader {
    async fn read_at(&self, offset: u64, limit: usize) -> crate::Result<oio::Buffer> {
        let Some(range) = self.op.range().apply_on_offset(offset, limit) else {
            return Ok(oio::Buffer::new());
        };

        let resp = self.core.webhdfs_read_file(&self.path, range).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(resp.into_body()),
            // WebHDFS will returns 403 when range is outside of the end.
            StatusCode::FORBIDDEN => {
                let (parts, mut body) = resp.into_parts();
                let bs = body.copy_to_bytes(body.remaining());
                let s = String::from_utf8_lossy(&bs);
                if s.contains("out of the range") {
                    Ok(oio::Buffer::new())
                } else {
                    Err(parse_error_msg(parts, &s)?)
                }
            }
            StatusCode::RANGE_NOT_SATISFIABLE => Ok(oio::Buffer::new()),
            _ => Err(parse_error(resp).await?),
        }
    }
}
