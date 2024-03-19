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

use super::error::parse_error;
use crate::raw::{oio, OpRead};
use crate::services::ipfs::backend::IpfsBackend;
use http::StatusCode;
use std::future::Future;
use std::sync::Arc;

pub struct IpfsReader {
    core: IpfsBackend,

    path: String,
    op: OpRead,
}

impl IpfsReader {
    pub fn new(core: IpfsBackend, path: &str, op: OpRead) -> Self {
        IpfsReader {
            core,
            path: path.to_string(),
            op: op,
        }
    }
}

impl oio::Read for IpfsReader {
    async fn read_at(&self, offset: u64, limit: usize) -> crate::Result<oio::Buffer> {
        let Some(range) = self.op.range().apply_on_offset(offset, limit) else {
            return Ok(oio::Buffer::new());
        };

        let resp = self.core.ipfs_get(&self.path, range).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(resp.into_body()),
            StatusCode::RANGE_NOT_SATISFIABLE => Ok(oio::Buffer::new()),
            _ => Err(parse_error(resp).await?),
        }
    }
}