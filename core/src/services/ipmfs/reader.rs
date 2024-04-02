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

use http::StatusCode;

use super::error::parse_error;
use crate::raw::*;
use crate::services::ipmfs::backend::IpmfsBackend;

pub struct IpmfsReader {
    core: IpmfsBackend,

    path: String,
    _op: OpRead,
}

impl IpmfsReader {
    pub fn new(core: IpmfsBackend, path: &str, op: OpRead) -> Self {
        IpmfsReader {
            core,
            path: path.to_string(),
            _op: op,
        }
    }
}

impl oio::Read for IpmfsReader {
    async fn read_at(&self, buf: oio::WritableBuf, offset: u64) -> crate::Result<usize> {
        let range = BytesRange::new(offset, Some(limit as u64));

        let resp = self.core.ipmfs_read(&self.path, range).await?;

        let status = resp.status();

        match parts.status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(resp.into_body()),
            StatusCode::RANGE_NOT_SATISFIABLE => Ok(oio::Buffer::new()),
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }
}
