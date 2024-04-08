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

use bytes::BufMut;
use http::StatusCode;

use super::error::parse_error;
use crate::raw::*;
use crate::services::ghac::backend::GhacBackend;
use crate::*;

pub struct GhacReader {
    core: GhacBackend,

    location: String,
    _op: OpRead,
}

impl GhacReader {
    pub fn new(core: GhacBackend, location: &str, op: OpRead) -> Self {
        GhacReader {
            core,
            location: location.to_string(),
            _op: op,
        }
    }

    async fn read(&self, range: BytesRange, buf: &mut oio::WritableBuf) -> Result<usize> {
        let req = self.core.ghac_get_location(&self.location, range).await?;
        let (parts, body) = self.core.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => body.read(buf).await,
            StatusCode::RANGE_NOT_SATISFIABLE => {
                body.consume().await?;
                Ok(0)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }
}

impl oio::Read for GhacReader {
    async fn read_at(&self, buf: &mut oio::WritableBuf, offset: u64) -> Result<usize> {
        let range = BytesRange::new(offset, Some(buf.remaining_mut() as u64));

        self.read(range, buf).await
    }
}
