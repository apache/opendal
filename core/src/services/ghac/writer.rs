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

use super::backend::GhacBackend;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct GhacWriter {
    backend: GhacBackend,

    cache_id: i64,
    size: u64,
}

impl GhacWriter {
    pub fn new(backend: GhacBackend, cache_id: i64) -> Self {
        GhacWriter {
            backend,
            cache_id,
            size: 0,
        }
    }
}

impl oio::Write for GhacWriter {
    async fn write(&mut self, bs: oio::ReadableBuf) -> Result<usize> {
        let size = bs.len();
        let offset = self.size;

        let req = self
            .backend
            .ghac_upload(
                self.cache_id,
                offset,
                size as u64,
                RequestBody::Bytes(bs.to_bytes()),
            )
            .await?;

        let (parts, body) = self.backend.client.send(req).await?.into_parts();

        if parts.status.is_success() {
            body.consume().await?;
            self.size += size as u64;
            Ok(size)
        } else {
            let bs = body.to_bytes().await?;
            Err(parse_error(parts, bs).map(|err| err.with_operation("Backend::ghac_upload"))?)
        }
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let req = self.backend.ghac_commit(self.cache_id, self.size).await?;
        let (parts, body) = self.backend.client.send(req).await?.into_parts();

        if parts.status.is_success() {
            body.consume().await?;
            Ok(())
        } else {
            let bs = body.to_bytes().await?;
            Err(parse_error(parts, bs).map(|err| err.with_operation("Backend::ghac_commit"))?)
        }
    }
}
