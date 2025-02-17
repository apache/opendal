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

use super::core::*;
use super::error::parse_error;
use crate::raw::*;
use crate::*;
use std::sync::Arc;

pub struct GhacWriter {
    core: Arc<GhacCore>,

    cache_id: i64,
    size: u64,
}

impl GhacWriter {
    pub fn new(core: Arc<GhacCore>, cache_id: i64) -> Self {
        GhacWriter {
            core,
            cache_id,
            size: 0,
        }
    }
}

impl oio::Write for GhacWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let size = bs.len();
        let offset = self.size;

        let req = self.core.ghac_upload(
            self.cache_id,
            offset,
            size as u64,
            Buffer::from(bs.to_bytes()),
        )?;

        let resp = self.core.client.send(req).await?;

        if !resp.status().is_success() {
            return Err(parse_error(resp).map(|err| err.with_operation("Backend::ghac_upload")));
        }

        self.size += size as u64;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let req = self.core.ghac_commit(self.cache_id, self.size)?;
        let resp = self.core.client.send(req).await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(parse_error(resp).map(|err| err.with_operation("Backend::ghac_commit")))
        }
    }
}
