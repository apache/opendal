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

use super::core::ChainsafeCore;
use super::error::parse_error;
use crate::raw::*;
use http::StatusCode;

use std::sync::Arc;

pub struct ChainsafeReader {
    core: Arc<ChainsafeCore>,

    path: String,
    op: OpRead,
}

impl ChainsafeReader {
    pub fn new(core: Arc<ChainsafeCore>, path: &str, op: OpRead) -> Self {
        ChainsafeReader {
            core,
            path: path.to_string(),
            op,
        }
    }
}

impl oio::Read for ChainsafeReader {
    async fn read_at(&self, offset: u64, limit: usize) -> crate::Result<oio::Buffer> {
        let range = BytesRange::new(offset, Some(limit as u64));

        let resp = self.core.download_object(&self.path, range).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(resp.into_body()),
            StatusCode::RANGE_NOT_SATISFIABLE => Ok(oio::Buffer::new()),
            _ => Err(parse_error(resp).await?),
        }
    }
}
