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

use super::error::parse_error;
use crate::raw::oio::WriteBuf;
use crate::raw::*;
use crate::services::dbfs::core::DbfsCore;
use crate::*;

pub struct DbfsWriter {
    core: Arc<DbfsCore>,
    path: String,
}

impl DbfsWriter {
    const MAX_SIMPLE_SIZE: usize = 1024 * 1024;

    pub fn new(core: Arc<DbfsCore>, _op: OpWrite, path: String) -> Self {
        DbfsWriter { core, path }
    }
}

#[async_trait]
impl oio::OneShotWrite for DbfsWriter {
    async fn write_once(&self, bs: Bytes) -> Result<()> {
        let size = bs.len();

        // MAX_BLOCK_SIZE_EXCEEDED will be thrown if this limit(1MB) is exceeded.
        if size >= Self::MAX_SIMPLE_SIZE {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "AppendWrite has not been implemented for Dbfs",
            ));
        }

        let req = self.core.dbfs_create_file_request(&self.path, bs)?;

        let resp = self.core.client.send(req).await?;

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
