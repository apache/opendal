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

use base64::engine::general_purpose;
use base64::Engine;
use bytes::Bytes;
use serde::Deserialize;

use super::core::DbfsCore;
use crate::raw::*;
use crate::*;

// The number of bytes to read starting from the offset. This has a limit of 1 MB
// Reference: https://docs.databricks.com/api/azure/workspace/dbfs/read
// const DBFS_READ_LIMIT: usize = 1024 * 1024;

#[allow(dead_code)]
pub struct DbfsReader {
    core: Arc<DbfsCore>,
    path: String,
    has_filled: u64,
}

impl DbfsReader {
    pub fn new(core: Arc<DbfsCore>, path: String) -> Self {
        DbfsReader {
            core,
            path,
            has_filled: 0,
        }
    }

    #[allow(dead_code)]
    fn serde_json_decode(&self, bs: &Bytes) -> Result<Bytes> {
        let response_body = match serde_json::from_slice::<ReadContentJsonResponse>(bs) {
            Ok(v) => v,
            Err(err) => {
                return Err(
                    Error::new(ErrorKind::Unexpected, "parse response content failed")
                        .with_operation("http_util::IncomingDbfsAsyncBody::poll_read")
                        .set_source(err),
                );
            }
        };

        let decoded_data = general_purpose::STANDARD
            .decode(response_body.data)
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "decode response content failed")
                    .with_operation("http_util::IncomingDbfsAsyncBody::poll_read")
                    .set_source(err)
            })?;

        Ok(decoded_data.into())
    }
}

/// # Safety
///
/// We will only take `&mut Self` reference for DbfsReader.
unsafe impl Sync for DbfsReader {}

impl oio::Read for DbfsReader {
    async fn read_at(&self, _buf: oio::WritableBuf, _offset: u64) -> Result<usize> {
        todo!()
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ReadContentJsonResponse {
    bytes_read: u64,
    data: String,
}
