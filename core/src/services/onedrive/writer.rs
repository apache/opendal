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

use async_trait::async_trait;
use bytes::Bytes;
use http::StatusCode;

use super::backend::OnedriveBackend;
use super::error::parse_error;
use crate::ops::OpWrite;
use crate::raw::*;
use crate::*;

pub struct OneDriveWriter {
    backend: OnedriveBackend,

    op: OpWrite,
    path: String,
}

impl OneDriveWriter {
    pub fn new(backend: OnedriveBackend, op: OpWrite, path: String) -> Self {
        OneDriveWriter { backend, op, path }
    }
}

#[async_trait]
impl oio::Write for OneDriveWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let resp = self
            .backend
            .onedrive_put(
                &self.path,
                Some(bs.len()),
                self.op.content_type(),
                AsyncBody::Bytes(bs),
            )
            .await?;

        let status = resp.status();

        match status {
            // Typical response code: 201 Created
            // Reference: https://learn.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_put_content?view=odsp-graph-online#response
            StatusCode::CREATED => {
                resp.into_body().consume().await?;
                Ok(())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
