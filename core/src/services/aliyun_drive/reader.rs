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

use http::{header, Request, StatusCode};

use crate::raw::*;
use crate::*;

use super::core::AliyunDriveCore;
use super::error::parse_error;

pub struct AliyunDriveReader {
    core: Arc<AliyunDriveCore>,

    download_url: String,
    size: u64,
    _op: OpRead,
}

impl AliyunDriveReader {
    pub fn new(core: Arc<AliyunDriveCore>, download_url: &str, size: u64, op: OpRead) -> Self {
        AliyunDriveReader {
            core,
            download_url: download_url.to_string(),
            size,
            _op: op,
        }
    }
}

impl oio::Read for AliyunDriveReader {
    async fn read_at(&self, offset: u64, size: usize) -> Result<Buffer> {
        // AliyunDrive responds with status OK even if the range is not statisfiable.
        // and then the whole file will be read.
        let limit = if offset >= self.size {
            return Ok(Buffer::new());
        } else if offset + (size as u64) - 1 > self.size {
            self.size - offset
        } else {
            size as u64
        };
        let range = BytesRange::new(offset, Some(limit));
        let req = Request::get(self.download_url.as_str())
            .header(header::RANGE, range.to_header())
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let res = self.core.client.send(req).await?;
        let status = res.status();
        match status {
            StatusCode::OK => Ok(Buffer::new()),
            StatusCode::PARTIAL_CONTENT => Ok(res.into_body()),
            _ => Err(parse_error(res).await?),
        }
    }
}
