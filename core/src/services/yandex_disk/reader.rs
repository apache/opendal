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
use std::sync::Arc;

use http::header;
use http::Request;
use http::StatusCode;

use super::core::YandexDiskCore;
use super::error::parse_error;
use crate::raw::*;

pub struct YandexDiskReader {
    core: Arc<YandexDiskCore>,

    path: String,
    _op: OpRead,
}

impl YandexDiskReader {
    pub fn new(core: Arc<YandexDiskCore>, path: &str, op: OpRead) -> Self {
        YandexDiskReader {
            core,
            path: path.to_string(),
            _op: op,
        }
    }
}

impl oio::Read for YandexDiskReader {
    async fn read_at(&self, buf: oio::WritableBuf, offset: u64) -> crate::Result<usize> {
        let range = BytesRange::new(offset, Some(buf.remaining_mut() as u64));

        // TODO: move this out of reader.
        let download_url = self.core.get_download_url(&self.path).await?;

        let req = Request::get(download_url)
            .header(header::RANGE, range.to_header())
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
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
