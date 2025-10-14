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

use http::StatusCode;

use super::core::*;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct WebdavWriter {
    core: Arc<WebdavCore>,

    op: OpWrite,
    path: String,
}

impl WebdavWriter {
    pub fn new(core: Arc<WebdavCore>, op: OpWrite, path: String) -> Self {
        WebdavWriter { core, op, path }
    }

    fn parse_metadata(headers: &http::HeaderMap) -> Result<Metadata> {
        let mut meta = Metadata::default();

        if let Some(etag) = parse_etag(headers)? {
            meta.set_etag(etag);
        }

        if let Some(last_modified) = parse_last_modified(headers)? {
            meta.set_last_modified(last_modified);
        }

        Ok(meta)
    }
}

impl oio::OneShotWrite for WebdavWriter {
    async fn write_once(&self, bs: Buffer) -> Result<Metadata> {
        let resp = self
            .core
            .webdav_put(&self.path, Some(bs.len() as u64), &self.op, bs)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK | StatusCode::NO_CONTENT => {
                let meta = WebdavWriter::parse_metadata(resp.headers())?;
                Ok(meta)
            }
            _ => Err(parse_error(resp)),
        }
    }
}
