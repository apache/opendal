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

use super::core::GithubCore;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub type GithubWriters = oio::OneShotWriter<GithubWriter>;

pub struct GithubWriter {
    core: Arc<GithubCore>,
    path: String,
}

impl GithubWriter {
    pub fn new(core: Arc<GithubCore>, path: String) -> Self {
        GithubWriter { core, path }
    }

    fn parse_metadata(headers: &http::HeaderMap, body: &[u8]) -> Result<Metadata> {
        let mut metadata = Metadata::default();

        if let Some(last_modified) = parse_last_modified(headers)? {
            metadata.set_last_modified(last_modified);
        }

        if let Some(etag) = parse_etag(headers)? {
            metadata.set_etag(etag);
        }

        if let Ok(body_str) = std::str::from_utf8(body) {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(body_str) {
                if let Some(content) = json.get("content") {
                    if let Some(sha) = content.get("sha").and_then(|v| v.as_str()) {
                        metadata.set_version(sha);
                    }
                } else if let Some(sha) = json.get("sha").and_then(|v| v.as_str()) {
                    metadata.set_version(sha);
                }
            }
        }

        Ok(metadata)
    }
}

impl oio::OneShotWrite for GithubWriter {
    async fn write_once(&self, bs: Buffer) -> Result<Metadata> {
        let resp = self.core.upload(&self.path, bs).await?;

        let status = resp.status();
        let headers = resp.headers().clone();

        match status {
            StatusCode::OK | StatusCode::CREATED => {
                let body = resp.into_body().to_bytes();
                let meta = Self::parse_metadata(&headers, &body)?;
                Ok(meta)
            }
            _ => Err(parse_error(resp)),
        }
    }
}
