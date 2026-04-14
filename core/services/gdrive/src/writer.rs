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

use bytes::Buf;
use http::StatusCode;

use super::core::GdriveCore;
use super::core::GdriveFile;
use super::error::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

pub struct GdriveWriter {
    core: Arc<GdriveCore>,

    path: String,

    file_id: Option<String>,
}

impl GdriveWriter {
    pub fn new(core: Arc<GdriveCore>, path: String, file_id: Option<String>) -> Self {
        GdriveWriter {
            core,
            path,

            file_id,
        }
    }
}

impl oio::OneShotWrite for GdriveWriter {
    async fn write_once(&self, bs: Buffer) -> Result<Metadata> {
        let size = bs.len();

        let mut current_file_id = self.file_id.clone();
        let mut retried = false;

        loop {
            let resp = if let Some(file_id) = &current_file_id {
                self.core
                    .gdrive_upload_overwrite_simple_request(file_id, size as u64, bs.clone())
                    .await
            } else {
                self.core
                    .gdrive_upload_simple_request(&self.path, size as u64, bs.clone())
                    .await
            }?;

            match resp.status() {
                StatusCode::OK | StatusCode::CREATED => {
                    let mut metadata =
                        Metadata::new(EntryMode::FILE).with_content_length(size as u64);

                    if current_file_id.is_none() {
                        let bs = resp.into_body();
                        let file: GdriveFile = serde_json::from_reader(bs.reader())
                            .map_err(new_json_deserialize_error)?;
                        metadata = metadata.with_content_type(file.mime_type);
                        self.core.cache_file_id(&self.path, &file.id).await;
                    }
                    self.core.record_recent_upsert(&self.path, metadata).await;
                    return Ok(Metadata::default());
                }
                StatusCode::NOT_FOUND if !retried && current_file_id.is_some() => {
                    retried = true;
                    self.core.refresh_path(&self.path).await;
                    current_file_id = self.core.resolve_path(&self.path).await?;
                    continue;
                }
                _ => return Err(parse_error(resp)),
            }
        }
    }
}
