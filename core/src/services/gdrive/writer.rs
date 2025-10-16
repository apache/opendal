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
use crate::raw::*;
use crate::*;

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

    fn parse_metadata(file: GdriveFile) -> Result<Metadata> {
        let mut metadata = Metadata::default();

        if let Some(size) = file.size {
            let content_length = size.parse::<u64>().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "parse content length").set_source(e)
            })?;
            metadata.set_content_length(content_length);
        }

        if let Some(modified_time) = file.modified_time {
            let last_modified = modified_time.parse::<Timestamp>().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "parse last modified time").set_source(e)
            })?;
            metadata.set_last_modified(last_modified);
        }

        if let Some(md5_checksum) = file.md5_checksum {
            metadata.set_content_md5(&md5_checksum);
        }

        if let Some(version) = file.version {
            metadata.set_version(&version);
        }

        Ok(metadata)
    }
}

impl oio::OneShotWrite for GdriveWriter {
    async fn write_once(&self, bs: Buffer) -> Result<Metadata> {
        let size = bs.len();

        let resp = if let Some(file_id) = &self.file_id {
            self.core
                .gdrive_upload_overwrite_simple_request(file_id, size as u64, bs)
                .await
        } else {
            self.core
                .gdrive_upload_simple_request(&self.path, size as u64, bs)
                .await
        }?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::CREATED => {
                let bs = resp.into_body();
                let file: GdriveFile =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                // If we don't have the file id before, let's update the cache to avoid re-fetching.
                if self.file_id.is_none() {
                    self.core.path_cache.insert(&self.path, &file.id).await;
                }

                let metadata = GdriveWriter::parse_metadata(file)?;
                Ok(metadata)
            }
            _ => Err(parse_error(resp)),
        }
    }
}
