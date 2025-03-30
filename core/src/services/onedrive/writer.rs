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
use bytes::Bytes;
use http::StatusCode;

use super::core::OneDriveCore;
use super::error::parse_error;
use super::graph_model::{OneDriveItem, OneDriveUploadSessionCreationResponseBody};
use crate::raw::*;
use crate::*;

pub struct OneDriveWriter {
    core: Arc<OneDriveCore>,
    op: OpWrite,
    path: String,
}

impl OneDriveWriter {
    const MAX_SIMPLE_SIZE: usize = 4 * 1024 * 1024; // 4MB

    // OneDrive demands the chunk size to be a multiple to to 320 KiB.
    // Choose a value smaller than `MAX_SIMPLE_SIZE`
    const CHUNK_SIZE_FACTOR: usize = 327_680 * 12; // floor(MAX_SIMPLE_SIZE / 320KB)

    pub fn new(core: Arc<OneDriveCore>, op: OpWrite, path: String) -> Self {
        OneDriveWriter { core, op, path }
    }
}

impl oio::OneShotWrite for OneDriveWriter {
    async fn write_once(&self, bs: Buffer) -> Result<Metadata> {
        let size = bs.len();

        let meta = if size <= Self::MAX_SIMPLE_SIZE {
            self.write_simple(bs).await?
        } else {
            self.write_chunked(bs).await?
        };

        Ok(meta)
    }
}

impl OneDriveWriter {
    async fn write_simple(&self, bs: Buffer) -> Result<Metadata> {
        let response = self
            .core
            .onedrive_upload_simple(&self.path, &self.op, bs)
            .await?;

        match response.status() {
            StatusCode::CREATED | StatusCode::OK => {
                let item: OneDriveItem = serde_json::from_reader(response.into_body().reader())
                    .map_err(new_json_deserialize_error)?;

                let mut meta = Metadata::new(EntryMode::FILE)
                    .with_etag(item.e_tag)
                    .with_content_length(item.size.max(0) as u64);

                let last_modified = item.last_modified_date_time;
                let date_utc_last_modified = parse_datetime_from_rfc3339(&last_modified)?;
                meta.set_last_modified(date_utc_last_modified);

                Ok(meta)
            }
            _ => Err(parse_error(response)),
        }
    }

    pub(crate) async fn write_chunked(&self, bs: Buffer) -> Result<Metadata> {
        // Upload large files via sessions: https://learn.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_createuploadsession?view=odsp-graph-online#upload-bytes-to-the-upload-session
        // 1. Create an upload session
        // 2. Upload the bytes of each chunk
        // 3. Commit the session

        let session_response = self.create_upload_session().await?;

        let mut offset = 0;
        let total_bytes = bs.to_bytes();
        let total_len = total_bytes.len();
        let chunks = total_bytes.chunks(OneDriveWriter::CHUNK_SIZE_FACTOR);

        for chunk in chunks {
            let mut end = offset + OneDriveWriter::CHUNK_SIZE_FACTOR;
            if end > total_bytes.len() {
                end = total_bytes.len();
            }
            let chunk_end = end - 1;

            let response = self
                .core
                .onedrive_chunked_upload(
                    &session_response.upload_url,
                    &self.op,
                    offset,
                    chunk_end,
                    total_len,
                    Buffer::from(Bytes::copy_from_slice(chunk)),
                )
                .await?;

            match response.status() {
                // Typical response code: 202 Accepted
                // Reference: https://learn.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_put_content?view=odsp-graph-online#response
                StatusCode::ACCEPTED | StatusCode::OK => {} // skip, in the middle of upload
                StatusCode::CREATED => {
                    // last trunk
                    let item: OneDriveItem = serde_json::from_reader(response.into_body().reader())
                        .map_err(new_json_deserialize_error)?;

                    let mut meta = Metadata::new(EntryMode::FILE)
                        .with_etag(item.e_tag)
                        .with_content_length(item.size.max(0) as u64);

                    let last_modified = item.last_modified_date_time;
                    let date_utc_last_modified = parse_datetime_from_rfc3339(&last_modified)?;
                    meta.set_last_modified(date_utc_last_modified);
                    return Ok(meta);
                }
                _ => return Err(parse_error(response)),
            }

            offset += OneDriveWriter::CHUNK_SIZE_FACTOR;
        }

        debug_assert!(false, "should have returned");

        Ok(Metadata::default()) // should not happen, but start with handling this gracefully - do nothing, but return the default metadata
    }

    async fn create_upload_session(&self) -> Result<OneDriveUploadSessionCreationResponseBody> {
        let response = self
            .core
            .onedrive_create_upload_session(&self.path, &self.op)
            .await?;
        match response.status() {
            StatusCode::OK => {
                let bs = response.into_body();
                let result: OneDriveUploadSessionCreationResponseBody =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                Ok(result)
            }
            _ => Err(parse_error(response)),
        }
    }
}
