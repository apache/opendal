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
use http::Response;
use http::StatusCode;
use http::header::{LOCATION, RANGE};

use super::core::GdriveFile;
use super::core::parse_error;
use super::core::{ErrorContext, GdriveCore};
use opendal_core::raw::*;
use opendal_core::*;

// Keep small uploads below Drive's recommended 5 MB single-request size.
const CHUNK_SIZE: usize = 4 * 1024 * 1024;

pub struct GdriveWriter {
    core: Arc<GdriveCore>,
    ctx: OperationContext,

    path: String,

    file_id: Option<String>,
    session_uri: Option<String>,
    offset: u64,
    pending: oio::QueueBuf,
    check_status: bool,
}

impl GdriveWriter {
    pub fn new(
        core: Arc<GdriveCore>,
        ctx: OperationContext,
        path: String,
        file_id: Option<String>,
    ) -> Self {
        GdriveWriter {
            core,
            ctx,
            path,

            file_id,
            session_uri: None,
            offset: 0,
            pending: oio::QueueBuf::new(),
            check_status: false,
        }
    }

    async fn write_once(&self, bs: Buffer) -> Result<Metadata> {
        let size = bs.len();

        let mut current_file_id = match &self.file_id {
            Some(file_id) => Some(file_id.clone()),
            None => match self.core.resolve_path(&self.ctx, &self.path).await? {
                Some(id) => Some(id),
                None => {
                    self.core
                        .resolve_path_after_refresh(&self.ctx, &self.path)
                        .await?
                }
            },
        };
        let mut retried = false;

        loop {
            let resp = if let Some(file_id) = &current_file_id {
                self.core
                    .gdrive_upload_overwrite_simple_request(
                        &self.ctx,
                        file_id,
                        size as u64,
                        bs.clone(),
                    )
                    .await
            } else {
                self.core
                    .gdrive_upload_simple_request(&self.ctx, &self.path, size as u64, bs.clone())
                    .await
            }?;

            match resp.status() {
                StatusCode::OK | StatusCode::CREATED => {
                    return self.finish_upload(resp, size as u64).await;
                }
                StatusCode::NOT_FOUND if !retried && current_file_id.is_some() => {
                    retried = true;
                    self.core.refresh_path(&self.path).await;
                    current_file_id = self.core.resolve_path(&self.ctx, &self.path).await?;
                    continue;
                }
                _ => {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("UploadFile")),
                        resp,
                    ));
                }
            }
        }
    }

    async fn finish_upload(&self, resp: Response<Buffer>, size: u64) -> Result<Metadata> {
        let file: GdriveFile = serde_json::from_reader(resp.into_body().reader())
            .map_err(new_json_deserialize_error)?;
        let mut metadata = MetadataBuilder::file(size);
        metadata.content_type(file.mime_type);
        let metadata = metadata.build();
        self.core.cache_file_id(&self.path, &file.id).await;
        self.core
            .record_recent_upsert(&self.path, metadata.clone())
            .await;
        Ok(metadata)
    }

    async fn ensure_session(&mut self) -> Result<String> {
        if let Some(uri) = &self.session_uri {
            return Ok(uri.clone());
        }

        let mut file_id = match &self.file_id {
            Some(id) => Some(id.clone()),
            None => match self.core.resolve_path(&self.ctx, &self.path).await? {
                Some(id) => Some(id),
                None => {
                    self.core
                        .resolve_path_after_refresh(&self.ctx, &self.path)
                        .await?
                }
            },
        };
        let mut retried = false;
        loop {
            let resp = self
                .core
                .gdrive_initiate_resumable_upload(&self.ctx, &self.path, file_id.as_deref())
                .await?;
            match resp.status() {
                StatusCode::OK => {
                    let uri = parse_header_to_str(resp.headers(), LOCATION)?
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::Unexpected,
                                "GDrive resumable upload response is missing Location",
                            )
                        })?
                        .to_string();
                    self.session_uri = Some(uri.clone());
                    return Ok(uri);
                }
                StatusCode::NOT_FOUND if !retried && file_id.is_some() => {
                    retried = true;
                    self.core.refresh_path(&self.path).await;
                    file_id = self.core.resolve_path(&self.ctx, &self.path).await?;
                }
                _ => {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("InitiateResumableUpload")),
                        resp,
                    ));
                }
            }
        }
    }

    fn persisted_offset(&self, resp: &Response<Buffer>, end: u64) -> Result<u64> {
        let persisted = match parse_header_to_str(resp.headers(), RANGE)? {
            None => 0,
            Some(range) => range
                .strip_prefix("bytes=0-")
                .and_then(|v| v.parse::<u64>().ok())
                .and_then(|v| v.checked_add(1))
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "invalid Range header in GDrive resumable upload response",
                    )
                    .with_context("range", range)
                })?,
        };
        if persisted < self.offset || persisted > end {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "GDrive resumable upload reported an invalid persisted range",
            )
            .with_context("start", self.offset)
            .with_context("end", end)
            .with_context("persisted", persisted));
        }
        Ok(persisted)
    }

    async fn upload_chunk(&mut self, mut body: Buffer, last: bool) -> Result<Option<Metadata>> {
        let uri = self.ensure_session().await?;
        let end = self.offset + body.len() as u64;
        let total = last.then_some(end);
        let mut start = self.offset;

        if self.check_status {
            let resp = self
                .core
                .gdrive_query_resumable_upload(&self.ctx, &uri, total)
                .await?;
            match resp.status() {
                StatusCode::OK | StatusCode::CREATED if last => {
                    let metadata = self.finish_upload(resp, end).await?;
                    self.offset = end;
                    self.check_status = false;
                    return Ok(Some(metadata));
                }
                StatusCode::PERMANENT_REDIRECT => {
                    let persisted = self.persisted_offset(&resp, end)?;
                    if persisted == end && !last {
                        self.offset = end;
                        self.check_status = false;
                        return Ok(None);
                    }
                    // If all final bytes are persisted but the session is still
                    // incomplete, resend the final range to declare the total.
                    if persisted < end {
                        body.advance((persisted - start) as usize);
                        start = persisted;
                    }
                }
                _ => {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("QueryResumableUpload")),
                        resp,
                    ));
                }
            }
        }

        // A lost response or cancelled future leaves the server's offset unknown.
        self.check_status = true;
        let resp = self
            .core
            .gdrive_upload_resumable_chunk(&self.ctx, &uri, start, body, total)
            .await?;
        match resp.status() {
            StatusCode::OK | StatusCode::CREATED if last => {
                let metadata = self.finish_upload(resp, end).await?;
                self.offset = end;
                self.check_status = false;
                Ok(Some(metadata))
            }
            StatusCode::PERMANENT_REDIRECT => {
                let persisted = self.persisted_offset(&resp, end)?;
                if persisted == end && !last {
                    self.offset = end;
                    self.check_status = false;
                    return Ok(None);
                }
                Err(Error::new(
                    ErrorKind::Unexpected,
                    "GDrive has not finished the resumable upload chunk",
                )
                .with_context("persisted", persisted)
                .with_context("end", end)
                .set_temporary())
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("UploadResumableChunk")),
                resp,
            )),
        }
    }

    async fn flush_pending(&mut self) -> Result<()> {
        // Retain the last chunk until close so its request includes the total size.
        while self.pending.len() > CHUNK_SIZE {
            let mut body = self.pending.clone().collect();
            body.truncate(CHUNK_SIZE);
            self.upload_chunk(body, false).await?;
            self.pending.advance(CHUNK_SIZE);
        }
        Ok(())
    }
}

impl oio::Write for GdriveWriter {
    async fn write(&mut self, body: Buffer) -> Result<()> {
        // Flush before accepting new input so a retried write cannot enqueue it twice.
        self.flush_pending().await?;
        self.pending.push(body);
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.flush_pending().await?;
        let body = self.pending.clone().collect();
        let metadata = if self.session_uri.is_none() {
            self.write_once(body).await?
        } else {
            self.upload_chunk(body, true)
                .await?
                .expect("final chunk returns metadata")
        };
        self.pending.clear();
        Ok(metadata)
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(uri) = &self.session_uri {
            let resp = self
                .core
                .gdrive_cancel_resumable_upload(&self.ctx, uri)
                .await?;
            if !(resp.status().is_success()
                || resp.status().as_u16() == 499
                || resp.status() == StatusCode::NOT_FOUND
                || resp.status() == StatusCode::GONE)
            {
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("CancelResumableUpload")),
                    resp,
                ));
            }
        }
        self.session_uri = None;
        self.pending.clear();
        Ok(())
    }
}
