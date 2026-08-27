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
use http::header::LOCATION;
use http::header::RANGE;

use super::core::CompleteMultipartUploadRequestPart;
use super::core::ErrorContext;
use super::core::GcsCore;
use super::core::InitiateMultipartUploadResult;
use super::core::constants::X_GOOG_GENERATION;
use super::core::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

pub type GcsWriters = TwoWays<oio::MultipartWriter<GcsWriter>, GcsConditionalWriter>;

const RESUMABLE_CHUNK_SIZE: usize = 8 * 1024 * 1024;

pub struct GcsWriter {
    core: Arc<GcsCore>,
    ctx: OperationContext,
    path: String,
    op: OpWrite,
}

impl GcsWriter {
    pub fn new(core: Arc<GcsCore>, ctx: OperationContext, path: &str, op: OpWrite) -> Self {
        GcsWriter {
            core,
            ctx,
            path: path.to_string(),
            op,
        }
    }
}

impl oio::MultipartWrite for GcsWriter {
    async fn write_once(&self, _: u64, body: Buffer) -> Result<Metadata> {
        let size = body.len() as u64;
        // Request builders own percent-encoding; writers pass logical paths unchanged.
        let req = self
            .core
            .gcs_insert_object_request(&self.path, Some(size), &self.op, body)?;

        let req = self.core.sign(&self.ctx, req).await?;

        let resp = self.core.send(&self.ctx, req).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                let metadata =
                    GcsCore::build_metadata_from_object_response(&self.path, resp.into_body())?;
                Ok(metadata)
            }
            _ => Err(parse_error(
                ErrorContext::new(Operation::Write, ServiceOperation("InsertObject"))
                    .with_if_not_exists(self.op.if_not_exists()),
                resp,
            )),
        }
    }

    async fn initiate_part(&self) -> Result<String> {
        let resp = self
            .core
            .gcs_initiate_multipart_upload(&self.ctx, &self.path, &self.op)
            .await?;

        if !resp.status().is_success() {
            return Err(parse_error(
                ErrorContext::new(Operation::Write, ServiceOperation("CreateMultipartUpload")),
                resp,
            ));
        }

        let buf = resp.into_body();
        let upload_id: InitiateMultipartUploadResult =
            quick_xml::de::from_reader(buf.reader()).map_err(new_xml_deserialize_error)?;
        Ok(upload_id.upload_id)
    }

    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> Result<oio::MultipartPart> {
        // Gcs requires part number must between [1..=10000]
        let part_number = part_number + 1;

        let resp = self
            .core
            .gcs_upload_part(&self.ctx, &self.path, upload_id, part_number, size, body)
            .await?;

        if !resp.status().is_success() {
            return Err(parse_error(
                ErrorContext::new(Operation::Write, ServiceOperation("UploadPart")),
                resp,
            ));
        }

        let etag = parse_etag(resp.headers())?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ETag not present in returning response",
                )
            })?
            .to_string();

        Ok(oio::MultipartPart {
            part_number,
            etag,
            checksum: None,
            size: None,
        })
    }

    async fn complete_part(
        &self,
        upload_id: &str,
        parts: &[oio::MultipartPart],
    ) -> Result<Metadata> {
        let parts = parts
            .iter()
            .map(|p| CompleteMultipartUploadRequestPart {
                part_number: p.part_number,
                etag: p.etag.clone(),
            })
            .collect();

        let resp = self
            .core
            .gcs_complete_multipart_upload(&self.ctx, &self.path, upload_id, parts)
            .await?;

        if !resp.status().is_success() {
            return Err(parse_error(
                ErrorContext::new(
                    Operation::Write,
                    ServiceOperation("CompleteMultipartUpload"),
                ),
                resp,
            ));
        }
        let mut metadata = Metadata::new(EntryMode::from_path(&self.path));
        if let Some(etag) = parse_etag(resp.headers())? {
            metadata.set_etag(etag);
        }
        if let Some(generation) = parse_header_to_str(resp.headers(), X_GOOG_GENERATION)? {
            metadata.set_version(generation);
        }
        Ok(metadata)
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        let resp = self
            .core
            .gcs_abort_multipart_upload(&self.ctx, &self.path, upload_id)
            .await?;
        match resp.status() {
            // gcs returns code 204 if abort succeeds.
            StatusCode::NO_CONTENT => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(Operation::Write, ServiceOperation("AbortMultipartUpload")),
                resp,
            )),
        }
    }
}

pub struct GcsConditionalWriter {
    core: Arc<GcsCore>,
    ctx: OperationContext,
    path: String,
    op: OpWrite,
    session_uri: Option<String>,
    offset: u64,
    pending: oio::QueueBuf,
}

impl GcsConditionalWriter {
    pub fn new(core: Arc<GcsCore>, ctx: OperationContext, path: &str, op: OpWrite) -> Self {
        Self {
            core,
            ctx,
            path: path.to_string(),
            op,
            session_uri: None,
            offset: 0,
            pending: oio::QueueBuf::new(),
        }
    }

    async fn ensure_session(&mut self) -> Result<&str> {
        if self.session_uri.is_none() {
            let resp = self
                .core
                .gcs_initiate_resumable_upload(&self.ctx, &self.path, &self.op)
                .await?;
            if !resp.status().is_success() {
                return Err(parse_error(
                    ErrorContext::new(
                        Operation::Write,
                        ServiceOperation("InitiateResumableUpload"),
                    )
                    .with_if_not_exists(self.op.if_not_exists())
                    .with_if_match(self.op.if_match().is_some())
                    .with_if_none_match(self.op.if_none_match().is_some())
                    .with_if_version_match(self.op.if_version_match().is_some())
                    .with_if_version_not_match(self.op.if_version_not_match().is_some()),
                    resp,
                ));
            }
            let location = parse_header_to_str(resp.headers(), LOCATION)?
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "GCS resumable upload response is missing Location",
                    )
                })?
                .to_string();
            self.session_uri = Some(location);
        }
        Ok(self.session_uri.as_deref().unwrap())
    }

    fn persisted_offset(resp: &http::Response<Buffer>) -> Result<u64> {
        let Some(range) = parse_header_to_str(resp.headers(), RANGE)? else {
            return Ok(0);
        };
        let end = range
            .strip_prefix("bytes=")
            .and_then(|v| v.rsplit_once('-'))
            .map(|(_, end)| end)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "invalid Range header in GCS resumable upload response",
                )
                .with_context("range", range)
            })?;
        end.parse::<u64>().map(|v| v + 1).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "invalid Range header in GCS resumable upload response",
            )
            .with_context("range", range)
            .set_source(err)
        })
    }

    async fn upload(&mut self, mut body: Buffer, total: Option<u64>) -> Result<Option<Metadata>> {
        loop {
            let session_uri = self.ensure_session().await?.to_string();
            let sent_offset = self.offset;
            let sent_end = sent_offset + body.len() as u64;
            let resp = self
                .core
                .gcs_upload_resumable_chunk(
                    &self.ctx,
                    &session_uri,
                    sent_offset,
                    body.clone(),
                    total,
                )
                .await?;

            if resp.status() == StatusCode::PERMANENT_REDIRECT {
                let persisted = Self::persisted_offset(&resp)?;
                if persisted <= sent_offset || persisted > sent_end {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "GCS resumable upload reported invalid persisted range",
                    )
                    .with_context("offset", sent_offset)
                    .with_context("persisted", persisted)
                    .set_temporary());
                }
                body.advance((persisted - sent_offset) as usize);
                self.offset = persisted;
                if body.is_empty() {
                    if total.is_some() {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "GCS did not finalize the last resumable upload chunk",
                        )
                        .set_temporary());
                    }
                    return Ok(None);
                }
                continue;
            }

            if resp.status() == StatusCode::OK || resp.status() == StatusCode::CREATED {
                if total.is_none() {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "GCS finalized a resumable upload before the last chunk",
                    ));
                }
                self.offset = sent_end;
                return GcsCore::build_metadata_from_object_response(&self.path, resp.into_body())
                    .map(Some);
            }

            return Err(parse_error(
                ErrorContext::new(Operation::Write, ServiceOperation("UploadResumableChunk"))
                    .with_if_not_exists(self.op.if_not_exists())
                    .with_if_match(self.op.if_match().is_some())
                    .with_if_none_match(self.op.if_none_match().is_some())
                    .with_if_version_match(self.op.if_version_match().is_some())
                    .with_if_version_not_match(self.op.if_version_not_match().is_some()),
                resp,
            ));
        }
    }

    async fn write_once(&self, body: Buffer) -> Result<Metadata> {
        let req = self.core.gcs_insert_object_request(
            &self.path,
            Some(body.len() as u64),
            &self.op,
            body,
        )?;
        let req = self.core.sign(&self.ctx, req).await?;
        let resp = self.core.send(&self.ctx, req).await?;
        match resp.status() {
            StatusCode::OK | StatusCode::CREATED => {
                GcsCore::build_metadata_from_object_response(&self.path, resp.into_body())
            }
            _ => Err(parse_error(
                ErrorContext::new(Operation::Write, ServiceOperation("InsertObject"))
                    .with_if_not_exists(self.op.if_not_exists())
                    .with_if_match(self.op.if_match().is_some())
                    .with_if_none_match(self.op.if_none_match().is_some())
                    .with_if_version_match(self.op.if_version_match().is_some())
                    .with_if_version_not_match(self.op.if_version_not_match().is_some()),
                resp,
            )),
        }
    }
}

impl oio::Write for GcsConditionalWriter {
    async fn write(&mut self, body: Buffer) -> Result<()> {
        self.pending.push(body);
        while self.pending.len() > RESUMABLE_CHUNK_SIZE {
            let mut buffered = self.pending.take().collect();
            let chunk = buffered.split_to(RESUMABLE_CHUNK_SIZE);
            self.pending.push(buffered);
            self.upload(chunk, None).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let body = self.pending.take().collect();
        if self.session_uri.is_none() {
            return self.write_once(body).await;
        }

        let total = self.offset + body.len() as u64;
        self.upload(body, Some(total))
            .await?
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "GCS upload returned no metadata"))
    }

    async fn abort(&mut self) -> Result<()> {
        self.pending.clear();
        let Some(session_uri) = self.session_uri.take() else {
            return Ok(());
        };
        let resp = self
            .core
            .gcs_cancel_resumable_upload(&self.ctx, &session_uri)
            .await?;
        if resp.status().is_success()
            || resp.status().as_u16() == 499
            || resp.status() == StatusCode::NOT_FOUND
            || resp.status() == StatusCode::GONE
        {
            Ok(())
        } else {
            Err(parse_error(
                ErrorContext::new(Operation::Write, ServiceOperation("CancelResumableUpload")),
                resp,
            ))
        }
    }
}
