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

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Buf;
use http::StatusCode;

use super::core::CompleteMultipartUploadRequestPart;
use super::core::GcsCore;
use super::core::InitiateMultipartUploadResult;
use super::core::constants::GCS_RESUMABLE_CHUNK_SIZE;
use super::core::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

pub type GcsWriters = TwoWays<oio::MultipartWriter<GcsWriter>, GcsResumableWriter>;

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
        write_object_once(&self.core, &self.ctx, &self.path, &self.op, body).await
    }

    async fn initiate_part(&self) -> Result<String> {
        let resp = self
            .core
            .gcs_initiate_multipart_upload(&self.ctx, &self.path, &self.op)
            .await?;

        if !resp.status().is_success() {
            return Err(parse_error(resp));
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
            return Err(parse_error(resp));
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
            return Err(parse_error(resp));
        }
        // we don't extract metadata from `CompleteMultipartUploadResult`, since we only need the `ETag` from it.
        // However, the `ETag` differs from the `ETag` obtained through the `stat` operation.
        // refer to: https://cloud.google.com/storage/docs/metadata#etags
        Ok(Metadata::default())
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        let resp = self
            .core
            .gcs_abort_multipart_upload(&self.ctx, &self.path, upload_id)
            .await?;
        match resp.status() {
            // gcs returns code 204 if abort succeeds.
            StatusCode::NO_CONTENT => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }
}

/// JSON API resumable uploads can carry `ifGenerationMatch=0`. XML multipart
/// uploads cannot, so chunked `if_not_exists` writes use this path instead.
pub struct GcsResumableWriter {
    core: Arc<GcsCore>,
    ctx: OperationContext,
    path: String,
    op: OpWrite,
    session_uri: Option<String>,
    written: u64,
    buffer: Buffer,
}

impl GcsResumableWriter {
    pub fn new(core: Arc<GcsCore>, ctx: OperationContext, path: &str, op: OpWrite) -> Self {
        GcsResumableWriter {
            core,
            ctx,
            path: path.to_string(),
            op,
            session_uri: None,
            written: 0,
            buffer: Buffer::new(),
        }
    }

    fn push_buffer(&mut self, bs: Buffer) {
        if bs.is_empty() {
            return;
        }
        if self.buffer.is_empty() {
            self.buffer = bs;
            return;
        }

        let mut parts = VecDeque::new();
        parts.extend(std::mem::replace(&mut self.buffer, Buffer::new()));
        parts.extend(bs);
        self.buffer = Buffer::from(parts);
    }

    async fn ensure_session(&mut self) -> Result<String> {
        if let Some(uri) = &self.session_uri {
            return Ok(uri.clone());
        }

        let resp = self
            .core
            .gcs_initiate_resumable_upload(&self.ctx, &self.path, &self.op)
            .await?;
        if !resp.status().is_success() {
            return Err(parse_error(resp));
        }

        let uri = parse_location(resp.headers())?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Location not present in resumable upload response",
                )
            })?
            .to_string();
        self.session_uri = Some(uri.clone());
        Ok(uri)
    }

    async fn upload_chunk(&mut self, body: Buffer, total: Option<u64>) -> Result<Option<Metadata>> {
        let uri = self.ensure_session().await?;
        let size = body.len() as u64;
        let start = self.written;
        let resp = self
            .core
            .gcs_upload_resumable_chunk(&self.ctx, &uri, start, total, body)
            .await?;

        match resp.status() {
            StatusCode::OK | StatusCode::CREATED => {
                self.written = start + size;
                let metadata =
                    GcsCore::build_metadata_from_object_response(&self.path, resp.into_body())
                        .unwrap_or_else(|_| Metadata::default());
                Ok(Some(metadata))
            }
            StatusCode::PERMANENT_REDIRECT => {
                self.written = start + size;
                Ok(None)
            }
            _ => Err(parse_error(resp)),
        }
    }
}

impl oio::Write for GcsResumableWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        if self.session_uri.is_none() && self.buffer.is_empty() {
            self.buffer = bs;
            return Ok(());
        }

        self.push_buffer(bs);
        while self.buffer.len() >= GCS_RESUMABLE_CHUNK_SIZE {
            let aligned = self.buffer.len() - (self.buffer.len() % GCS_RESUMABLE_CHUNK_SIZE);
            let chunk = self.buffer.split_to(aligned);
            self.upload_chunk(chunk, None).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        if self.session_uri.is_none() {
            let body = std::mem::replace(&mut self.buffer, Buffer::new());
            return write_object_once(&self.core, &self.ctx, &self.path, &self.op, body).await;
        }

        let remaining = std::mem::replace(&mut self.buffer, Buffer::new());
        let total = self.written + remaining.len() as u64;
        match self.upload_chunk(remaining, Some(total)).await? {
            Some(meta) => Ok(meta),
            None => Ok(Metadata::default()),
        }
    }

    async fn abort(&mut self) -> Result<()> {
        self.buffer = Buffer::new();
        let Some(uri) = self.session_uri.take() else {
            return Ok(());
        };

        let resp = self
            .core
            .gcs_abort_resumable_upload(&self.ctx, &uri)
            .await?;
        match resp.status() {
            s if s.is_success() || s.as_u16() == 499 => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }
}

async fn write_object_once(
    core: &GcsCore,
    ctx: &OperationContext,
    path: &str,
    op: &OpWrite,
    body: Buffer,
) -> Result<Metadata> {
    let size = body.len() as u64;
    // Request builders own percent-encoding; writers pass logical paths unchanged.
    let req = core.gcs_insert_object_request(path, Some(size), op, body)?;
    let req = core.sign(ctx, req).await?;
    let resp = core.send(ctx, req).await?;

    match resp.status() {
        StatusCode::CREATED | StatusCode::OK => {
            GcsCore::build_metadata_from_object_response(path, resp.into_body())
        }
        _ => Err(parse_error(resp)),
    }
}
