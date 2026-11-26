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
use crc_fast::{CrcAlgorithm, Digest};
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

pub type GcsWriters = TwoWays<oio::MultipartWriter<GcsWriter>, GcsResumableWriter>;

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
        let req = self.core.gcs_insert_object_request(
            &self.path,
            Some(size),
            &self.op,
            body,
            None,
            None,
        )?;

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
                ErrorContext::new(ServiceOperation("InsertObject"))
                    .with_caller_condition(self.op.is_conditional()),
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
                ErrorContext::new(ServiceOperation("CreateMultipartUpload")),
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
                ErrorContext::new(ServiceOperation("UploadPart")),
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
                ErrorContext::new(ServiceOperation("CompleteMultipartUpload")),
                resp,
            ));
        }
        let mut metadata = if self.path.ends_with('/') {
            MetadataBuilder::dir()
        } else {
            MetadataBuilder::unknown()
        };
        if let Some(etag) = parse_etag(resp.headers())? {
            metadata.etag(etag);
        }
        if let Some(generation) = parse_header_to_str(resp.headers(), X_GOOG_GENERATION)? {
            metadata.version(generation);
        }
        Ok(metadata.build())
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
                ErrorContext::new(ServiceOperation("AbortMultipartUpload")),
                resp,
            )),
        }
    }
}

pub struct GcsResumableWriter {
    core: Arc<GcsCore>,
    ctx: OperationContext,
    path: String,
    op: OpWrite,
    session_uri: Option<String>,
    offset: u64,
    pending: oio::QueueBuf,
    finalize_state: FinalizeState,
    crc32c: Option<WriteCrc32c>,
    // Retries of the same request reuse its token so GCS can return the original
    // response instead of applying the write twice.
    insert_idempotency_token: String,
    session_idempotency_token: String,
}

enum ResumableUploadStatus {
    Incomplete(u64),
    Complete(Box<Metadata>),
}

#[derive(Eq, PartialEq)]
enum FinalizeState {
    Ready,
    CheckStatus,
}

enum WriteCrc32c {
    Computed(Box<Digest>),
    Expected([u8; 4]),
}

impl WriteCrc32c {
    fn digest(&self) -> [u8; 4] {
        match self {
            Self::Computed(digest) => (digest.finalize() as u32).to_be_bytes(),
            Self::Expected(value) => *value,
        }
    }
}

impl GcsResumableWriter {
    pub fn new(core: Arc<GcsCore>, ctx: OperationContext, path: &str, op: OpWrite) -> Result<Self> {
        let crc32c = if let Some(intent) = op.checksum() {
            op.check(&core.capability)?;
            let checksum = match intent {
                WriteChecksum::Any
                | WriteChecksum::Fast
                | WriteChecksum::Algo(ChecksumAlgo::Crc32c) => {
                    WriteCrc32c::Computed(Box::new(Digest::new(CrcAlgorithm::Crc32Iscsi)))
                }
                WriteChecksum::Expected(checksum) if checksum.algo == ChecksumAlgo::Crc32c => {
                    WriteCrc32c::Expected(checksum.value.try_into().map_err(|_| {
                        Error::new(
                            ErrorKind::ConfigInvalid,
                            "CRC32C requires a four-byte digest",
                        )
                    })?)
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "GCS write checksums require CRC32C",
                    ));
                }
            };
            Some(checksum)
        } else {
            None
        };
        Ok(Self {
            core,
            ctx,
            path: path.to_string(),
            op,
            session_uri: None,
            offset: 0,
            pending: oio::QueueBuf::new(),
            finalize_state: FinalizeState::Ready,
            crc32c,
            insert_idempotency_token: uuid::Uuid::new_v4().to_string(),
            session_idempotency_token: uuid::Uuid::new_v4().to_string(),
        })
    }

    async fn ensure_session(&mut self) -> Result<&str> {
        if self.session_uri.is_none() {
            let resp = self
                .core
                .gcs_initiate_resumable_upload(
                    &self.ctx,
                    &self.path,
                    &self.op,
                    &self.session_idempotency_token,
                )
                .await?;
            if !resp.status().is_success() {
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("InitiateResumableUpload"))
                        .with_caller_condition(self.op.is_conditional()),
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

    async fn query_status(&mut self, total: u64) -> Result<ResumableUploadStatus> {
        let session_uri = self.ensure_session().await?.to_string();
        let resp = self
            .core
            .gcs_query_resumable_upload(&self.ctx, &session_uri, total)
            .await?;

        match resp.status() {
            StatusCode::PERMANENT_REDIRECT => {
                let persisted = Self::persisted_offset(&resp)?;
                Ok(ResumableUploadStatus::Incomplete(persisted))
            }
            StatusCode::OK | StatusCode::CREATED => {
                let metadata =
                    GcsCore::build_metadata_from_object_response(&self.path, resp.into_body())?;
                Ok(ResumableUploadStatus::Complete(Box::new(metadata)))
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("QueryResumableUpload")),
                resp,
            )),
        }
    }

    async fn upload_chunk(&mut self, body: Buffer) -> Result<()> {
        let start = self.offset;
        let end = start + body.len() as u64;
        let session_uri = self.ensure_session().await?.to_string();
        let resp = self
            .core
            .gcs_upload_resumable_chunk(&self.ctx, &session_uri, start, body, None, None)
            .await?;

        if resp.status() == StatusCode::PERMANENT_REDIRECT {
            let persisted = Self::persisted_offset(&resp)?;
            if persisted < start || persisted > end {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "GCS resumable upload reported invalid persisted range",
                )
                .with_context("start", start)
                .with_context("end", end)
                .with_context("persisted", persisted)
                .set_temporary());
            }
            if persisted < end {
                // GCS ignores an already persisted prefix when the same chunk
                // is retried, so advance only after the full chunk is acknowledged.
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "GCS resumable upload persisted only part of a chunk",
                )
                .with_context("start", start)
                .with_context("end", end)
                .with_context("persisted", persisted)
                .set_temporary());
            }
            self.offset = end;
            return Ok(());
        }

        if resp.status() == StatusCode::OK || resp.status() == StatusCode::CREATED {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "GCS finalized a resumable upload before the last chunk",
            ));
        }

        Err(parse_error(
            ErrorContext::new(ServiceOperation("UploadResumableChunk"))
                .with_caller_condition(self.op.is_conditional()),
            resp,
        ))
    }

    async fn write_once(&self, body: Buffer, crc32c: Option<[u8; 4]>) -> Result<Metadata> {
        let req = self.core.gcs_insert_object_request(
            &self.path,
            Some(body.len() as u64),
            &self.op,
            body,
            Some(&self.insert_idempotency_token),
            crc32c,
        )?;
        let req = self.core.sign(&self.ctx, req).await?;
        let resp = self.core.send(&self.ctx, req).await?;

        if resp.status() == StatusCode::OK || resp.status() == StatusCode::CREATED {
            let metadata =
                GcsCore::build_metadata_from_object_response(&self.path, resp.into_body())?;
            return Self::verify_receipt(metadata, crc32c);
        }
        let error = parse_error(
            ErrorContext::new(ServiceOperation("InsertObject"))
                .with_caller_condition(self.op.is_conditional()),
            resp,
        );
        if error.kind() == ErrorKind::ChecksumMismatch
            && matches!(self.crc32c, Some(WriteCrc32c::Computed(_)))
        {
            // The rejected insert retains the same replayable body and idempotency token.
            return Err(error.set_temporary());
        }
        Err(error)
    }

    fn verify_receipt(metadata: Metadata, sent: Option<[u8; 4]>) -> Result<Metadata> {
        if let Some(sent) = sent
            && metadata
                .checksum()
                .is_none_or(|received| received.value != sent)
        {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "GCS completed the write without confirming the requested CRC32C",
            ));
        }
        Ok(metadata)
    }

    async fn flush_pending(&mut self) -> Result<()> {
        while self.pending.len() > RESUMABLE_CHUNK_SIZE {
            let mut chunk = self.pending.clone().collect();
            chunk.truncate(RESUMABLE_CHUNK_SIZE);
            self.upload_chunk(chunk).await?;
            self.pending.advance(RESUMABLE_CHUNK_SIZE);
        }
        Ok(())
    }
}

impl oio::Write for GcsResumableWriter {
    async fn write(&mut self, body: Buffer) -> Result<()> {
        self.flush_pending().await?;
        if let Some(WriteCrc32c::Computed(digest)) = &mut self.crc32c {
            // Hash input after the previous flush succeeds so a retried write counts it once.
            for chunk in body.clone() {
                digest.update(&chunk);
            }
        }
        self.pending.push(body);
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let crc32c = self.crc32c.as_ref().map(WriteCrc32c::digest);
        // A body that fits in one chunk has no partial upload to resume, so it skips the
        // request that opens a resumable session.
        if self.session_uri.is_none() && self.pending.len() <= RESUMABLE_CHUNK_SIZE {
            let body = self.pending.clone().collect();
            let metadata = self.write_once(body, crc32c).await?;
            self.pending.clear();
            return Ok(metadata);
        }

        self.flush_pending().await?;
        let body = self.pending.clone().collect();
        let start = self.offset;
        let total = start + body.len() as u64;

        if self.finalize_state == FinalizeState::CheckStatus {
            match self.query_status(total).await? {
                ResumableUploadStatus::Incomplete(persisted) => {
                    if persisted < start || persisted > total {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "GCS resumable upload reported invalid persisted range",
                        )
                        .with_context("start", start)
                        .with_context("end", total)
                        .with_context("persisted", persisted)
                        .set_temporary());
                    }
                    self.finalize_state = FinalizeState::Ready;
                }
                ResumableUploadStatus::Complete(metadata) => {
                    let metadata = Self::verify_receipt(*metadata, crc32c)?;
                    self.finalize_state = FinalizeState::Ready;
                    self.offset = total;
                    self.pending.advance(body.len());
                    return Ok(metadata);
                }
            }
        }

        let session_uri = self.ensure_session().await?.to_string();
        self.finalize_state = FinalizeState::CheckStatus;
        let resp = self
            .core
            .gcs_upload_resumable_chunk(
                &self.ctx,
                &session_uri,
                start,
                body.clone(),
                Some(total),
                crc32c,
            )
            .await?;

        if resp.status() == StatusCode::PERMANENT_REDIRECT {
            let persisted = Self::persisted_offset(&resp)?;
            if persisted < start || persisted > total {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "GCS resumable upload reported invalid persisted range",
                )
                .with_context("start", start)
                .with_context("end", total)
                .with_context("persisted", persisted)
                .set_temporary());
            }
            self.finalize_state = FinalizeState::Ready;
            return Err(Error::new(
                ErrorKind::Unexpected,
                "GCS did not finalize the last resumable upload chunk",
            )
            .with_context("persisted", persisted)
            .set_temporary());
        }

        if resp.status() == StatusCode::OK || resp.status() == StatusCode::CREATED {
            let metadata =
                GcsCore::build_metadata_from_object_response(&self.path, resp.into_body())?;
            let metadata = Self::verify_receipt(metadata, crc32c)?;
            self.finalize_state = FinalizeState::Ready;
            self.offset = total;
            self.pending.advance(body.len());
            return Ok(metadata);
        }

        let err = parse_error(
            ErrorContext::new(ServiceOperation("UploadResumableChunk"))
                .with_caller_condition(self.op.is_conditional()),
            resp,
        );
        if !err.is_temporary() && err.kind() != ErrorKind::ChecksumMismatch {
            self.finalize_state = FinalizeState::Ready;
        }
        Err(err)
    }

    async fn abort(&mut self) -> Result<()> {
        let Some(session_uri) = self.session_uri.as_deref() else {
            self.pending.clear();
            return Ok(());
        };
        let resp = self
            .core
            .gcs_cancel_resumable_upload(&self.ctx, session_uri)
            .await?;
        if !resp.status().is_success()
            && resp.status().as_u16() != 499
            && resp.status() != StatusCode::NOT_FOUND
            && resp.status() != StatusCode::GONE
        {
            let error = parse_error(
                ErrorContext::new(ServiceOperation("CancelResumableUpload")),
                resp,
            );
            // A session with a rejected final CRC32C replays its terminal error on DELETE.
            if error.kind() != ErrorKind::ChecksumMismatch {
                return Err(error);
            }
        }
        self.session_uri = None;
        self.pending.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use bytes::Bytes;
    use futures::stream;
    use http::Request;
    use http::Response;
    use http::header::CONTENT_LENGTH;
    use opendal_core::raw::oio::Write as _;
    use reqsign_core::Context;
    use reqsign_core::ProvideCredentialChain;
    use reqsign_google::RequestSigner;
    use reqsign_google::TokenCredentialProvider;

    use super::super::core::constants::X_GOOG_GCS_IDEMPOTENCY_TOKEN;
    use super::*;
    use crate::Gcs;
    use crate::backend::GcsBackend;

    #[derive(Default)]
    struct MockState {
        uploaded: Vec<u8>,
        ranges: Vec<String>,
        fail_chunk_response: bool,
        fail_final_response: bool,
        fail_final_response_partially: bool,
        fail_insert_response: bool,
        fail_initiate_response: bool,
        finalized: bool,
        generation: Option<u64>,
        requests: Vec<String>,
        idempotency_tokens: Vec<Option<String>>,
        honor_idempotency_tokens: bool,
        insert_results_by_token: HashMap<String, (usize, u64)>,
    }

    #[derive(Clone, Copy)]
    enum MockFinalResponse {
        Committed(Option<&'static str>),
        BadRequest(&'static str),
    }

    #[derive(Clone)]
    struct MockResumableTransport {
        state: Arc<Mutex<MockState>>,
        insert_checksums: Option<(&'static str, &'static str)>,
        final_hash: Option<&'static str>,
        final_response: MockFinalResponse,
    }

    impl Default for MockResumableTransport {
        fn default() -> Self {
            Self {
                state: Arc::new(Mutex::new(MockState {
                    honor_idempotency_tokens: true,
                    ..Default::default()
                })),
                insert_checksums: None,
                final_hash: None,
                final_response: MockFinalResponse::Committed(None),
            }
        }
    }

    impl MockResumableTransport {
        fn fail_chunk_response(&self) {
            self.state.lock().unwrap().fail_chunk_response = true;
        }

        fn fail_final_response(&self) {
            self.state.lock().unwrap().fail_final_response = true;
        }

        fn fail_final_response_partially(&self) {
            self.state.lock().unwrap().fail_final_response_partially = true;
        }

        fn fail_insert_response(&self) {
            self.state.lock().unwrap().fail_insert_response = true;
        }

        fn fail_initiate_response(&self) {
            self.state.lock().unwrap().fail_initiate_response = true;
        }

        // Models a retry sent after GCS's deduplication window has passed.
        fn ignore_idempotency_tokens(&self) {
            self.state.lock().unwrap().honor_idempotency_tokens = false;
        }

        fn idempotency_tokens(&self) -> Vec<Option<String>> {
            self.state.lock().unwrap().idempotency_tokens.clone()
        }

        fn existing_object(&self, generation: u64) {
            self.state.lock().unwrap().generation = Some(generation);
        }

        fn requests(&self) -> Vec<String> {
            self.state.lock().unwrap().requests.clone()
        }

        fn uploaded(&self) -> Vec<u8> {
            self.state.lock().unwrap().uploaded.clone()
        }

        fn ranges(&self) -> Vec<String> {
            self.state.lock().unwrap().ranges.clone()
        }

        fn response(
            status: StatusCode,
            range: Option<String>,
            body: Option<String>,
        ) -> Result<Response<HttpBody>> {
            let body = body.unwrap_or_default();
            let size = body.len() as u64;
            let mut response = Response::builder()
                .status(status)
                .header(CONTENT_LENGTH, size);
            if let Some(range) = range {
                response = response.header(RANGE, range);
            }
            let stream = if body.is_empty() {
                stream::iter(Vec::<Result<Buffer>>::new())
            } else {
                stream::iter(vec![Ok(Buffer::from(Bytes::from(body)))])
            };
            Ok(response
                .body(HttpBody::new(stream, Some(size)))
                .expect("mock response must build"))
        }

        fn completed_response(
            size: usize,
            generation: u64,
            crc32c: Option<&str>,
        ) -> Result<Response<HttpBody>> {
            let mut metadata = serde_json::json!({
                "size": size.to_string(),
                "generation": generation.to_string(),
            });
            if let Some(crc32c) = crc32c {
                metadata["crc32c"] = serde_json::json!(crc32c);
            }
            Self::response(StatusCode::OK, None, Some(metadata.to_string()))
        }

        fn finalized_response(&self, state: &MockState) -> Result<Response<HttpBody>> {
            match self.final_response {
                MockFinalResponse::Committed(crc32c) => Self::completed_response(
                    state.uploaded.len(),
                    state.generation.unwrap(),
                    crc32c,
                ),
                MockFinalResponse::BadRequest(error) => {
                    Self::response(StatusCode::BAD_REQUEST, None, Some(error.to_owned()))
                }
            }
        }

        // https://cloud.google.com/storage/docs/json_api/v1/objects/insert: ifGenerationMatch=0
        // passes only when no live object exists, and ifGenerationNotMatch fails when no live
        // object exists. GCS does not document the status for the second case; 412 is assumed.
        fn precondition_failure(query: &str, generation: Option<u64>) -> Option<StatusCode> {
            query
                .split('&')
                .find_map(|pair| match (pair.split_once('='), generation) {
                    (Some(("ifGenerationMatch", v)), _) => (v.parse::<u64>().unwrap()
                        != generation.unwrap_or(0))
                    .then_some(StatusCode::PRECONDITION_FAILED),
                    (Some(("ifGenerationNotMatch", _)), None) => {
                        Some(StatusCode::PRECONDITION_FAILED)
                    }
                    (Some(("ifGenerationNotMatch", v)), Some(current)) => {
                        (v.parse::<u64>().unwrap() == current).then_some(StatusCode::NOT_MODIFIED)
                    }
                    _ => None,
                })
        }
    }

    impl HttpTransport for MockResumableTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let query = req.uri().query().unwrap_or_default().to_string();
            let kind = match *req.method() {
                http::Method::POST if query.contains("uploadType=resumable") => {
                    "initiate".to_string()
                }
                http::Method::POST => format!("insert {query}"),
                _ => "chunk".to_string(),
            };
            let token = req
                .headers()
                .get(X_GOOG_GCS_IDEMPOTENCY_TOKEN)
                .map(|v| v.to_str().unwrap().to_string());
            {
                let mut state = self.state.lock().unwrap();
                state.requests.push(kind);
                state.idempotency_tokens.push(token.clone());
            }

            if let Some((request_crc32c, receipt_crc32c)) = self.insert_checksums {
                assert_eq!(req.method(), http::Method::POST);
                assert!(
                    query
                        .split('&')
                        .any(|param| param == "uploadType=multipart")
                );
                let boundary = parse_multipart_boundary(req.headers())
                    .unwrap()
                    .expect("insert must contain multipart object metadata");
                let body = String::from_utf8(req.body().to_vec()).unwrap();
                let (_, parts) = body.split_once("\r\n\r\n").unwrap();
                let (metadata, media) = parts.split_once(&format!("\r\n--{boundary}\r\n")).unwrap();
                let metadata: serde_json::Value = serde_json::from_str(metadata).unwrap();
                assert_eq!(
                    metadata["crc32c"], request_crc32c,
                    "CRC32C in insert metadata"
                );
                let (_, media) = media.split_once("\r\n\r\n").unwrap();
                let (media, _) = media.split_once(&format!("\r\n--{boundary}--")).unwrap();
                return Self::completed_response(media.len(), 1, Some(receipt_crc32c));
            }

            if req.method() == http::Method::POST && !query.contains("uploadType=resumable") {
                let mut state = self.state.lock().unwrap();
                if state.honor_idempotency_tokens
                    && let Some((size, generation)) = token
                        .as_ref()
                        .and_then(|t| state.insert_results_by_token.get(t))
                {
                    return Self::completed_response(*size, *generation, None);
                }
                if let Some(status) = Self::precondition_failure(&query, state.generation) {
                    return Self::response(status, None, None);
                }
                state.uploaded = req.body().to_vec();
                let generation = state.generation.map_or(1, |g| g + 1);
                state.generation = Some(generation);
                if let Some(token) = token {
                    let size = state.uploaded.len();
                    state
                        .insert_results_by_token
                        .insert(token, (size, generation));
                }
                if state.fail_insert_response {
                    state.fail_insert_response = false;
                    return Err(
                        Error::new(ErrorKind::Unexpected, "mock lost insert response")
                            .set_temporary(),
                    );
                }
                return Self::completed_response(state.uploaded.len(), generation, None);
            }

            let hash = req
                .headers()
                .get("x-goog-hash")
                .map(|value| value.to_str().unwrap());
            if req.method() == http::Method::POST {
                assert_eq!(hash, None, "session initiation must not send a hash header");
                let metadata: serde_json::Value =
                    serde_json::from_slice(&req.body().to_bytes()).unwrap();
                assert!(
                    metadata.get("crc32c").is_none(),
                    "session initiation must not send CRC32C metadata"
                );
                let mut state = self.state.lock().unwrap();
                if state.fail_initiate_response {
                    state.fail_initiate_response = false;
                    return Err(
                        Error::new(ErrorKind::Unexpected, "mock lost initiate response")
                            .set_temporary(),
                    );
                }
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(LOCATION, "https://upload.example/session")
                    .header(CONTENT_LENGTH, 0)
                    .body(HttpBody::new(
                        stream::iter(Vec::<Result<Buffer>>::new()),
                        Some(0),
                    ))
                    .expect("mock response must build"));
            }

            assert_eq!(req.method(), http::Method::PUT);
            assert_eq!(req.uri(), "https://upload.example/session");

            let content_range = req
                .headers()
                .get("content-range")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            let body = req.body().to_bytes();
            let mut state = self.state.lock().unwrap();
            state.ranges.push(content_range.clone());

            if body.is_empty() {
                assert_eq!(hash, None, "a status query must not send a hash header");
                if state.finalized {
                    return self.finalized_response(&state);
                }
                let range = (!state.uploaded.is_empty())
                    .then(|| format!("bytes=0-{}", state.uploaded.len() - 1));
                return Self::response(StatusCode::PERMANENT_REDIRECT, range, None);
            }

            let value = content_range.strip_prefix("bytes ").unwrap();
            let (range, total) = value.split_once('/').unwrap();
            let (start, end) = range.split_once('-').unwrap();
            let start = start.parse::<usize>().unwrap();
            let end = end.parse::<usize>().unwrap();
            assert_eq!(end + 1 - start, body.len());
            assert!(start <= state.uploaded.len());
            let persisted = state.uploaded.len() - start;
            assert!(persisted <= body.len());
            assert_eq!(&state.uploaded[start..], &body[..persisted]);
            let remaining = &body[persisted..];

            if total == "*" {
                assert_eq!(hash, None, "a non-final PUT must not send a hash header");
                if state.fail_chunk_response {
                    state.fail_chunk_response = false;
                    state
                        .uploaded
                        .extend_from_slice(&remaining[..remaining.len() / 2]);
                    return Err(
                        Error::new(ErrorKind::Unexpected, "mock lost chunk response")
                            .set_temporary(),
                    );
                }
                state.uploaded.extend_from_slice(remaining);
                let range = Some(format!("bytes=0-{}", state.uploaded.len() - 1));
                return Self::response(StatusCode::PERMANENT_REDIRECT, range, None);
            }

            assert_eq!(hash, self.final_hash, "hash header on the final data PUT");
            if state.fail_final_response_partially {
                state.fail_final_response_partially = false;
                state
                    .uploaded
                    .extend_from_slice(&remaining[..remaining.len() / 2]);
                return Err(
                    Error::new(ErrorKind::Unexpected, "mock lost partial final response")
                        .set_temporary(),
                );
            }
            assert_eq!(
                total.parse::<usize>().unwrap(),
                state.uploaded.len() + remaining.len()
            );
            if matches!(self.final_response, MockFinalResponse::Committed(_)) {
                state.uploaded.extend_from_slice(remaining);
                state.generation = Some(state.generation.map_or(1, |g| g + 1));
            }
            state.finalized = true;
            if state.fail_final_response {
                state.fail_final_response = false;
                return Err(
                    Error::new(ErrorKind::Unexpected, "mock lost final response").set_temporary(),
                );
            }
            self.finalized_response(&state)
        }
    }

    fn test_writer(transport: MockResumableTransport) -> GcsWriters {
        test_writer_with(
            transport,
            options::WriteOptions {
                if_not_exists: true,
                ..Default::default()
            },
        )
    }

    fn test_writer_with(
        transport: MockResumableTransport,
        options: options::WriteOptions,
    ) -> GcsWriters {
        let core = Arc::new(GcsCore {
            info: ServiceInfo::new("gcs", "/", "test-bucket"),
            capability: Capability {
                write: true,
                write_can_empty: true,
                write_can_multi: true,
                write_with_cache_control: true,
                write_with_content_type: true,
                write_with_content_encoding: true,
                write_with_user_metadata: true,
                write_with_if_not_exists: true,
                write_with_if_version_match: true,
                write_with_if_version_not_match: true,
                write_with_checksum: true,
                ..Default::default()
            },
            endpoint: "https://storage.googleapis.com".to_string(),
            bucket: "test-bucket".to_string(),
            root: "/".to_string(),
            sign_ctx: Context::new(),
            credential_provider: Arc::new(
                ProvideCredentialChain::new().push(TokenCredentialProvider::new("test-token")),
            ),
            request_signer: Arc::new(RequestSigner::new("storage")),
            signers: HttpTransportCache::new(),
            predefined_acl: None,
            default_storage_class: None,
            skip_signature: true,
        });
        let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
        let (op, _) = OpWrite::from_options(&core.capability, options).unwrap();
        Service::write(&GcsBackend { core }, &ctx, "object", op).unwrap()
    }

    #[tokio::test]
    async fn retry_write_resends_interrupted_chunk_before_accepting_input() {
        let transport = MockResumableTransport::default();
        transport.fail_chunk_response();
        let mut writer = test_writer(transport.clone());
        let mut expected = vec![7; RESUMABLE_CHUNK_SIZE + 3];
        let body = Buffer::from(Bytes::copy_from_slice(&expected));
        let next = Bytes::from_static(b"next");

        writer.write(body).await.unwrap();
        let err = writer.write(Buffer::from(next.clone())).await.unwrap_err();
        assert!(err.is_temporary());
        writer.write(Buffer::from(next.clone())).await.unwrap();
        let metadata = writer.close().await.unwrap();

        expected.extend_from_slice(&next);
        assert_eq!(transport.uploaded(), expected);
        assert_eq!(
            metadata.content_length(),
            (RESUMABLE_CHUNK_SIZE + 3 + next.len()) as u64
        );
        assert_eq!(
            transport.ranges(),
            vec![
                format!("bytes 0-{}/*", RESUMABLE_CHUNK_SIZE - 1),
                format!("bytes 0-{}/*", RESUMABLE_CHUNK_SIZE - 1),
                format!(
                    "bytes {}-{}/{}",
                    RESUMABLE_CHUNK_SIZE,
                    RESUMABLE_CHUNK_SIZE + 2 + next.len(),
                    RESUMABLE_CHUNK_SIZE + 3 + next.len()
                ),
            ]
        );
    }

    fn multi_chunk_body() -> Vec<u8> {
        let mut body = vec![7; RESUMABLE_CHUNK_SIZE];
        body.extend_from_slice(b"hello");
        body
    }

    #[tokio::test]
    async fn retry_close_reconciles_lost_final_response() {
        let transport = MockResumableTransport::default();
        transport.fail_final_response();
        let mut writer = test_writer(transport.clone());
        let expected = multi_chunk_body();
        writer
            .write(Buffer::from(Bytes::copy_from_slice(&expected)))
            .await
            .unwrap();

        let err = writer.close().await.unwrap_err();
        assert!(err.is_temporary());
        let metadata = writer.close().await.unwrap();

        let total = RESUMABLE_CHUNK_SIZE + 5;
        assert_eq!(transport.uploaded(), expected);
        assert_eq!(metadata.content_length(), total as u64);
        assert_eq!(
            transport.ranges(),
            vec![
                format!("bytes 0-{}/*", RESUMABLE_CHUNK_SIZE - 1),
                format!("bytes {RESUMABLE_CHUNK_SIZE}-{}/{total}", total - 1),
                format!("bytes */{total}"),
            ]
        );
    }

    #[tokio::test]
    async fn close_resumable_requires_matching_crc32c_receipt() {
        // CRC32C of 8 MiB of 0x07 followed by "hello", in big-endian byte order.
        let checksum = Checksum::new(ChecksumAlgo::Crc32c, [0x0e, 0x9e, 0x4d, 0x7f]);
        for (lose_final_response, receipt_crc32c, expect_success) in [
            (false, Some("Dp5Nfw=="), true),
            (true, Some("Dp5Nfw=="), true),
            (false, None, false),
            (true, Some("AAAAAA=="), false),
        ] {
            let transport = MockResumableTransport {
                final_hash: Some("crc32c=Dp5Nfw=="),
                final_response: MockFinalResponse::Committed(receipt_crc32c),
                ..Default::default()
            };
            if lose_final_response {
                transport.fail_final_response();
            }
            let mut writer = test_writer_with(
                transport.clone(),
                options::WriteOptions {
                    checksum: Some(WriteChecksum::Algo(ChecksumAlgo::Crc32c)),
                    ..Default::default()
                },
            );
            let mut body = Buffer::from(multi_chunk_body());
            writer
                .write(body.split_to(RESUMABLE_CHUNK_SIZE))
                .await
                .unwrap();
            writer.write(body).await.unwrap();
            if lose_final_response {
                let error = writer.close().await.expect_err("final response was lost");
                assert!(error.is_temporary());
            }

            let total = RESUMABLE_CHUNK_SIZE + 5;
            let result = writer.close().await;
            let mut ranges = vec![
                format!("bytes 0-{}/*", RESUMABLE_CHUNK_SIZE - 1),
                format!("bytes {RESUMABLE_CHUNK_SIZE}-{}/{total}", total - 1),
            ];
            if lose_final_response {
                ranges.push(format!("bytes */{total}"));
            }
            assert_eq!(transport.ranges(), ranges);
            if expect_success {
                let metadata = result.unwrap();
                assert_eq!(metadata.checksum().as_ref(), Some(&checksum));
                assert_eq!(metadata.content_length(), total as u64);
                assert_eq!(metadata.version(), Some("1"));
            } else {
                let error = result.expect_err("close must reject an unconfirmed CRC32C receipt");
                assert_eq!(error.kind(), ErrorKind::Unexpected);
                assert!(!error.is_temporary());
            }
        }
    }

    #[tokio::test]
    async fn close_resumable_rejects_incorrect_expected_crc32c() {
        let transport = MockResumableTransport {
            final_hash: Some("crc32c=AAAAAA=="),
            final_response: MockFinalResponse::BadRequest(
                r#"{"error":{"code":400,"message":"Provided CRC32C 'AAAAAA==' doesn't match calculated CRC32C 'Dp5Nfw=='.","errors":[{"reason":"invalid"}]}}"#,
            ),
            ..Default::default()
        };
        let mut writer = test_writer_with(
            transport.clone(),
            options::WriteOptions {
                checksum: Some(WriteChecksum::Expected(Checksum::new(
                    ChecksumAlgo::Crc32c,
                    [0; 4],
                ))),
                ..Default::default()
            },
        );
        writer
            .write(Buffer::from(multi_chunk_body()))
            .await
            .unwrap();

        for _ in 0..2 {
            let error = writer
                .close()
                .await
                .expect_err("GCS rejected the final CRC32C");
            assert_eq!(error.kind(), ErrorKind::ChecksumMismatch);
            assert!(!error.is_temporary());
        }
        let total = RESUMABLE_CHUNK_SIZE + 5;
        assert_eq!(
            transport.ranges(),
            vec![
                format!("bytes 0-{}/*", RESUMABLE_CHUNK_SIZE - 1),
                format!("bytes {RESUMABLE_CHUNK_SIZE}-{}/{total}", total - 1),
                format!("bytes */{total}"),
            ]
        );
    }

    #[tokio::test]
    async fn retry_close_resends_after_incomplete_status() {
        let transport = MockResumableTransport::default();
        transport.fail_final_response_partially();
        let mut writer = test_writer(transport.clone());
        let expected = multi_chunk_body();
        writer
            .write(Buffer::from(Bytes::copy_from_slice(&expected)))
            .await
            .unwrap();

        let err = writer.close().await.unwrap_err();
        assert!(err.is_temporary());
        let metadata = writer.close().await.unwrap();

        let total = RESUMABLE_CHUNK_SIZE + 5;
        assert_eq!(transport.uploaded(), expected);
        assert_eq!(metadata.content_length(), total as u64);
        assert_eq!(
            transport.ranges(),
            vec![
                format!("bytes 0-{}/*", RESUMABLE_CHUNK_SIZE - 1),
                format!("bytes {RESUMABLE_CHUNK_SIZE}-{}/{total}", total - 1),
                format!("bytes */{total}"),
                format!("bytes {RESUMABLE_CHUNK_SIZE}-{}/{total}", total - 1),
            ]
        );
    }

    #[tokio::test]
    async fn close_small_object_sends_one_conditional_insert() {
        let transport = MockResumableTransport::default();
        let mut writer = test_writer(transport.clone());
        writer
            .write(Buffer::from(Bytes::from_static(b"hello")))
            .await
            .unwrap();

        let metadata = writer.close().await.unwrap();

        assert_eq!(metadata.content_length(), 5);
        assert_eq!(transport.uploaded(), b"hello".to_vec());
        assert_eq!(
            transport.requests(),
            vec!["insert uploadType=media&name=object&ifGenerationMatch=0".to_string()]
        );
    }

    #[tokio::test]
    async fn close_small_object_requires_matching_crc32c_receipt() {
        let body = "a complete GCS CRC32C control\n";
        // CRC32C of the payload from the GCS insert control, in big-endian byte order.
        let checksum = Checksum::new(ChecksumAlgo::Crc32c, [0xcf, 0xc6, 0x9a, 0x90]);
        for (receipt_crc32c, expected) in [
            ("z8aakA==", Ok(checksum)),
            ("AAAAAA==", Err(ErrorKind::Unexpected)),
        ] {
            let transport = MockResumableTransport {
                insert_checksums: Some(("z8aakA==", receipt_crc32c)),
                ..Default::default()
            };
            let op = Operator::new(
                Gcs::default()
                    .bucket("example")
                    .token("test-token".to_string())
                    .disable_config_load()
                    .disable_vm_metadata(),
            )
            .unwrap()
            .with_context(
                OperationContext::new()
                    .with_http_transport(HttpTransporter::new(transport.clone())),
            );
            let mut writer = op
                .writer_with("object")
                .content_type("application/octet-stream")
                .checksum(WriteChecksum::Fast)
                .await
                .unwrap();
            writer.write(body).await.unwrap();

            let result = writer.close().await;
            assert_eq!(transport.requests().len(), 1);
            match expected {
                Ok(checksum) => {
                    let metadata = result.expect("close must accept the matching CRC32C receipt");
                    assert_eq!(metadata.checksum(), Some(checksum));
                    assert_eq!(metadata.content_length(), body.len() as u64);
                }
                Err(kind) => {
                    let error = result.expect_err("close must reject a mismatched CRC32C receipt");
                    assert_eq!(error.kind(), kind);
                }
            }
        }
    }

    #[tokio::test]
    async fn close_full_chunk_sends_one_conditional_insert() {
        let transport = MockResumableTransport::default();
        let mut writer = test_writer(transport.clone());
        let expected = vec![7; RESUMABLE_CHUNK_SIZE];
        writer
            .write(Buffer::from(Bytes::copy_from_slice(&expected)))
            .await
            .unwrap();

        writer.close().await.unwrap();

        assert_eq!(transport.uploaded(), expected);
        assert_eq!(
            transport.requests(),
            vec!["insert uploadType=media&name=object&ifGenerationMatch=0".to_string()]
        );
    }

    #[tokio::test]
    async fn close_multi_chunk_object_uses_resumable_upload() {
        let transport = MockResumableTransport::default();
        let mut writer = test_writer(transport.clone());
        let expected = multi_chunk_body();
        writer
            .write(Buffer::from(Bytes::copy_from_slice(&expected)))
            .await
            .unwrap();

        writer.close().await.unwrap();

        assert_eq!(transport.uploaded(), expected);
        assert_eq!(transport.requests(), vec!["initiate", "chunk", "chunk"]);
    }

    #[tokio::test]
    async fn close_empty_object_sends_one_conditional_insert() {
        let transport = MockResumableTransport::default();
        let mut writer = test_writer(transport.clone());

        let metadata = writer.close().await.unwrap();

        assert_eq!(metadata.content_length(), 0);
        assert_eq!(transport.uploaded(), Vec::<u8>::new());
        assert_eq!(
            transport.requests(),
            vec!["insert uploadType=media&name=object&ifGenerationMatch=0".to_string()]
        );
    }

    #[tokio::test]
    async fn close_if_version_match_sends_one_conditional_insert() {
        let transport = MockResumableTransport::default();
        transport.existing_object(123);
        let mut writer = test_writer_with(
            transport.clone(),
            options::WriteOptions {
                if_version_match: Some("123".to_string()),
                ..Default::default()
            },
        );
        writer
            .write(Buffer::from(Bytes::from_static(b"hello")))
            .await
            .unwrap();

        let metadata = writer.close().await.unwrap();

        assert_eq!(metadata.version(), Some("124"));
        assert_eq!(transport.uploaded(), b"hello".to_vec());
        assert_eq!(
            transport.requests(),
            vec!["insert uploadType=media&name=object&ifGenerationMatch=123".to_string()]
        );
    }

    #[tokio::test]
    async fn close_stale_if_version_match_reports_condition_not_match() {
        let transport = MockResumableTransport::default();
        transport.existing_object(5);
        let mut writer = test_writer_with(
            transport.clone(),
            options::WriteOptions {
                if_version_match: Some("4".to_string()),
                ..Default::default()
            },
        );
        writer
            .write(Buffer::from(Bytes::from_static(b"hello")))
            .await
            .unwrap();

        let err = writer.close().await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert!(!err.is_temporary());
        assert_eq!(transport.uploaded(), Vec::<u8>::new());
        assert_eq!(
            transport.requests(),
            vec!["insert uploadType=media&name=object&ifGenerationMatch=4".to_string()]
        );
    }

    #[tokio::test]
    async fn close_if_version_not_match_sends_one_conditional_insert() {
        let transport = MockResumableTransport::default();
        transport.existing_object(5);
        let mut writer = test_writer_with(
            transport.clone(),
            options::WriteOptions {
                if_version_not_match: Some("7".to_string()),
                ..Default::default()
            },
        );
        writer
            .write(Buffer::from(Bytes::from_static(b"hello")))
            .await
            .unwrap();

        let metadata = writer.close().await.unwrap();

        assert_eq!(metadata.version(), Some("6"));
        assert_eq!(transport.uploaded(), b"hello".to_vec());
        assert_eq!(
            transport.requests(),
            vec!["insert uploadType=media&name=object&ifGenerationNotMatch=7".to_string()]
        );
    }

    #[tokio::test]
    async fn close_with_metadata_sends_one_conditional_multipart_insert() {
        let transport = MockResumableTransport::default();
        let mut writer = test_writer_with(
            transport.clone(),
            options::WriteOptions {
                if_not_exists: true,
                content_type: Some("text/plain".to_string()),
                user_metadata: Some(HashMap::from([("owner".to_string(), "alice".to_string())])),
                ..Default::default()
            },
        );
        writer
            .write(Buffer::from(Bytes::from_static(b"hello")))
            .await
            .unwrap();

        writer.close().await.unwrap();

        assert_eq!(
            transport.requests(),
            vec!["insert uploadType=multipart&name=object&ifGenerationMatch=0".to_string()]
        );
        let sent = String::from_utf8(transport.uploaded()).unwrap();
        assert!(sent.contains(r#""contentType":"text/plain""#), "{sent}");
        assert!(sent.contains(r#""metadata":{"owner":"alice"}"#), "{sent}");
        assert!(sent.contains("hello"), "{sent}");
    }

    #[tokio::test]
    async fn retry_close_reuses_insert_idempotency_token_after_lost_response() {
        let transport = MockResumableTransport::default();
        transport.fail_insert_response();
        let mut writer = test_writer(transport.clone());
        writer
            .write(Buffer::from(Bytes::from_static(b"hello")))
            .await
            .unwrap();

        let err = writer.close().await.unwrap_err();
        assert!(err.is_temporary());
        let metadata = writer.close().await.unwrap();

        assert_eq!(metadata.content_length(), 5);
        assert_eq!(metadata.version(), Some("1"));
        assert_eq!(transport.uploaded(), b"hello".to_vec());
        let tokens = transport.idempotency_tokens();
        assert_eq!(tokens.len(), 2);
        assert!(tokens[0].is_some());
        assert_eq!(tokens[0], tokens[1]);

        let other = MockResumableTransport::default();
        let mut other_writer = test_writer(other.clone());
        other_writer.close().await.unwrap();
        assert_ne!(other.idempotency_tokens()[0], tokens[0]);
    }

    #[tokio::test]
    async fn retry_close_reuses_session_idempotency_token_after_lost_response() {
        let transport = MockResumableTransport::default();
        transport.fail_initiate_response();
        let mut writer = test_writer(transport.clone());
        let expected = multi_chunk_body();
        writer
            .write(Buffer::from(Bytes::copy_from_slice(&expected)))
            .await
            .unwrap();

        let err = writer.close().await.unwrap_err();
        assert!(err.is_temporary());
        writer.close().await.unwrap();

        assert_eq!(transport.uploaded(), expected);
        assert_eq!(
            transport.requests(),
            vec!["initiate", "initiate", "chunk", "chunk"]
        );
        let tokens = transport.idempotency_tokens();
        assert!(tokens[0].is_some());
        assert_eq!(tokens[0], tokens[1]);

        let other = MockResumableTransport::default();
        let mut other_writer = test_writer(other.clone());
        other_writer
            .write(Buffer::from(Bytes::copy_from_slice(&expected)))
            .await
            .unwrap();
        other_writer.close().await.unwrap();
        assert_eq!(other.requests()[0], "initiate");
        assert_ne!(other.idempotency_tokens()[0], tokens[0]);
    }

    #[tokio::test]
    async fn close_if_version_not_match_on_missing_object_reports_condition_not_match() {
        let transport = MockResumableTransport::default();
        let mut writer = test_writer_with(
            transport.clone(),
            options::WriteOptions {
                if_version_not_match: Some("7".to_string()),
                ..Default::default()
            },
        );
        writer
            .write(Buffer::from(Bytes::from_static(b"hello")))
            .await
            .unwrap();

        let err = writer.close().await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert!(!err.is_temporary());
        assert_eq!(transport.uploaded(), Vec::<u8>::new());
    }

    #[tokio::test]
    async fn close_if_version_match_on_missing_object_reports_condition_not_match() {
        let transport = MockResumableTransport::default();
        let mut writer = test_writer_with(
            transport.clone(),
            options::WriteOptions {
                if_version_match: Some("7".to_string()),
                ..Default::default()
            },
        );
        writer
            .write(Buffer::from(Bytes::from_static(b"hello")))
            .await
            .unwrap();

        let err = writer.close().await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert!(!err.is_temporary());
        assert_eq!(transport.uploaded(), Vec::<u8>::new());
    }

    #[tokio::test]
    async fn close_current_if_version_not_match_reports_condition_not_match() {
        let transport = MockResumableTransport::default();
        transport.existing_object(5);
        let mut writer = test_writer_with(
            transport.clone(),
            options::WriteOptions {
                if_version_not_match: Some("5".to_string()),
                ..Default::default()
            },
        );
        writer
            .write(Buffer::from(Bytes::from_static(b"hello")))
            .await
            .unwrap();

        let err = writer.close().await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert!(!err.is_temporary());
        assert_eq!(transport.uploaded(), Vec::<u8>::new());
    }

    #[tokio::test]
    async fn retry_close_after_dedup_window_reports_precondition_failure() {
        let transport = MockResumableTransport::default();
        transport.fail_insert_response();
        transport.ignore_idempotency_tokens();
        let mut writer = test_writer(transport.clone());
        writer
            .write(Buffer::from(Bytes::from_static(b"hello")))
            .await
            .unwrap();

        let err = writer.close().await.unwrap_err();
        assert!(err.is_temporary());
        let err = writer.close().await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert!(!err.is_temporary());
        let insert = "insert uploadType=media&name=object&ifGenerationMatch=0".to_string();
        assert_eq!(transport.requests(), vec![insert.clone(), insert]);
    }
}
