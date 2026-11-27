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
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Buf;
use constants::X_TOS_HASH_CRC64ECMA;
use constants::X_TOS_OBJECT_SIZE;
use constants::X_TOS_VERSION_ID;
use crc_fast::{CrcAlgorithm, Digest, checksum_combine};
use http::{HeaderMap, Request, Response, StatusCode};

use crate::core::parse_error;
use crate::core::tos_parse_etag;
use crate::core::*;
use opendal_core::raw::*;
use opendal_core::*;

pub struct TosWriter {
    core: Arc<TosCore>,
    ctx: OperationContext,

    op: OpWrite,
    path: String,
    verify_checksum: bool,
    publication_observed: AtomicBool,
}

impl TosWriter {
    pub fn new(core: Arc<TosCore>, ctx: OperationContext, path: &str, op: OpWrite) -> Result<Self> {
        let verify_checksum = match op.checksum() {
            None => false,
            Some(WriteChecksum::Any | WriteChecksum::Fast)
                if core.capability.write_with_checksum && !op.append() =>
            {
                true
            }
            Some(_) => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "TOS supports Any and Fast checksums for signed, non-append writes",
                ));
            }
        };
        Ok(Self {
            core,
            ctx,
            path: path.to_string(),
            op,
            verify_checksum,
            publication_observed: AtomicBool::new(false),
        })
    }

    fn check_publication_open(&self) -> Result<()> {
        if self.publication_observed.load(Ordering::Relaxed) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "TOS already returned a successful write status. The object may already be visible",
            ));
        }
        Ok(())
    }

    async fn send_publication_request(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        if !self.verify_checksum {
            return self.core.send(&self.ctx, req).await;
        }

        let (parts, mut body) = self.core.fetch(&self.ctx, req).await?.into_parts();
        if parts.status.is_success() {
            // An explicit second close can reach the writer after a receipt failure.
            // Record success before reading the body, which can also fail or be cancelled.
            self.publication_observed.store(true, Ordering::Relaxed);
        }
        let body = body.to_buffer().await.map_err(|err| {
            if parts.status.is_success() {
                Error::new(
                    ErrorKind::Unexpected,
                    "TOS accepted the write but its response could not be read. The object may already be visible",
                )
                .set_source(err)
            } else {
                err
            }
        })?;
        Ok(Response::from_parts(parts, body))
    }

    fn parse_header_into_meta(path: &str, headers: &HeaderMap) -> Result<MetadataBuilder> {
        let mut meta = if path.ends_with('/') {
            MetadataBuilder::dir()
        } else {
            MetadataBuilder::unknown()
        };
        if let Some(etag) = tos_parse_etag(headers)? {
            meta.etag(etag);
        }
        if let Some(version) = parse_header_to_str(headers, X_TOS_VERSION_ID)? {
            meta.version(version);
        }
        if !path.ends_with('/')
            && let Some(value) =
                parse_header_to_str(headers, X_TOS_OBJECT_SIZE)?.and_then(|size| size.parse().ok())
        {
            meta.set_file(value);
        }
        Ok(meta)
    }
}

fn crc64_ecma(body: &Buffer) -> u64 {
    let mut digest = Digest::new(CrcAlgorithm::Crc64Xz);
    for chunk in body.clone() {
        digest.update(&chunk);
    }
    digest.finalize()
}

fn parse_crc64_receipt(headers: &HeaderMap) -> Option<u64> {
    let mut values = headers.get_all(X_TOS_HASH_CRC64ECMA).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn verify_publication_checksum(headers: &HeaderMap, expected: u64) -> Result<Checksum> {
    let received = parse_crc64_receipt(headers);
    if received != Some(expected) {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "TOS did not confirm the write CRC64-ECMA. The object may already be visible",
        )
        .with_context("expected_crc64", expected)
        .with_context("received_crc64", format!("{received:?}")));
    }
    Ok(Checksum::new(
        ChecksumAlgo::Crc64Ecma,
        expected.to_be_bytes(),
    ))
}

impl oio::MultipartWrite for TosWriter {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        self.check_publication_open()?;
        let checksum = self.verify_checksum.then(|| crc64_ecma(&body));
        let req = self
            .core
            .tos_put_object_request(&self.path, Some(size), &self.op, body)?;
        let resp = self.send_publication_request(req).await?;

        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => {
                let mut meta = Self::parse_header_into_meta(&self.path, resp.headers())?;
                if let Some(expected) = checksum {
                    meta.checksum(verify_publication_checksum(resp.headers(), expected)?);
                }
                Ok(meta.build())
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("PutObject")),
                resp,
            )),
        }
    }

    async fn initiate_part(&self) -> Result<String> {
        self.check_publication_open()?;
        let resp = self
            .core
            .tos_initiate_multipart_upload(&self.ctx, &self.path, &self.op)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let result: InitiateMultipartUploadResult =
                    serde_json::from_reader(resp.into_body().reader())
                        .map_err(new_json_deserialize_error)?;

                Ok(result.upload_id)
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("CreateMultipartUpload")),
                resp,
            )),
        }
    }

    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> Result<oio::MultipartPart> {
        self.check_publication_open()?;
        let part_number = part_number + 1;
        let checksum = self.verify_checksum.then(|| crc64_ecma(&body));

        let req =
            self.core
                .tos_upload_part_request(&self.path, upload_id, part_number, size, body)?;
        let resp = self.core.send(&self.ctx, req).await?;

        match resp.status() {
            StatusCode::OK => {
                if let Some(expected) = checksum {
                    let received = parse_crc64_receipt(resp.headers()).ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "TOS returned no valid CRC64-ECMA for the uploaded part",
                        )
                        .with_context("part_number", part_number)
                    })?;
                    if received != expected {
                        return Err(Error::new(
                            ErrorKind::ChecksumMismatch,
                            "TOS part CRC64-ECMA differs from the uploaded bytes",
                        )
                        .with_context("part_number", part_number)
                        .with_context("expected_crc64", expected)
                        .with_context("received_crc64", received));
                    }
                }
                let etag = tos_parse_etag(resp.headers())?
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
                    checksum: checksum.map(|crc| crc.to_string()),
                    size: checksum.map(|_| size),
                })
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("UploadPart")),
                resp,
            )),
        }
    }

    async fn complete_part(
        &self,
        upload_id: &str,
        parts: &[oio::MultipartPart],
    ) -> Result<Metadata> {
        self.check_publication_open()?;
        let checksum = if self.verify_checksum {
            // MultipartWriter yields parts in submission order, which matches the manifest.
            let mut crc = 0;
            for part in parts {
                let part_crc = part
                    .checksum
                    .as_deref()
                    .and_then(|value| value.parse().ok());
                let (Some(part_crc), Some(size)) = (part_crc, part.size) else {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "missing verified CRC64-ECMA or size for TOS completion",
                    )
                    .with_context("part_number", part.part_number));
                };
                crc = checksum_combine(CrcAlgorithm::Crc64Xz, crc, part_crc, size);
            }
            Some(crc)
        } else {
            None
        };
        let parts = parts
            .iter()
            .map(|p| CompleteMultipartUploadRequestPart {
                part_number: p.part_number,
                etag: p.etag.clone(),
            })
            .collect();
        let req = self
            .core
            .tos_complete_multipart_upload_request(&self.path, upload_id, parts, &self.op)?;
        let resp = self.send_publication_request(req).await?;

        match resp.status() {
            StatusCode::OK => {
                let mut meta = Self::parse_header_into_meta(&self.path, resp.headers())?;
                if let Some(expected) = checksum {
                    meta.checksum(verify_publication_checksum(resp.headers(), expected)?);
                }
                let ret: CompleteMultipartUploadResult =
                    serde_json::from_reader(resp.into_body().reader())
                        .map_err(new_json_deserialize_error)?;
                if !ret.code.is_empty() {
                    return Err(Error::new(ErrorKind::Unexpected, ret.message));
                }
                if !ret.etag.is_empty() {
                    // CompleteMultipartUpload response wraps ETag in quotes:
                    // https://www.volcengine.com/docs/6349/74868
                    meta.etag(ret.etag.trim_matches('"'));
                }
                if !ret.version_id.is_empty() {
                    meta.version(&ret.version_id);
                }

                Ok(meta.build())
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("CompleteMultipartUpload")),
                resp,
            )),
        }
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        let resp = self
            .core
            .tos_abort_multipart_upload(&self.ctx, &self.path, upload_id)
            .await?;

        match resp.status() {
            StatusCode::NO_CONTENT => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("AbortMultipartUpload")),
                resp,
            )),
        }
    }
}
