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
use http::StatusCode;
use sha1::{Digest, Sha1, digest::Output};

use super::core::StartLargeFileResponse;
use super::core::UploadPartResponse;
use super::core::UploadResponse;
use super::core::parse_error;
use super::core::{B2Core, ErrorContext};
use opendal_core::raw::*;
use opendal_core::*;

pub type B2Writers = oio::MultipartWriter<B2Writer>;

pub struct B2Writer {
    core: Arc<B2Core>,
    ctx: OperationContext,

    op: OpWrite,
    path: String,
    verify_checksum: bool,
    completed_single_upload: AtomicBool,
}

impl B2Writer {
    pub fn new(core: Arc<B2Core>, ctx: OperationContext, path: &str, op: OpWrite) -> Result<Self> {
        let verify_checksum = match op.checksum() {
            None => false,
            Some(WriteChecksum::Any) => true,
            Some(_) => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "B2 only supports the Any write checksum intent",
                ));
            }
        };
        Ok(Self {
            core,
            ctx,
            path: path.to_string(),
            op,
            verify_checksum,
            completed_single_upload: AtomicBool::new(false),
        })
    }

    fn check_single_upload_open(&self) -> Result<()> {
        if self.completed_single_upload.load(Ordering::Relaxed) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "B2 already completed the ordinary upload",
            ));
        }
        Ok(())
    }

    fn upload_metadata(path: &str, resp: UploadResponse) -> MetadataBuilder {
        let mut meta = if path.ends_with('/') {
            MetadataBuilder::dir()
        } else {
            MetadataBuilder::file(resp.content_length)
        };

        if let Some(md5) = resp.content_md5 {
            meta.content_md5(&md5);
        }

        if let Some(content_type) = resp.content_type {
            meta.content_type(&content_type);
        }

        meta
    }
}

fn sha1(body: &Buffer) -> (Output<Sha1>, String) {
    use std::fmt::Write;

    let mut hasher = Sha1::new();
    for chunk in body.clone() {
        hasher.update(&chunk);
    }
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest.iter() {
        write!(&mut encoded, "{byte:02x}").expect("writing to String must succeed");
    }
    (digest, encoded)
}

impl oio::MultipartWrite for B2Writer {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        self.check_single_upload_open()?;
        let checksum = self.verify_checksum.then(|| sha1(&body));
        let mut resp = self
            .core
            .upload_file(
                &self.ctx,
                &self.path,
                size,
                &self.op,
                checksum.as_ref().map(|(_, encoded)| encoded.as_str()),
                body,
            )
            .await?;

        let status = resp.status();
        let completed = status == StatusCode::OK && self.verify_checksum;
        if completed {
            // A lost or invalid receipt must not publish a second version on another close.
            self.completed_single_upload.store(true, Ordering::Relaxed);
        }
        let body = resp
            .body_mut()
            .to_buffer()
            .await
            .map_err(|err| err.with_permanent(completed))?;

        match status {
            StatusCode::OK => {
                let result: UploadResponse =
                    serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;

                if let Some((_, sent)) = &checksum
                    && result.content_sha1.as_deref() != Some(sent.as_str())
                {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "B2 completed the upload without confirming the submitted SHA-1; \
                         the object may already be visible",
                    ));
                }
                let mut meta = Self::upload_metadata(&self.path, result);
                if let Some((digest, _)) = checksum {
                    meta.checksum(Checksum::new(ChecksumAlgo::Sha1, digest.as_slice()));
                }
                Ok(meta.build())
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("UploadFile"))
                    .with_checksum(self.verify_checksum),
                resp.map(|_| body),
            )),
        }
    }

    async fn initiate_part(&self) -> Result<String> {
        self.check_single_upload_open()?;
        let resp = self
            .core
            .start_large_file(&self.ctx, &self.path, &self.op)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let result: StartLargeFileResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                Ok(result.file_id)
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("StartLargeFile")),
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
        // B2 requires part number must between [1..=10000]
        let part_number = part_number + 1;
        let checksum = self.verify_checksum.then(|| sha1(&body).1);

        let resp = self
            .core
            .upload_part(
                &self.ctx,
                upload_id,
                part_number,
                size,
                checksum.as_deref(),
                body,
            )
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let result: UploadPartResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                if let Some(sent) = checksum
                    && result.content_sha1 != sent
                {
                    // An unconfirmed part can be replaced on the same upload and part number.
                    return Err(Error::new(
                        ErrorKind::ChecksumMismatch,
                        "B2 part receipt does not confirm the submitted SHA-1",
                    )
                    .with_context("part_number", part_number.to_string())
                    .set_temporary());
                }

                Ok(oio::MultipartPart {
                    etag: result.content_sha1,
                    part_number,
                    checksum: None,
                    size: None,
                })
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("UploadPart"))
                    .with_checksum(self.verify_checksum),
                resp,
            )),
        }
    }

    async fn complete_part(
        &self,
        upload_id: &str,
        parts: &[oio::MultipartPart],
    ) -> Result<Metadata> {
        // MultipartWriter collects receipts in part order even with concurrent uploads.
        let part_sha1_array = parts
            .iter()
            .map(|part| {
                if self.verify_checksum {
                    part.etag.clone()
                } else {
                    part.etag
                        .strip_prefix("unverified:")
                        .unwrap_or_default()
                        .to_string()
                }
            })
            .collect();

        let resp = self
            .core
            .finish_large_file(&self.ctx, upload_id, part_sha1_array)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let result: UploadResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                Ok(Self::upload_metadata(&self.path, result).build())
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("FinishLargeFile"))
                    .with_checksum(self.verify_checksum),
                resp,
            )),
        }
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        let resp = self.core.cancel_large_file(&self.ctx, upload_id).await?;
        match resp.status() {
            // b2 returns code 200 if abort succeeds.
            StatusCode::OK => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("CancelLargeFile")),
                resp,
            )),
        }
    }
}
