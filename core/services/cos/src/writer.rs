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
use crc_fast::{CrcAlgorithm, Digest, checksum_combine};
use http::{HeaderMap, Request, Response, StatusCode};
use serde::Deserialize;

use super::core::parse_error;
use super::core::*;
use opendal_core::Buffer;
use opendal_core::Checksum;
use opendal_core::ChecksumAlgo;
use opendal_core::Error;
use opendal_core::ErrorKind;
use opendal_core::Metadata;
use opendal_core::MetadataBuilder;
use opendal_core::OperationContext;
use opendal_core::Result;
use opendal_core::WriteChecksum;
use opendal_core::raw::*;

pub type CosWriters = TwoWays<oio::MultipartWriter<CosWriter>, oio::AppendWriter<CosWriter>>;

pub struct CosWriter {
    core: Arc<CosCore>,
    ctx: OperationContext,

    op: OpWrite,
    path: String,
    verify_checksum: bool,
    publication_observed: AtomicBool,
}

#[derive(Deserialize)]
enum CompleteMultipartUploadResponse {
    #[serde(rename = "CompleteMultipartUploadResult")]
    Published(CompleteMultipartUploadResult),
    Error {
        #[serde(rename = "Code")]
        code: String,
    },
}

impl CosWriter {
    pub fn new(core: Arc<CosCore>, ctx: OperationContext, path: &str, op: OpWrite) -> Result<Self> {
        let verify_checksum = match op.checksum() {
            None => false,
            Some(_) if op.append() => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "COS does not support write checksums with append",
                ));
            }
            Some(WriteChecksum::Any | WriteChecksum::Fast) => true,
            Some(_) => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "COS supports only the Any and Fast write checksum intents",
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
                "COS already returned a successful write response; the object may already be visible",
            ));
        }
        Ok(())
    }

    async fn send_publication_request(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        let req = self.core.sign(&self.ctx, req).await?;
        let (parts, mut body) = self.ctx.http_transport().fetch(req).await?.into_parts();
        let publication_observed = self.verify_checksum && parts.status.is_success();
        if publication_observed {
            // Hold off retries even if reading the response fails or is cancelled.
            self.publication_observed.store(true, Ordering::Relaxed);
        }
        let body = body.to_buffer().await.map_err(|err| {
            if publication_observed {
                Error::new(
                    ErrorKind::Unexpected,
                    "could not read COS write response; the object may already be visible",
                )
                .set_source(err)
            } else {
                err
            }
        })?;
        Ok(Response::from_parts(parts, body))
    }

    fn parse_metadata(headers: &HeaderMap) -> Result<MetadataBuilder> {
        let mut meta = MetadataBuilder::unknown();
        if let Some(etag) = parse_etag(headers)? {
            meta.etag(etag);
        }
        if let Some(md5) = parse_content_md5(headers)? {
            meta.content_md5(md5);
        }
        if let Some(version) = parse_header_to_str(headers, constants::X_COS_VERSION_ID)?
            && version != "null"
        {
            meta.version(version);
        }

        Ok(meta)
    }
}

fn crc64(body: &Buffer) -> u64 {
    let mut digest = Digest::new(CrcAlgorithm::Crc64Xz);
    for chunk in body.clone() {
        digest.update(&chunk);
    }
    digest.finalize()
}

fn parse_crc64_receipt(headers: &HeaderMap) -> Option<u64> {
    let mut values = headers.get_all(constants::X_COS_HASH_CRC64ECMA).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn confirm_publication_checksum(
    headers: &HeaderMap,
    expected: u64,
    meta: &mut MetadataBuilder,
) -> Result<()> {
    if parse_crc64_receipt(headers) != Some(expected) {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "COS write did not confirm the CRC64-ECMA receipt; the object may already be visible",
        )
        .with_context("expected_crc64", expected));
    }
    meta.checksum(Checksum::new(
        ChecksumAlgo::Crc64Ecma,
        expected.to_be_bytes(),
    ));
    Ok(())
}

fn combine_part_checksums(parts: &[oio::MultipartPart]) -> Result<u64> {
    // MultipartWriter returns parts in submission order, even with concurrent uploads.
    parts.iter().try_fold(0, |prefix, part| {
        let crc = part
            .checksum
            .as_deref()
            .and_then(|value| value.parse().ok());
        let (Some(crc), Some(size)) = (crc, part.size) else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "COS multipart upload is missing a verified part CRC64-ECMA or length",
            )
            .with_context("part_number", part.part_number));
        };
        Ok(checksum_combine(CrcAlgorithm::Crc64Xz, prefix, crc, size))
    })
}

impl oio::MultipartWrite for CosWriter {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        self.check_publication_open()?;
        let checksum = self.verify_checksum.then(|| crc64(&body));
        let req = self
            .core
            .cos_put_object_request(&self.path, Some(size), &self.op, body)?;
        let resp = self.send_publication_request(req).await?;

        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => {
                let mut meta = Self::parse_metadata(resp.headers())?;
                if let Some(checksum) = checksum {
                    confirm_publication_checksum(resp.headers(), checksum, &mut meta)?;
                }
                Ok(meta.build())
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("PutObject"))
                    .with_if_not_exists(self.op.if_not_exists())
                    .with_checksum(self.verify_checksum),
                resp,
            )),
        }
    }

    async fn initiate_part(&self) -> Result<String> {
        self.check_publication_open()?;
        let resp = self
            .core
            .cos_initiate_multipart_upload(&self.ctx, &self.path, &self.op)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let result: InitiateMultipartUploadResult =
                    quick_xml::de::from_reader(bytes::Buf::reader(bs))
                        .map_err(new_xml_deserialize_error)?;

                Ok(result.upload_id)
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("InitiateMultipartUpload"))
                    .with_if_not_exists(self.op.if_not_exists()),
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
        // COS requires part number must between [1..=10000]
        let part_number = part_number + 1;
        let checksum = self.verify_checksum.then(|| crc64(&body));

        let resp = self
            .core
            .cos_upload_part_request(&self.ctx, &self.path, upload_id, part_number, size, body)
            .await?;

        match resp.status() {
            StatusCode::OK => {
                if let Some(expected) = checksum {
                    let received = parse_crc64_receipt(resp.headers()).ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "COS returned no valid part CRC64-ECMA receipt",
                        )
                        .with_context("part_number", part_number)
                        .set_temporary()
                    })?;
                    if received != expected {
                        // Replacing the same staged part is safe before completion.
                        return Err(Error::new(
                            ErrorKind::ChecksumMismatch,
                            "COS part CRC64-ECMA receipt differs from the uploaded bytes",
                        )
                        .with_context("part_number", part_number)
                        .with_context("expected_crc64", expected)
                        .with_context("received_crc64", received)
                        .set_temporary());
                    }
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
                    checksum: checksum.map(|value| value.to_string()),
                    size: checksum.map(|_| size),
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
        self.check_publication_open()?;
        let checksum = self
            .verify_checksum
            .then(|| combine_part_checksums(parts))
            .transpose()?;
        let parts = parts
            .iter()
            .map(|p| CompleteMultipartUploadRequestPart {
                part_number: p.part_number,
                etag: p.etag.clone(),
            })
            .collect();

        let req = self.core.cos_complete_multipart_upload_request(
            &self.path,
            upload_id,
            parts,
            self.op.if_not_exists(),
            Operation::Write,
        )?;
        let resp = self.send_publication_request(req).await?;
        let error_context = ErrorContext::new(ServiceOperation("CompleteMultipartUpload"))
            .with_if_not_exists(self.op.if_not_exists())
            .with_checksum(self.verify_checksum);
        if resp.status() != StatusCode::OK {
            return Err(parse_error(error_context, resp));
        }

        let body = resp.body().clone().reader();
        let result = if self.verify_checksum {
            let result = quick_xml::de::from_reader(body).map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "COS returned invalid multipart completion XML; the object may already be visible",
                )
                .set_source(err)
            })?;
            match result {
                CompleteMultipartUploadResponse::Published(result)
                    if !result.etag.trim().is_empty() =>
                {
                    result
                }
                CompleteMultipartUploadResponse::Error { code } if !code.trim().is_empty() => {
                    // An explicit rejection in the XML body leaves the upload uncommitted.
                    self.publication_observed.store(false, Ordering::Relaxed);
                    return Err(parse_error(error_context, resp));
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "COS returned an incomplete multipart completion result; the object may already be visible",
                    ));
                }
            }
        } else {
            quick_xml::de::from_reader(body).map_err(new_xml_deserialize_error)?
        };
        let mut meta = Self::parse_metadata(resp.headers())?;
        meta.etag(&result.etag);
        if let Some(checksum) = checksum {
            confirm_publication_checksum(resp.headers(), checksum, &mut meta)?;
        }
        Ok(meta.build())
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        let resp = self
            .core
            .cos_abort_multipart_upload(&self.ctx, &self.path, upload_id)
            .await?;
        match resp.status() {
            // cos returns code 204 if abort succeeds.
            // Reference: https://www.tencentcloud.com/document/product/436/7740
            StatusCode::NO_CONTENT => Ok(()),
            // Completion consumes the upload. Abort remains usable if a 200
            // response was observed but its body could not confirm completion.
            StatusCode::NOT_FOUND if self.publication_observed.load(Ordering::Relaxed) => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("AbortMultipartUpload")),
                resp,
            )),
        }
    }
}

impl oio::AppendWrite for CosWriter {
    async fn offset(&self) -> Result<u64> {
        let resp = self
            .core
            .cos_head_object(&self.ctx, &self.path, &OpStat::default())
            .await?;

        let status = resp.status();
        match status {
            StatusCode::OK => {
                let content_length = parse_content_length(resp.headers())?.ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Content-Length not present in returning response",
                    )
                })?;
                Ok(content_length)
            }
            StatusCode::NOT_FOUND => Ok(0),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("HeadObject")),
                resp,
            )),
        }
    }

    async fn append(&self, offset: u64, size: u64, body: Buffer) -> Result<Metadata> {
        let req = self
            .core
            .cos_append_object_request(&self.path, offset, size, &self.op, body)?;

        let req = self.core.sign(&self.ctx, req).await?;

        let resp = self.core.send(&self.ctx, req).await?;

        let meta = Self::parse_metadata(resp.headers())?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(meta.build()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("AppendObject")),
                resp,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use futures::stream;
    use oio::Write;
    use opendal_core::options::WriteOptions;
    use opendal_core::{Builder, HttpBody, HttpTransport, HttpTransporter};

    use super::*;

    struct Reply {
        operation: &'static str,
        crc: Option<&'static str>,
        body: Result<Buffer>,
    }

    struct ScriptedTransport(Mutex<VecDeque<Reply>>);

    impl HttpTransport for ScriptedTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let Reply {
                operation,
                crc,
                body,
            } = self
                .0
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected request");
            assert_eq!(
                req.extensions().get::<ServiceOperation>().unwrap().0,
                operation
            );
            let mut response = Response::builder().header(http::header::ETAG, "\"part\"");
            if let Some(crc) = crc {
                response = response.header(constants::X_COS_HASH_CRC64ECMA, crc);
            }
            Ok(response
                .body(HttpBody::new(stream::iter([body]), None))
                .unwrap())
        }
    }

    fn test_writer(replies: impl IntoIterator<Item = Reply>) -> impl Write {
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(
            ScriptedTransport(Mutex::new(replies.into_iter().collect())),
        ));
        let service = crate::Cos::default()
            .bucket("test")
            .endpoint("https://cos.example.com")
            .secret_id("test")
            .secret_key("test")
            .disable_config_load()
            .build()
            .unwrap();
        let args = OpWrite::from_options(
            &service.capability(),
            WriteOptions {
                checksum: Some(WriteChecksum::Fast),
                ..Default::default()
            },
        )
        .unwrap()
        .0;
        service.write(&ctx, "target.bin", args).unwrap()
    }

    fn staged_parts() -> [Reply; 3] {
        [
            Reply {
                operation: "InitiateMultipartUpload",
                crc: None,
                body: Ok(Buffer::from(
                    "<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>",
                )),
            },
            Reply {
                operation: "UploadPart",
                crc: Some("14865968487149912872"),
                body: Ok(Buffer::new()),
            },
            Reply {
                operation: "UploadPart",
                crc: Some("7598038797645275211"),
                body: Ok(Buffer::new()),
            },
        ]
    }

    #[tokio::test]
    async fn a_success_status_with_a_lost_body_prevents_replaying_close() {
        for (operation, parts) in [
            ("PutObject", Vec::new()),
            ("CompleteMultipartUpload", Vec::from(staged_parts())),
        ] {
            let mut writer =
                test_writer(
                    parts.into_iter().chain([Reply {
                        operation,
                        crc: Some("11051210869376104954"),
                        body: Err(Error::new(ErrorKind::Unexpected, "response body truncated")
                            .set_temporary()),
                    }]),
                );
            if operation == "PutObject" {
                writer.write(Buffer::from("123456789")).await.unwrap();
            } else {
                writer.write(Buffer::from("1234")).await.unwrap();
                writer.write(Buffer::from("56789")).await.unwrap();
            }

            let error = writer.close().await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Unexpected);
            assert!(error.is_permanent());
            assert!(error.message().contains("may already be visible"));
            let error = writer.close().await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Unexpected);
            assert!(error.is_permanent());
        }
    }

    #[tokio::test]
    async fn a_completion_error_body_reports_rejection_before_publication() {
        let mut writer = test_writer(staged_parts().into_iter().chain([
            Reply {
                operation: "CompleteMultipartUpload",
                crc: None,
                body: Ok(Buffer::from("<Error><Code>BadDigest</Code><Message>digest rejected</Message></Error>")),
            },
            Reply {
                operation: "CompleteMultipartUpload",
                crc: Some("11051210869376104954"),
                body: Ok(Buffer::from("<CompleteMultipartUploadResult><ETag>&quot;complete&quot;</ETag></CompleteMultipartUploadResult>")),
            },
        ]));
        writer.write(Buffer::from("1234")).await.unwrap();
        writer.write(Buffer::from("56789")).await.unwrap();

        let error = writer.close().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ChecksumMismatch);
        assert!(error.is_permanent());
        let meta = writer.close().await.unwrap();
        assert_eq!(meta.etag(), Some("\"complete\""));
        assert_eq!(
            meta.checksum(),
            Some(Checksum::new(
                ChecksumAlgo::Crc64Ecma,
                0x995d_c9bb_df19_39fa_u64.to_be_bytes()
            ))
        );
    }
}
