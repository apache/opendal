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

use crc_fast::{CrcAlgorithm, Digest, checksum_combine};
use http::{HeaderMap, Request, Response, StatusCode};

use super::core::parse_error;
use super::core::*;
use opendal_core::raw::*;
use opendal_core::*;

pub type OssWriters = TwoWays<oio::MultipartWriter<OssWriter>, oio::AppendWriter<OssWriter>>;

pub struct OssWriter {
    core: Arc<OssCore>,
    ctx: OperationContext,

    op: OpWrite,
    path: String,
    verify_checksum: bool,
    final_response_received: AtomicBool,
}

impl OssWriter {
    pub fn new(core: Arc<OssCore>, ctx: OperationContext, path: &str, op: OpWrite) -> Result<Self> {
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
                    "OSS cannot fulfill the requested write checksum intent",
                ));
            }
        };
        Ok(Self {
            core,
            ctx,
            path: path.to_string(),
            op,
            verify_checksum,
            final_response_received: AtomicBool::new(false),
        })
    }

    fn check_upload_open(&self) -> Result<()> {
        if self.final_response_received.load(Ordering::Relaxed) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "OSS already accepted the final write request. The object may already be visible",
            ));
        }
        Ok(())
    }

    fn parse_metadata(headers: &HeaderMap) -> Result<MetadataBuilder> {
        let mut meta = MetadataBuilder::unknown();
        if let Some(etag) = parse_etag(headers)? {
            meta.etag(etag);
        }
        if let Some(md5) = parse_content_md5(headers)? {
            meta.content_md5(md5);
        }
        if let Some(version) = parse_header_to_str(headers, constants::X_OSS_VERSION_ID)? {
            meta.version(version);
        }

        Ok(meta)
    }

    async fn finish_write(
        &self,
        request: Request<Buffer>,
        operation: ServiceOperation,
        checksum: Option<u64>,
    ) -> Result<Metadata> {
        let request = self.core.sign(&self.ctx, request).await?;
        let (parts, mut body) = self.ctx.http_transport().fetch(request).await?.into_parts();
        let accepted = parts.status == StatusCode::OK
            || (operation == ServiceOperation("PutObject") && parts.status == StatusCode::CREATED);
        let verify_receipt = accepted && checksum.is_some();
        if verify_receipt {
            // A successful status may precede a lost body or an invalid receipt. A later
            // close must not publish the object again even when verification fails.
            self.final_response_received.store(true, Ordering::Relaxed);
        }
        let body = body.to_buffer().await.map_err(|err| {
            if verify_receipt {
                Error::new(
                    ErrorKind::Unexpected,
                    "OSS accepted the write but its response could not be read. The object may already be visible",
                )
                .with_context("service_operation", operation.0)
                .set_source(err)
            } else {
                err
            }
        })?;
        let mut metadata = Self::parse_metadata(&parts.headers)?;
        if !accepted {
            return Err(parse_error(
                ErrorContext::new(operation).with_if_not_exists(self.op.if_not_exists()),
                Response::from_parts(parts, body),
            ));
        }
        if let Some(expected) = checksum {
            let received = parse_crc64_ecma(&parts.headers);
            if received != Some(expected) {
                let received = received
                    .map_or_else(|| "missing or invalid".to_string(), |crc| crc.to_string());
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "OSS accepted the write without a matching CRC64-ECMA receipt. The object may already be visible",
                )
                .with_context("service_operation", operation.0)
                .with_context("expected_crc64_ecma", expected)
                .with_context("received_crc64_ecma", received));
            }
            metadata.checksum(Checksum::new(
                ChecksumAlgo::Crc64Ecma,
                expected.to_be_bytes(),
            ));
        }
        Ok(metadata.build())
    }
}

fn crc64_ecma(body: &Buffer) -> u64 {
    let mut digest = Digest::new(CrcAlgorithm::Crc64Xz);
    for chunk in body.clone() {
        digest.update(&chunk);
    }
    digest.finalize()
}

fn parse_crc64_ecma(headers: &HeaderMap) -> Option<u64> {
    let mut values = headers.get_all(constants::X_OSS_HASH_CRC64_ECMA).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

impl oio::MultipartWrite for OssWriter {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        self.check_upload_open()?;
        let checksum = self.verify_checksum.then(|| crc64_ecma(&body));
        let request =
            self.core
                .oss_put_object_request(&self.path, Some(size), &self.op, body, false)?;
        self.finish_write(request, ServiceOperation("PutObject"), checksum)
            .await
    }

    async fn initiate_part(&self) -> Result<String> {
        self.check_upload_open()?;
        let resp = self
            .core
            .oss_initiate_upload(&self.ctx, &self.path, &self.op, false)
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
        self.check_upload_open()?;
        // OSS requires part number must between [1..=10000]
        let part_number = part_number + 1;
        let checksum = self.verify_checksum.then(|| crc64_ecma(&body));

        let resp = self
            .core
            .oss_upload_part_request(
                &self.ctx,
                &self.path,
                upload_id,
                part_number,
                false,
                size,
                body,
            )
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let checksum = if let Some(expected) = checksum {
                    let received = parse_crc64_ecma(resp.headers()).ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "OSS part receipt has no valid CRC64-ECMA",
                        )
                        .with_context("part_number", part_number)
                    })?;
                    if received != expected {
                        // Retrying this part can replace it before the upload is published.
                        return Err(Error::new(
                            ErrorKind::ChecksumMismatch,
                            "OSS part receipt does not match the uploaded CRC64-ECMA",
                        )
                        .with_context("part_number", part_number)
                        .set_temporary());
                    }
                    Some(received.to_string())
                } else {
                    None
                };
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
                    size: checksum.as_ref().map(|_| size),
                    checksum,
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
        self.check_upload_open()?;
        let checksum = if self.verify_checksum {
            // MultipartWriter provides receipts in submission order, which is also
            // the order of the manifest. The CRC of the empty prefix is zero.
            let mut crc = 0;
            for part in parts {
                let (part_crc, size) = part
                    .checksum
                    .as_deref()
                    .and_then(|value| value.parse().ok())
                    .zip(part.size)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "OSS part is missing its verified CRC64-ECMA or byte length",
                        )
                        .with_context("part_number", part.part_number)
                    })?;
                crc = checksum_combine(CrcAlgorithm::Crc64Xz, crc, part_crc, size);
            }
            Some(crc)
        } else {
            None
        };
        let manifest = parts
            .iter()
            .map(|p| MultipartUploadPart {
                part_number: p.part_number,
                etag: p.etag.clone(),
            })
            .collect();
        let request = self
            .core
            .oss_complete_multipart_upload_request(&self.path, upload_id, manifest, &self.op)?;
        self.finish_write(
            request,
            ServiceOperation("CompleteMultipartUpload"),
            checksum,
        )
        .await
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        let resp = self
            .core
            .oss_abort_multipart_upload(&self.ctx, &self.path, upload_id)
            .await?;
        if resp.status() == StatusCode::NO_CONTENT {
            return Ok(());
        }
        let err = parse_error(
            ErrorContext::new(ServiceOperation("AbortMultipartUpload")),
            resp,
        );
        if self.final_response_received.load(Ordering::Relaxed) && err.kind() == ErrorKind::NotFound
        {
            // Completion may have consumed the upload even when its receipt could not be verified.
            return Ok(());
        }
        Err(err)
    }
}

impl oio::AppendWrite for OssWriter {
    async fn offset(&self) -> Result<u64> {
        let resp = self
            .core
            .oss_head_object(&self.ctx, &self.path, &OpStat::new())
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
            .oss_append_object_request(&self.path, offset, size, &self.op, body)?;
        let req = self.core.sign(&self.ctx, req).await?;

        let resp = self.core.send(&self.ctx, req).await?;

        let meta = Self::parse_metadata(resp.headers())?.build();
        let status = resp.status();

        match status {
            StatusCode::OK => Ok(meta),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("AppendObject")),
                resp,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::Oss;

    struct LostResponseBody(Arc<AtomicUsize>);

    impl HttpTransport for LostResponseBody {
        async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
            assert_eq!(request.method(), http::Method::PUT);
            self.0.fetch_add(1, Ordering::Relaxed);
            let body = futures::stream::iter([Err(Error::new(
                ErrorKind::Unexpected,
                "test transport lost the response body",
            )
            .set_temporary())]);
            Ok(Response::builder()
                .header(constants::X_OSS_HASH_CRC64_ECMA, "11051210869376104954")
                .body(HttpBody::new(body, None))
                .unwrap())
        }
    }

    #[tokio::test]
    async fn test_checksum_write_does_not_replay_after_a_lost_success_body() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let op = Operator::new(
            Oss::default()
                .bucket("test-bucket")
                .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
                .access_key_id("test-key")
                .access_key_secret("test-secret"),
        )
        .unwrap();
        let transport = HttpTransporter::new(LostResponseBody(attempts.clone()));
        let context = op.context().with_http_transport(transport);
        let op = op.with_context(context);
        let mut writer = op
            .writer_with("data")
            .checksum(WriteChecksum::Fast)
            .await
            .unwrap();
        writer.write("123456789").await.unwrap();
        let error = writer.close().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(error.is_permanent());
        let retry = writer.close().await.unwrap_err();
        assert_eq!(retry.kind(), ErrorKind::Unexpected);
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }
}
