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

use http::HeaderMap;
use http::HeaderValue;
use http::Response;
use http::StatusCode;

use super::core::parse_error;
use super::core::*;
use opendal_core::Buffer;
use opendal_core::Error;
use opendal_core::ErrorKind;
use opendal_core::Metadata;
use opendal_core::MetadataBuilder;
use opendal_core::OperationContext;
use opendal_core::Result;
use opendal_core::raw::*;

pub type CosWriters = TwoWays<oio::MultipartWriter<CosWriter>, oio::AppendWriter<CosWriter>>;

pub struct CosWriter {
    core: Arc<CosCore>,
    ctx: OperationContext,

    op: OpWrite,
    path: String,
}

impl CosWriter {
    pub fn new(core: Arc<CosCore>, ctx: OperationContext, path: &str, op: OpWrite) -> Self {
        CosWriter {
            core,
            ctx,
            path: path.to_string(),
            op,
        }
    }

    fn parse_metadata(headers: &HeaderMap<HeaderValue>) -> Result<Metadata> {
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

        Ok(meta.build())
    }
}

impl oio::MultipartWrite for CosWriter {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        let req = self
            .core
            .cos_put_object_request(&self.path, Some(size), &self.op, body)?;

        let req = self.core.sign(&self.ctx, req).await?;

        let resp = self.core.send(&self.ctx, req).await?;

        let meta = Self::parse_metadata(resp.headers())?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(meta),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("PutObject"))
                    .with_if_not_exists(self.op.if_not_exists()),
                resp,
            )),
        }
    }

    async fn initiate_part(&self) -> Result<String> {
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
        // COS requires part number must between [1..=10000]
        let part_number = part_number + 1;

        let resp = self
            .core
            .cos_upload_part_request(&self.ctx, &self.path, upload_id, part_number, size, body)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
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
        let parts = parts
            .iter()
            .map(|p| CompleteMultipartUploadRequestPart {
                part_number: p.part_number,
                etag: p.etag.clone(),
            })
            .collect();

        let resp = self
            .core
            .cos_complete_multipart_upload(&self.ctx, &self.path, upload_id, parts, &self.op)
            .await?;

        let status = resp.status();
        if status != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("CompleteMultipartUpload"))
                    .with_if_not_exists(self.op.if_not_exists()),
                resp,
            ));
        }

        // COS can return HTTP 200 OK with an XML `<Error>` body when late multipart
        // assembly fails, so check for an error code or missing ETag before succeeding.
        let (parts, body) = resp.into_parts();
        let bs = body.to_bytes();

        let result: CompleteMultipartUploadResult =
            quick_xml::de::from_reader(bs.as_ref()).map_err(new_xml_deserialize_error)?;
        if !result.code.is_empty() {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("CompleteMultipartUpload"))
                    .with_if_not_exists(self.op.if_not_exists()),
                Response::from_parts(parts, Buffer::from(bs)),
            ));
        }
        if result.etag.is_empty() {
            let msg = if result.message.is_empty() {
                "CompleteMultipartUpload response missing ETag".to_string()
            } else {
                format!(
                    "CompleteMultipartUpload failed without error code: {}",
                    result.message
                )
            };
            return Err(with_error_response_context(
                Error::new(ErrorKind::Unexpected, msg)
                    .with_context("service_operation", "CompleteMultipartUpload")
                    .set_temporary(),
                parts,
            ));
        }

        let mut meta = Self::parse_metadata(&parts.headers)?.into_builder();
        meta.etag(&result.etag);
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
    use std::time::Duration;

    use futures::stream;
    use http::Request;

    use super::*;
    use crate::core::test_support::test_core;
    use opendal_core::raw::oio::MultipartWrite;
    use opendal_core::*;

    #[derive(Clone)]
    struct CompletePartTransport {
        status: StatusCode,
        body: &'static str,
    }

    impl HttpTransport for CompletePartTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let buf = Buffer::from(self.body);
            let len = buf.len() as u64;
            Response::builder()
                .status(self.status)
                .body(HttpBody::new(stream::iter([Ok(buf)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    #[tokio::test]
    async fn complete_part_rejects_200_xml_errors_and_empty_etag() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let core = test_core("/");
            let parts = [oio::MultipartPart {
                part_number: 1,
                etag: "\"part1\"".to_string(),
                checksum: None,
                size: None,
            }];

            let cases = [
                (
                    "<Error><Code>InternalError</Code><Message>assembly failed</Message></Error>",
                    false,
                    ErrorKind::Unexpected,
                    true,
                ),
                (
                    "<Error><Code>ServiceUnavailable</Code><Message>busy</Message></Error>",
                    false,
                    ErrorKind::Unexpected,
                    true,
                ),
                (
                    "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>",
                    false,
                    ErrorKind::PermissionDenied,
                    false,
                ),
                (
                    "<Error><Code>FileAlreadyExists</Code><Message>exists</Message></Error>",
                    true,
                    ErrorKind::ConditionNotMatch,
                    false,
                ),
                (
                    "<CompleteMultipartUploadResult/>",
                    false,
                    ErrorKind::Unexpected,
                    true,
                ),
                (
                    "<Error><Message>missing code</Message></Error>",
                    false,
                    ErrorKind::Unexpected,
                    true,
                ),
            ];

            for (body, if_not_exists, expected_kind, expected_temporary) in cases {
                let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(
                    CompletePartTransport {
                        status: StatusCode::OK,
                        body,
                    },
                ));
                let (op, _) = OpWrite::from_options(
                    &core.capability,
                    options::WriteOptions {
                        if_not_exists,
                        ..Default::default()
                    },
                )
                .unwrap();
                let writer = CosWriter::new(core.clone(), ctx, "object.bin", op);
                let err = writer
                    .complete_part("upload-1", &parts)
                    .await
                    .expect_err("complete_part must reject 200-OK XML error / empty ETag");
                assert_eq!(err.kind(), expected_kind, "body={body}");
                assert_eq!(err.is_temporary(), expected_temporary, "body={body}");
            }

            // A valid 200 OK CompleteMultipartUploadResult with non-empty ETag succeeds.
            let ok_ctx = OperationContext::default().with_http_transport(HttpTransporter::new(
                CompletePartTransport {
                    status: StatusCode::OK,
                    body: "<CompleteMultipartUploadResult><Location>loc</Location><Bucket>test-bucket</Bucket><Key>object.bin</Key><ETag>\"abc-2\"</ETag></CompleteMultipartUploadResult>",
                },
            ));
            let writer = CosWriter::new(core, ok_ctx, "object.bin", OpWrite::default());
            let meta = writer.complete_part("upload-1", &parts).await.unwrap();
            assert_eq!(meta.etag(), Some("\"abc-2\""));
        })
        .await
        .expect("test timed out");
    }
}
