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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use bytes::Buf;
use constants::X_AMZ_OBJECT_SIZE;
use constants::X_AMZ_VERSION_ID;
use http::Response;
use http::StatusCode;

use crate::core::parse_error;
use crate::core::*;
use opendal_core::raw::*;
use opendal_core::*;

pub type S3Writers = TwoWays<oio::MultipartWriter<S3Writer>, oio::AppendWriter<S3Writer>>;

pub struct S3Writer {
    core: Arc<S3Core>,
    ctx: OperationContext,

    op: OpWrite,
    path: String,
    appended: AtomicBool,
}

impl S3Writer {
    pub fn new(core: Arc<S3Core>, ctx: OperationContext, path: &str, op: OpWrite) -> Self {
        S3Writer {
            core,
            ctx,
            path: path.to_string(),
            op,
            appended: AtomicBool::new(false),
        }
    }

    fn parse_header_into_meta(path: &str, headers: &http::HeaderMap) -> Result<Metadata> {
        let mut meta = if path.ends_with('/') {
            MetadataBuilder::dir()
        } else {
            MetadataBuilder::unknown()
        };
        if let Some(etag) = parse_etag(headers)? {
            meta.etag(etag);
        }
        if let Some(version) = parse_header_to_str(headers, X_AMZ_VERSION_ID)? {
            meta.version(version);
        }
        if !path.ends_with('/')
            && let Some(value) =
                parse_header_to_str(headers, X_AMZ_OBJECT_SIZE)?.and_then(|size| size.parse().ok())
        {
            meta.set_file(value);
        }
        Ok(meta.build())
    }

    fn error_context(&self, service_operation: ServiceOperation) -> ErrorContext {
        ErrorContext::new(service_operation).with_caller_condition(self.op.is_conditional())
    }
}

impl oio::MultipartWrite for S3Writer {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        let req = self
            .core
            .s3_put_object_request(&self.path, Some(size), &self.op, body)?;

        let resp = self.core.send(&self.ctx, req).await?;

        let status = resp.status();

        let meta = S3Writer::parse_header_into_meta(&self.path, resp.headers())?;

        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(meta),
            _ => {
                let err = parse_error(self.error_context(ServiceOperation("PutObject")), resp);
                if self.op.if_match().is_some() && err.kind() == ErrorKind::NotFound {
                    Err(Error::new(
                        ErrorKind::ConditionNotMatch,
                        "write precondition requires a live target",
                    ))
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn initiate_part(&self) -> Result<String> {
        let resp = self
            .core
            .s3_initiate_multipart_upload(&self.ctx, &self.path, &self.op)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let result: InitiateMultipartUploadResult =
                    quick_xml::de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

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
        // AWS S3 requires part number must between [1..=10000]
        let part_number = part_number + 1;

        let checksum = self.core.calculate_checksum(&body);

        let req = self.core.s3_upload_part_request(
            &self.path,
            upload_id,
            part_number,
            size,
            body,
            checksum.clone(),
        )?;

        let resp = self.core.send(&self.ctx, req).await?;

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
                    checksum,
                    size: None,
                })
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("UploadPart")),
                resp,
            )),
        }
    }

    async fn copy_part(
        &self,
        upload_id: &str,
        part_number: usize,
        path: &str,
        args: OpRead,
        range: BytesRange,
    ) -> Result<oio::MultipartPart> {
        let size = range
            .size()
            .expect("multipart writer copy range must be bounded");
        let part_number = part_number + 1;
        let error_context = ErrorContext::new(ServiceOperation("UploadPartCopy"))
            .with_caller_condition(args.is_conditional());
        let req = self
            .core
            .s3_upload_part_copy_request(S3UploadPartCopyRequest {
                from: path,
                to: &self.path,
                source_version: args.version(),
                if_match: args.if_match(),
                if_none_match: args.if_none_match(),
                if_modified_since: args.if_modified_since(),
                if_unmodified_since: args.if_unmodified_since(),
                upload_id,
                part_number,
                range,
                operation: Operation::Write,
            })?;

        let resp = self.core.send_iam(&self.ctx, req).await?;
        match resp.status() {
            StatusCode::OK => {
                let (parts, body) = resp.into_parts();
                let bs = body.to_bytes();
                let result: CopyObjectResult =
                    quick_xml::de::from_reader(bs.as_ref()).map_err(new_xml_deserialize_error)?;

                // S3 may return 200 OK with an <Error> body for UploadPartCopy.
                if result.etag.is_empty() {
                    return Err(parse_error(
                        error_context,
                        Response::from_parts(parts, Buffer::from(bs)),
                    ));
                }

                Ok(oio::MultipartPart {
                    part_number,
                    etag: result.etag,
                    checksum: None,
                    size: Some(size),
                })
            }
            _ => Err(parse_error(error_context, resp)),
        }
    }

    async fn complete_part(
        &self,
        upload_id: &str,
        parts: &[oio::MultipartPart],
    ) -> Result<Metadata> {
        let parts = parts
            .iter()
            .map(|p| match &self.core.checksum_algorithm {
                None => CompleteMultipartUploadRequestPart {
                    part_number: p.part_number,
                    etag: p.etag.clone(),
                    ..Default::default()
                },
                Some(checksum_algorithm) => match checksum_algorithm {
                    ChecksumAlgorithm::Crc32c => CompleteMultipartUploadRequestPart {
                        part_number: p.part_number,
                        etag: p.etag.clone(),
                        checksum_crc32c: p.checksum.clone(),
                    },
                    ChecksumAlgorithm::Md5 => CompleteMultipartUploadRequestPart {
                        part_number: p.part_number,
                        etag: p.etag.clone(),
                        ..Default::default()
                    },
                },
            })
            .collect();

        let resp = self
            .core
            .s3_complete_multipart_upload(&self.ctx, &self.path, upload_id, parts, &self.op)
            .await?;

        let status = resp.status();

        let meta = S3Writer::parse_header_into_meta(&self.path, resp.headers())?;

        match status {
            StatusCode::OK => {
                // still check if there is any error because S3 might return error for status code 200
                // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html#API_CompleteMultipartUpload_Example_4
                let (parts, body) = resp.into_parts();
                let bs = body.to_bytes();

                let ret: CompleteMultipartUploadResult =
                    quick_xml::de::from_reader(bs.as_ref()).map_err(new_xml_deserialize_error)?;
                if !ret.code.is_empty() || ret.etag.is_empty() {
                    let err = parse_error(
                        self.error_context(ServiceOperation("CompleteMultipartUpload")),
                        Response::from_parts(parts, Buffer::from(bs)),
                    );
                    return if self.op.if_match().is_some() && err.kind() == ErrorKind::NotFound {
                        Err(Error::new(
                            ErrorKind::ConditionNotMatch,
                            "write precondition requires a live target",
                        ))
                    } else {
                        Err(err)
                    };
                }
                let mut meta = meta.into_builder();
                meta.etag(&ret.etag);

                Ok(meta.build())
            }
            _ => {
                let err = parse_error(
                    self.error_context(ServiceOperation("CompleteMultipartUpload")),
                    resp,
                );
                if self.op.if_match().is_some() && err.kind() == ErrorKind::NotFound {
                    Err(Error::new(
                        ErrorKind::ConditionNotMatch,
                        "write precondition requires a live target",
                    ))
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        let resp = self
            .core
            .s3_abort_multipart_upload(&self.ctx, &self.path, upload_id)
            .await?;
        match resp.status() {
            // s3 returns code 204 if abort succeeds.
            StatusCode::NO_CONTENT => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("AbortMultipartUpload")),
                resp,
            )),
        }
    }
}

impl oio::AppendWrite for S3Writer {
    async fn offset(&self) -> Result<u64> {
        let resp = self
            .core
            .s3_head_object(&self.ctx, &self.path, OpStat::default())
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(parse_content_length(resp.headers())?.unwrap_or_default()),
            StatusCode::NOT_FOUND => Ok(0),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("HeadObject")),
                resp,
            )),
        }
    }

    async fn append(&self, offset: u64, size: u64, body: Buffer) -> Result<Metadata> {
        let include_conditions = !self.appended.load(Ordering::Relaxed);
        let error_ctx = if include_conditions {
            self.error_context(ServiceOperation("PutObject"))
        } else {
            ErrorContext::new(ServiceOperation("PutObject"))
        };
        let req = self.core.s3_append_object_request(
            &self.path,
            offset,
            size,
            &self.op,
            include_conditions,
            body,
        )?;

        let resp = self.core.send(&self.ctx, req).await?;

        let status = resp.status();

        let meta = S3Writer::parse_header_into_meta(&self.path, resp.headers())?;

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                self.appended.store(true, Ordering::Relaxed);
                Ok(meta)
            }
            _ => {
                let err = parse_error(error_ctx, resp);
                if include_conditions
                    && self.op.if_match().is_some()
                    && err.kind() == ErrorKind::NotFound
                {
                    Err(Error::new(
                        ErrorKind::ConditionNotMatch,
                        "write precondition requires a live target",
                    ))
                } else {
                    Err(err)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http::Method;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use http::header::ETAG;
    use opendal_core::*;

    use crate::backend::S3Builder;

    #[derive(Clone)]
    struct MultipartCompleteTransport {
        complete_body: &'static str,
    }

    impl HttpTransport for MultipartCompleteTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let query = req.uri().query().unwrap_or_default();
            match (req.method(), query) {
                (&Method::POST, "uploads") => {
                    let body = Buffer::from(Bytes::from_static(
                        b"<InitiateMultipartUploadResult><UploadId>u1</UploadId></InitiateMultipartUploadResult>",
                    ));
                    let size = body.len() as u64;
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .body(HttpBody::new(
                            futures::stream::iter(vec![Ok(body)]),
                            Some(size),
                        ))
                        .expect("mock response must build"))
                }
                (&Method::PUT, q) if q.contains("partNumber=") => Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(ETAG, "\"part-etag\"")
                    .body(HttpBody::new(futures::stream::empty(), Some(0)))
                    .expect("mock response must build")),
                (&Method::POST, q) if q.contains("uploadId=u1") => {
                    let body = Buffer::from(Bytes::from_static(self.complete_body.as_bytes()));
                    let size = body.len() as u64;
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .body(HttpBody::new(
                            futures::stream::iter(vec![Ok(body)]),
                            Some(size),
                        ))
                        .expect("mock response must build"))
                }
                (method, q) => panic!("unexpected request: {method} ?{q}"),
            }
        }
    }

    #[tokio::test]
    async fn complete_part_rejects_empty_etag_and_200_xml_errors() {
        const PART_SIZE: usize = 5 * 1024 * 1024;
        let part_payload = Bytes::from(vec![0u8; PART_SIZE]);

        for (body, expected_kind, expected_temporary) in [
            (
                "<CompleteMultipartUploadResult/>",
                ErrorKind::Unexpected,
                false,
            ),
            (
                "<Error><Message>missing code</Message></Error>",
                ErrorKind::Unexpected,
                false,
            ),
            (
                "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>",
                ErrorKind::Unexpected,
                false,
            ),
            (
                "<Error><Code>InternalError</Code><Message>retry me</Message></Error>",
                ErrorKind::Unexpected,
                true,
            ),
        ] {
            let op = Operator::new(
                S3Builder::default()
                    .bucket("example")
                    .region("us-east-1")
                    .skip_signature()
                    .disable_config_load()
                    .disable_ec2_metadata(),
            )
            .expect("S3 operator must build")
            .with_context(
                OperationContext::new().with_http_transport(HttpTransporter::new(
                    MultipartCompleteTransport {
                        complete_body: body,
                    },
                )),
            );

            let mut writer = op.writer("object.bin").await.expect("writer must open");
            writer
                .write(part_payload.clone())
                .await
                .expect("first 5 MiB part");
            writer.write("tail").await.expect("second part");
            let err = writer
                .close()
                .await
                .expect_err("complete_part must reject empty ETag / 200-OK XML error");
            assert_eq!(err.kind(), expected_kind, "body={body}");
            assert_eq!(err.is_temporary(), expected_temporary, "body={body}");
        }
    }

    #[derive(Clone)]
    struct SinglePutMd5Transport;

    impl HttpTransport for SinglePutMd5Transport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            assert_eq!(
                req.method(),
                Method::PUT,
                "checksum_algorithm=md5 write must use single PutObject, not multipart: {} {:?}",
                req.method(),
                req.uri()
            );
            assert!(
                req.headers().contains_key("content-md5"),
                "single PutObject with checksum_algorithm=md5 must attach content-md5"
            );
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(ETAG, "\"single-put-etag\"")
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
                .expect("mock response must build"))
        }
    }

    #[tokio::test]
    async fn md5_checksum_writes_over_5mib_in_single_put_without_multipart() {
        let op = Operator::new(
            S3Builder::default()
                .bucket("example")
                .region("us-east-1")
                .checksum_algorithm("md5")
                .skip_signature()
                .disable_config_load()
                .disable_ec2_metadata(),
        )
        .expect("S3 operator must build")
        .with_context(
            OperationContext::new()
                .with_http_transport(HttpTransporter::new(SinglePutMd5Transport)),
        );

        let payload = Bytes::from(vec![b'a'; 6 * 1024 * 1024]);
        let meta = op
            .write("large-md5.bin", payload)
            .await
            .expect("single write >5 MiB with checksum_algorithm=md5 must succeed via PutObject");
        assert_eq!(meta.etag(), Some("\"single-put-etag\""));
    }
}
