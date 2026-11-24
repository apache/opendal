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
use opendal_core::raw::*;
use opendal_core::*;

use super::core::parse_error;
use super::core::*;

pub type ObsWriters = TwoWays<oio::MultipartWriter<ObsWriter>, oio::AppendWriter<ObsWriter>>;

pub struct ObsWriter {
    core: Arc<ObsCore>,
    ctx: OperationContext,

    op: OpWrite,
    path: String,
}

impl ObsWriter {
    pub fn new(core: Arc<ObsCore>, ctx: OperationContext, path: &str, op: OpWrite) -> Self {
        ObsWriter {
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
        if let Some(version) = parse_header_to_str(headers, constants::X_OBS_VERSION_ID)? {
            meta.version(version);
        }

        Ok(meta.build())
    }
}

impl oio::MultipartWrite for ObsWriter {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        let req = self
            .core
            .obs_put_object_request(&self.path, Some(size), &self.op, body)?;

        let req = self.core.sign(&self.ctx, req).await?;

        let resp = self.core.send(&self.ctx, req).await?;

        let meta = Self::parse_metadata(resp.headers())?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(meta),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("PutObject")),
                resp,
            )),
        }
    }

    async fn initiate_part(&self) -> Result<String> {
        let resp = self
            .core
            .obs_initiate_multipart_upload(&self.ctx, &self.path, self.op.content_type())
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
                ErrorContext::new(ServiceOperation("InitiateMultipartUpload")),
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
        // Obs service requires part number must between [1..=10000]
        let part_number = part_number + 1;

        let resp = self
            .core
            .obs_upload_part_request(
                &self.ctx,
                &self.path,
                upload_id,
                part_number,
                Some(size),
                body,
            )
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
            .obs_complete_multipart_upload(&self.ctx, &self.path, upload_id, parts)
            .await?;

        if resp.status() != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("CompleteMultipartUpload")),
                resp,
            ));
        }

        let (parts, body) = resp.into_parts();
        let bs = body.to_bytes();

        let result: CompleteMultipartUploadResult =
            quick_xml::de::from_reader(bs.as_ref()).map_err(new_xml_deserialize_error)?;
        if !result.code.is_empty() || result.etag.is_empty() {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("CompleteMultipartUpload")),
                Response::from_parts(parts, Buffer::from(bs)),
            ));
        }

        let mut meta = Self::parse_metadata(&parts.headers)?.into_builder();
        meta.etag(&result.etag);

        Ok(meta.build())
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        let resp = self
            .core
            .obs_abort_multipart_upload(&self.ctx, &self.path, upload_id)
            .await?;
        match resp.status() {
            // Obs returns code 204 No Content if abort succeeds.
            // Reference: https://support.huaweicloud.com/intl/en-us/api-obs/obs_04_0103.html
            StatusCode::NO_CONTENT => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("AbortMultipartUpload")),
                resp,
            )),
        }
    }
}

impl oio::AppendWrite for ObsWriter {
    async fn offset(&self) -> Result<u64> {
        let resp = self
            .core
            .obs_head_object(&self.ctx, &self.path, &OpStat::default())
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
            .obs_append_object_request(&self.path, offset, size, &self.op, body)?;

        let req = self.core.sign(&self.ctx, req).await?;

        let resp = self.core.send(&self.ctx, req).await?;

        let mut meta = MetadataBuilder::unknown();
        if let Some(md5) = parse_content_md5(resp.headers())? {
            meta.content_md5(md5);
        }
        if let Some(version) = parse_header_to_str(resp.headers(), constants::X_OBS_VERSION_ID)? {
            meta.version(version);
        }

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
    use std::time::Duration;

    use bytes::Bytes;
    use futures::stream;
    use http::Request;
    use opendal_core::raw::oio::MultipartWrite;

    use super::*;
    use crate::backend::ObsBackend;
    use crate::backend::ObsBuilder;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    struct StaticCompleteTransport {
        status: StatusCode,
        body: &'static [u8],
        version_id: Option<&'static str>,
    }

    impl HttpTransport for StaticCompleteTransport {
        async fn fetch(&self, _: Request<Buffer>) -> Result<Response<HttpBody>> {
            let buf = Buffer::from(Bytes::from_static(self.body));
            let size = buf.len() as u64;
            let mut builder = Response::builder().status(self.status);
            if let Some(version_id) = self.version_id {
                builder = builder.header(constants::X_OBS_VERSION_ID, version_id);
            }
            Ok(builder
                .body(HttpBody::new(stream::iter(vec![Ok(buf)]), Some(size)))
                .expect("mock response must build"))
        }
    }

    fn test_writer(transport: StaticCompleteTransport) -> ObsWriter {
        let service = ObsBuilder::default()
            .bucket("example-bucket")
            .endpoint("https://obs.cn-north-4.myhuaweicloud.com")
            .skip_signature()
            .disable_config_load()
            .build()
            .expect("obs backend must build");
        let backend = (&service as &dyn std::any::Any)
            .downcast_ref::<ObsBackend>()
            .expect("builder must produce ObsBackend");
        let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
        ObsWriter::new(backend.core.clone(), ctx, "test.bin", OpWrite::default())
    }

    fn sample_parts() -> Vec<oio::MultipartPart> {
        vec![oio::MultipartPart {
            part_number: 1,
            etag: "\"part-etag-1\"".to_string(),
            checksum: None,
            size: None,
        }]
    }

    #[tokio::test]
    async fn complete_part_preserves_retryability_on_non_xml_502() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            for html_body in [
                b"<!DOCTYPE html><html><body>502 Bad Gateway</body></html>".as_slice(),
                b"<html><body>502 Bad Gateway</body></html>".as_slice(),
            ] {
                let writer = test_writer(StaticCompleteTransport {
                    status: StatusCode::BAD_GATEWAY,
                    body: html_body,
                    version_id: None,
                });

                let err = writer
                    .complete_part("upload-1", &sample_parts())
                    .await
                    .expect_err("502 response must fail");

                assert_eq!(err.kind(), ErrorKind::Unexpected);
                assert!(err.is_temporary(), "502 error must remain retryable");
                assert!(
                    err.to_string().contains("502 Bad Gateway"),
                    "error message must preserve response body: {err}"
                );
            }
        })
        .await
        .expect("test must not time out");
    }

    #[tokio::test]
    async fn complete_part_preserves_xml_error_details_on_404() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let writer = test_writer(StaticCompleteTransport {
                status: StatusCode::NOT_FOUND,
                body: br#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>NoSuchUpload</Code>
    <Message>The specified upload does not exist.</Message>
    <RequestId>req-404-upload</RequestId>
</Error>"#,
                version_id: None,
            });

            let err = writer
                .complete_part("upload-missing", &sample_parts())
                .await
                .expect_err("404 response must fail");

            assert_eq!(err.kind(), ErrorKind::NotFound);
            assert!(!err.is_temporary());
            let msg = err.to_string();
            assert!(msg.contains("NoSuchUpload"), "missing Code in {msg}");
            assert!(
                msg.contains("The specified upload does not exist."),
                "missing Message in {msg}"
            );
            assert!(msg.contains("req-404-upload"), "missing RequestId in {msg}");
        })
        .await
        .expect("test must not time out");
    }

    #[tokio::test]
    async fn complete_part_rejects_200_ok_xml_error_and_empty_etag() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let err = test_writer(StaticCompleteTransport {
                status: StatusCode::OK,
                body: br#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>InternalError</Code>
    <Message>We encountered an internal error.</Message>
    <RequestId>req-200-err</RequestId>
</Error>"#,
                version_id: None,
            })
            .complete_part("upload-1", &sample_parts())
            .await
            .expect_err("200 OK <Error> must fail");

            assert_eq!(err.kind(), ErrorKind::Unexpected);
            let msg = err.to_string();
            assert!(msg.contains("InternalError"), "missing Code in {msg}");
            assert!(
                msg.contains("We encountered an internal error."),
                "missing Message in {msg}"
            );

            for empty_etag_body in [
                b"<CompleteMultipartUploadResult/>".as_slice(),
                b"<CompleteMultipartUploadResult><ETag></ETag></CompleteMultipartUploadResult>"
                    .as_slice(),
            ] {
                let err = test_writer(StaticCompleteTransport {
                    status: StatusCode::OK,
                    body: empty_etag_body,
                    version_id: None,
                })
                .complete_part("upload-1", &sample_parts())
                .await
                .expect_err("empty ETag must fail");

                assert_eq!(err.kind(), ErrorKind::Unexpected);
            }
        })
        .await
        .expect("test must not time out");
    }

    #[tokio::test]
    async fn complete_part_returns_etag_and_version_on_valid_200() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let writer = test_writer(StaticCompleteTransport {
                status: StatusCode::OK,
                body: br#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult>
    <Location>https://example-bucket.obs.cn-north-4.myhuaweicloud.com/test.bin</Location>
    <Bucket>example-bucket</Bucket>
    <Key>test.bin</Key>
    <ETag>"final-etag-123"</ETag>
</CompleteMultipartUploadResult>"#,
                version_id: Some("ver-99"),
            });

            let meta = writer
                .complete_part("upload-1", &sample_parts())
                .await
                .expect("valid complete response must succeed");

            assert_eq!(meta.etag(), Some("\"final-etag-123\""));
            assert_eq!(meta.version(), Some("ver-99"));
        })
        .await
        .expect("test must not time out");
    }
}
