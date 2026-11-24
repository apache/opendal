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
use constants::X_TOS_OBJECT_SIZE;
use constants::X_TOS_VERSION_ID;
use http::StatusCode;

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
}

impl TosWriter {
    pub fn new(core: Arc<TosCore>, ctx: OperationContext, path: &str, op: OpWrite) -> Self {
        TosWriter {
            core,
            ctx,
            path: path.to_string(),
            op,
        }
    }

    fn parse_header_into_meta(path: &str, headers: &http::HeaderMap) -> Result<Metadata> {
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
        Ok(meta.build())
    }
}

impl oio::MultipartWrite for TosWriter {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        let req = self
            .core
            .tos_put_object_request(&self.path, Some(size), &self.op, body)?;

        let resp = self.core.send(&self.ctx, req).await?;

        let status = resp.status();

        let meta = TosWriter::parse_header_into_meta(&self.path, resp.headers())?;

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
        let part_number = part_number + 1;

        let req =
            self.core
                .tos_upload_part_request(&self.path, upload_id, part_number, size, body)?;

        let resp = self.core.send(&self.ctx, req).await?;
        let status = resp.status();

        match status {
            StatusCode::OK => {
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
            .tos_complete_multipart_upload(&self.ctx, &self.path, upload_id, parts, &self.op)
            .await?;

        let status = resp.status();
        let mut meta =
            TosWriter::parse_header_into_meta(&self.path, resp.headers())?.into_builder();

        match status {
            StatusCode::OK => {
                let ret: CompleteMultipartUploadResult =
                    serde_json::from_reader(resp.into_body().reader())
                        .map_err(new_json_deserialize_error)?;
                if !ret.code.is_empty() {
                    let (kind, retryable) =
                        parse_tos_error_code(&ret.code).unwrap_or((ErrorKind::Unexpected, false));
                    let mut err = Error::new(kind, format!("{ret:?}"));
                    if retryable {
                        err = err.set_temporary();
                    }
                    return Err(err);
                }
                let etag = ret.etag.trim_matches('"');
                if etag.is_empty() {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "ETag not present in complete multipart upload response",
                    )
                    .set_temporary());
                }
                // CompleteMultipartUpload response wraps ETag in quotes:
                // https://www.volcengine.com/docs/6349/74868
                meta.etag(etag);
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::stream;
    use http::{Request, Response};
    use opendal_core::raw::oio::MultipartWrite;

    use super::*;

    struct StaticTransport {
        body: String,
    }

    impl HttpTransport for StaticTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let len = self.body.len() as u64;
            Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from(self.body.clone()))]),
                    Some(len),
                ))
                .map_err(new_request_build_error)
        }
    }

    fn build_core() -> Arc<TosCore> {
        let backend = crate::Tos::default()
            .bucket("test-bucket")
            .skip_signature()
            .build()
            .expect("build must succeed");
        let backend = (&backend as &dyn std::any::Any)
            .downcast_ref::<crate::backend::TosBackend>()
            .expect("build must return TosBackend");
        backend.core.clone()
    }

    #[tokio::test]
    async fn test_complete_part_classifies_200_ok_error_codes() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let core = build_core();
            let cases = [
                ("SlowDown", ErrorKind::RateLimited, true),
                ("InternalError", ErrorKind::Unexpected, true),
                ("OperationAborted", ErrorKind::Unexpected, true),
                ("AccessDenied", ErrorKind::PermissionDenied, false),
            ];

            for (code, expected_kind, expected_temporary) in cases {
                let body = format!(r#"{{"Code":"{code}","Message":"error for {code}"}}"#);
                let ctx = OperationContext::new()
                    .with_http_transport(HttpTransporter::new(StaticTransport { body }));
                let writer = TosWriter::new(core.clone(), ctx, "a.txt", OpWrite::default());
                let err = writer
                    .complete_part("uid", &[])
                    .await
                    .expect_err("200 OK error body must fail");
                assert_eq!(err.kind(), expected_kind, "code={code}");
                assert_eq!(err.is_temporary(), expected_temporary, "code={code}");
            }
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn test_complete_part_rejects_empty_etag() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let core = build_core();
            for body in [r#"{}"#, r#"{"ETag":""}"#, r#"{"ETag":"\"\""}"#] {
                let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(
                    StaticTransport {
                        body: body.to_string(),
                    },
                ));
                let writer = TosWriter::new(core.clone(), ctx, "a.txt", OpWrite::default());
                let err = writer
                    .complete_part("uid", &[])
                    .await
                    .expect_err("empty ETag must fail");
                assert_eq!(err.kind(), ErrorKind::Unexpected);
                assert!(err.is_temporary());
            }
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn test_complete_part_parses_quoted_etag_and_version_id() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(
                StaticTransport {
                    body: r#"{"ETag":"\"etag-123\"","VersionId":"v-1"}"#.to_string(),
                },
            ));
            let writer = TosWriter::new(build_core(), ctx, "a.txt", OpWrite::default());
            let meta = writer.complete_part("uid", &[]).await.unwrap();
            assert_eq!(meta.etag(), Some("etag-123"));
            assert_eq!(meta.version(), Some("v-1"));
        })
        .await
        .expect("test timed out");
    }
}
