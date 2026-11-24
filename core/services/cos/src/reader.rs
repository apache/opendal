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

use super::backend::*;
use super::core::{ErrorContext, parse_error, parse_object_metadata};
use http::Response;
use http::StatusCode;
use opendal_core::raw::*;
use opendal_core::*;

/// Reader returned by this backend.
pub struct CosReader {
    backend: CosBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl CosReader {
    pub(super) fn new(
        backend: CosBackend,
        ctx: OperationContext,
        path: &str,
        args: OpRead,
    ) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::StreamRead for CosReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend
            .core
            .cos_get_object(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();

        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                RpRead::new(parse_object_metadata(path, resp.headers())?),
                resp.into_body(),
            ),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("GetObject"))
                        .with_caller_condition(args.is_conditional()),
                    Response::from_parts(part, buf),
                ));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::stream;
    use http::Request;
    use http::header::{CONTENT_LENGTH, CONTENT_RANGE};

    use super::*;
    use crate::core::constants;
    use crate::core::test_support::test_core;
    use opendal_core::raw::oio::StreamRead;

    #[derive(Clone)]
    struct ReaderTestTransport {
        status: StatusCode,
        body: &'static [u8],
        content_range: Option<&'static str>,
        with_user_meta: bool,
        version_id: Option<&'static str>,
    }

    impl HttpTransport for ReaderTestTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let mut builder = Response::builder()
                .status(self.status)
                .header(CONTENT_LENGTH, self.body.len());
            if let Some(cr) = self.content_range {
                builder = builder.header(CONTENT_RANGE, cr);
            }
            if self.with_user_meta {
                builder = builder
                    .header("x-cos-meta-author", "alice")
                    .header("x-cos-meta-env", "prod");
            }
            if let Some(version_id) = self.version_id {
                builder = builder.header(constants::X_COS_VERSION_ID, version_id);
            }
            let buf = Buffer::from(self.body);
            let len = buf.len() as u64;
            builder
                .body(HttpBody::new(stream::iter([Ok(buf)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    #[tokio::test]
    async fn reader_open_parses_user_metadata_and_version_id() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let backend = CosBackend {
                core: test_core("/"),
            };

            for (status, range, content_range) in [
                (StatusCode::OK, BytesRange::from(..), None),
                (
                    StatusCode::PARTIAL_CONTENT,
                    BytesRange::new(0, Some(5)),
                    Some("bytes 0-4/5"),
                ),
            ] {
                let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(
                    ReaderTestTransport {
                        status,
                        body: b"hello",
                        content_range,
                        with_user_meta: true,
                        version_id: Some("v1+2"),
                    },
                ));
                let reader = CosReader::new(backend.clone(), ctx, "hello.txt", OpRead::new());
                let (rp, _) = reader.open(range).await.unwrap();
                let meta = rp.into_metadata().expect("reader must return metadata");
                assert_eq!(meta.version(), Some("v1+2"));
                let user_meta = meta
                    .user_metadata()
                    .expect("x-cos-meta-* headers must be parsed");
                assert_eq!(user_meta.get("author"), Some("alice"));
                assert_eq!(user_meta.get("env"), Some("prod"));
            }

            // `x-cos-version-id: null` and absent user metadata leave both fields as None.
            let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(
                ReaderTestTransport {
                    status: StatusCode::OK,
                    body: b"hello",
                    content_range: None,
                    with_user_meta: false,
                    version_id: Some("null"),
                },
            ));
            let reader = CosReader::new(backend, ctx, "hello.txt", OpRead::new());
            let (rp, _) = reader.open(BytesRange::from(..)).await.unwrap();
            let meta = rp.into_metadata().expect("reader must return metadata");
            assert_eq!(meta.version(), None);
            assert!(meta.user_metadata().is_none());
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn reader_open_maps_401_to_permission_denied() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let backend = CosBackend {
                core: test_core("/"),
            };
            let ctx =
                OperationContext::default()
                    .with_http_transport(HttpTransporter::new(ReaderTestTransport {
                    status: StatusCode::UNAUTHORIZED,
                    body:
                        b"<Error><Code>AccessDenied</Code><Message>Unauthorized</Message></Error>",
                    content_range: None,
                    with_user_meta: false,
                    version_id: None,
                }));
            let reader = CosReader::new(backend, ctx, "hello.txt", OpRead::new());
            let err = match reader.open(BytesRange::from(..)).await {
                Ok(_) => panic!("401 response must fail"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::PermissionDenied);
            assert!(!err.is_temporary());
        })
        .await
        .expect("test timed out");
    }
}
